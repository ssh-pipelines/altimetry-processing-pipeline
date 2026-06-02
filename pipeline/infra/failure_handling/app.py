import json
import logging
import os
import re
from collections import defaultdict
from typing import Any
from urllib.parse import quote

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sns = boto3.client("sns")
s3 = boto3.client("s3")

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")

AUTH_FAILURE_PATTERN = re.compile(r"\b(401|403|Unauthorized|Forbidden)\b")
MAX_DISTINCT_FAILURES_IN_BODY = 10

# Error-type prefixes/values that indicate the Lambda runtime (not handler code)
# killed the process. `Sandbox.Timedout` is the Lambda execution-environment
# timeout; `States.Timeout` is a state-level TimeoutSeconds breach.
RUNTIME_ERROR_PREFIXES = ("Lambda.", "Sandbox.")
RUNTIME_ERROR_TYPES = {"States.Timeout"}


# Layout mirrors utilities/pipeline_layout.py:stage_results_prefix.
# Duplicated here because infra Lambdas don't ship the utilities package.
def _stage_results_prefix(source: str, run_id: str, stage: str) -> str:
    return f"pipeline_runs/{source}/{run_id}/results/{stage}/"


def _run_id_from_jobs_key(jobs_key: str) -> str:
    # jobs_key shape: pipeline_runs/{source}/{run_id}/jobs.json
    parts = jobs_key.split("/")
    return parts[2] if len(parts) >= 3 else "unknown"


def _classify(error_type: str, error_message: str) -> str:
    if error_type.startswith(RUNTIME_ERROR_PREFIXES) or error_type in RUNTIME_ERROR_TYPES:
        return "Runtime failure"
    if error_type == "PipelineError" and AUTH_FAILURE_PATTERN.search(error_message or ""):
        return "Auth failure"
    return "Code failure"


def _parse_failed_item(entry: dict) -> dict:
    """Extract structured info from a failure envelope.

    Step Functions nests failure data in two distinct envelope shapes:
      - Child-SM-failure envelope: {Cause, Error, ExecutionArn, Input, Status, ...}
        where Cause is itself a JSON string of the next envelope.
      - Lambda-failure envelope: {errorType, errorMessage, stackTrace}
        where errorMessage may *itself* be a JSON string carrying our
        PipelineError-packaged {errorType, errorMessage, input} payload.

    A parent SM's Catch sees a child-SM-failure envelope at the top; the leaf
    Lambda-failure envelope (with our packaged payload, if any) is nested one
    or two levels deeper. Unwrap iteratively until we find the leaf or run out.
    """
    cause_raw = entry.get("Cause", "")
    error_type = entry.get("Error", "")
    error_message = ""
    item_input: Any = None

    while cause_raw:
        try:
            cause = json.loads(cause_raw)
        except (json.JSONDecodeError, TypeError):
            error_message = cause_raw
            break
        if not isinstance(cause, dict):
            error_message = cause_raw
            break

        if "errorType" in cause or "errorMessage" in cause:
            error_type = cause.get("errorType", error_type)
            outer_message = cause.get("errorMessage", "")
            try:
                inner = json.loads(outer_message)
            except (json.JSONDecodeError, TypeError):
                inner = None
            if isinstance(inner, dict) and ("errorType" in inner or "input" in inner):
                error_type = inner.get("errorType", error_type)
                error_message = inner.get("errorMessage", outer_message)
                item_input = inner.get("input", item_input)
            else:
                error_message = outer_message
            break

        if "Cause" in cause and "Error" in cause:
            error_type = cause.get("Error", error_type)
            sm_input = cause.get("Input")
            if item_input is None and sm_input is not None:
                if isinstance(sm_input, str):
                    try:
                        item_input = json.loads(sm_input)
                    except (json.JSONDecodeError, TypeError):
                        item_input = sm_input
                else:
                    item_input = sm_input
            cause_raw = cause.get("Cause", "")
            continue

        error_message = cause_raw
        break

    return {
        "errorType": error_type or "Unknown",
        "errorMessage": error_message or "",
        "input": item_input,
        "category": _classify(error_type, error_message),
    }


def _read_failed_items(bucket: str, prefix: str) -> list[dict]:
    paginator = s3.get_paginator("list_objects_v2")
    failed_keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if "/FAILED_" in obj["Key"]:
                failed_keys.append(obj["Key"])

    items: list[dict] = []
    for key in failed_keys:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            logger.warning("Could not parse FAILED file %s as JSON", key)
            continue
        entries = payload if isinstance(payload, list) else payload.get("Items", [])
        for entry in entries:
            items.append(_parse_failed_item(entry))
    return items


def _dedupe(items: list[dict]) -> list[dict]:
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for item in items:
        key = (item["category"], item["errorType"], item["errorMessage"])
        groups[key].append(item)

    out = []
    for (category, error_type, error_message), members in groups.items():
        affected_dates = sorted({
            (m["input"] or {}).get("date", "?") for m in members
        })
        out.append({
            "category": category,
            "errorType": error_type,
            "errorMessage": error_message,
            "count": len(members),
            "sample_input": members[0]["input"],
            "affected_dates": affected_dates,
        })
    out.sort(key=lambda g: (-g["count"], g["category"], g["errorType"]))
    return out


def _child_execution_arn_from_cause(cause_str: str) -> str | None:
    if not cause_str:
        return None
    try:
        cause = json.loads(cause_str)
    except (json.JSONDecodeError, TypeError):
        return None
    return cause.get("ExecutionArn") if isinstance(cause, dict) else None


def _cloudwatch_deep_link(child_arn: str | None) -> str:
    if not child_arn:
        return ""
    encoded = quote(child_arn, safe="")
    return (
        f"https://{AWS_REGION}.console.aws.amazon.com/states/home"
        f"?region={AWS_REGION}#/v2/executions/details/{encoded}"
    )


def _sample_input_for_display(sample_input: Any) -> Any:
    """Strip known-large fields (granule URI lists) so the sample is scannable."""
    if not isinstance(sample_input, dict):
        return sample_input
    display = dict(sample_input)
    granules = display.get("granules")
    if isinstance(granules, list):
        display["granules"] = f"[{len(granules)} URIs omitted]"
    return display


def _top_level_envelope_error(top_cause: str) -> str:
    if not top_cause:
        return ""
    try:
        env = json.loads(top_cause)
    except (json.JSONDecodeError, TypeError):
        return ""
    return env.get("Error", "") if isinstance(env, dict) else ""


def _format_message(
    stage: str,
    source: str,
    run_id: str,
    top_cause: str,
    child_arn: str | None,
    deep_link: str,
    distinct: list[dict],
    total_failed: int,
    overflow_url: str | None,
) -> str:
    lines = [
        f"Stage: {stage}",
        f"Source: {source}",
        f"Run ID: {run_id}",
        f"Total failed items: {total_failed}",
    ]
    envelope_error = _top_level_envelope_error(top_cause)
    if envelope_error:
        lines.append(f"Top-level error: {envelope_error}")
    lines.append("")
    if child_arn:
        lines += [f"Child execution: {child_arn}", f"Console: {deep_link}", ""]

    if not distinct:
        lines.append("No per-item detail available (no FAILED_*.json under the ResultWriter prefix).")
    else:
        shown = distinct[:MAX_DISTINCT_FAILURES_IN_BODY]
        lines.append(f"Distinct failures ({len(distinct)} groups, showing {len(shown)}):")
        for i, group in enumerate(shown, 1):
            lines.append(
                f"  [{i}] [{group['category']}] {group['errorType']}: "
                f"{group['errorMessage']} (x{group['count']})"
            )
            if group["sample_input"]:
                display = _sample_input_for_display(group["sample_input"])
                lines.append(f"      Sample input: {json.dumps(display)}")
            if group["affected_dates"]:
                dates_preview = ", ".join(group["affected_dates"][:5])
                if len(group["affected_dates"]) > 5:
                    dates_preview += f", ... ({len(group['affected_dates'])} dates total)"
                lines.append(f"      Affected dates: {dates_preview}")
        if len(distinct) > MAX_DISTINCT_FAILURES_IN_BODY:
            extra = len(distinct) - MAX_DISTINCT_FAILURES_IN_BODY
            lines.append(f"  ...and {extra} more distinct failures.")
            if overflow_url:
                lines.append(f"  Full manifest: {overflow_url}")

    return "\n".join(lines)


def lambda_handler(event, context):
    stage = event.get("stage", "unknown")
    source = event.get("source", "unknown")
    bucket = event.get("bucket", "")
    jobs_key = event.get("jobs_key", "")
    error_output = event.get("errorOutput") or {}
    top_error = error_output.get("Error", "")
    top_cause = error_output.get("Cause", "")

    run_id = _run_id_from_jobs_key(jobs_key)
    prefix = _stage_results_prefix(source, run_id, stage)
    child_arn = _child_execution_arn_from_cause(top_cause)
    deep_link = _cloudwatch_deep_link(child_arn)

    distinct: list[dict] = []
    total_failed = 0
    overflow_url = None

    if bucket and prefix:
        try:
            items = _read_failed_items(bucket, prefix)
            total_failed = len(items)
            distinct = _dedupe(items)
            if len(distinct) > MAX_DISTINCT_FAILURES_IN_BODY:
                overflow_url = f"s3://{bucket}/{prefix}"
        except Exception as e:
            logger.warning("Could not read FAILED items under %s: %s", prefix, e)

    if total_failed == 0:
        # Fallback for cases with no per-item ResultWriter output: pipeline_init
        # / set_sg_jobs / indicators failures, ItemReader failures, etc.
        item = _parse_failed_item({"Cause": top_cause, "Error": top_error})
        distinct = [{
            "category": item["category"],
            "errorType": item["errorType"],
            "errorMessage": item["errorMessage"] or top_cause,
            "count": 1,
            "sample_input": item["input"],
            "affected_dates": [],
        }]
        total_failed = 1

    subject = f"Pipeline failure: {stage} / {source}"[:100]
    message = _format_message(
        stage, source, run_id, top_cause, child_arn,
        deep_link, distinct, total_failed, overflow_url,
    )

    try:
        sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject, Message=message)
    except Exception as e:
        # Never raise — the parent SM owns the Fail transition. A failure_handling
        # exception would mask the original failure with our own notification bug.
        logger.error("Failed to publish SNS for %s/%s: %s", stage, source, e)

    return {"status": "notified", "stage": stage, "source": source, "failed_count": total_failed}
