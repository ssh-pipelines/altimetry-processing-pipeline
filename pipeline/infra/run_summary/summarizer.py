"""Reconciliation core for the ``run_summary`` Lambda (see ADR 0005).

Reconciles, per **Product pipeline**, the **Job specs** a run expected (from its manifest)
against the **Job outcomes** its deliverable stages produced (from the Distributed Map
``ResultWriter``), and assembles the **Run summary** artifact.

The pure functions here (`reconcile_pipeline`, `build_summary`) take already-loaded data so
they are unit-testable without S3; the thin I/O helpers (`read_manifest`, `read_outcomes`)
wrap boto3 and are exercised with a stubbed client. All S3-key derivation goes through
``utilities.pipeline_layout`` — this Lambda ships ``utilities`` precisely so it can.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from utilities.job_outcome import SCHEMA_VERSION, SKIPPED, SUCCESS
from utilities.pipeline_layout import (
    jobs_key_identity,
    run_summary_key,
    sg_jobs_key,
    stage_results_prefix,
)

logger = logging.getLogger(__name__)


# A deliverable the run summary reconciles. `anchor` deliverables define a Product
# pipeline's `missing` set (one manifest date -> one expected anchor output).
# `conditional` deliverables are reported only when they actually ran (the unifier
# runs only on a unified run), so a non-unified run does not show them as all-missing.
PRODUCT_PIPELINES = {
    "along_track": {
        "manifest": lambda jobs_key: jobs_key,
        "deliverables": [
            {"kind": "daily_file_p3", "stage": "finalizer", "anchor": True, "conditional": False},
            {"kind": "nasa_ssh_p3", "stage": "unifier", "anchor": False, "conditional": True},
        ],
    },
    "gridded": {
        "manifest": lambda jobs_key: sg_jobs_key(jobs_key),
        "deliverables": [
            {"kind": "simple_grid", "stage": "simple_grids", "anchor": True, "conditional": False},
            {"kind": "enso", "stage": "enso", "anchor": False, "conditional": False},
        ],
    },
}


def manifest_dates(job_specs: list[dict]) -> list[str]:
    """Sorted distinct ISO dates declared by a manifest's Job specs."""
    return sorted({j["date"] for j in job_specs if isinstance(j, dict) and j.get("date")})


def _provenance_incomplete(successes: list[dict]) -> int:
    """Count outcomes whose metadata explicitly declares incomplete lineage.

    A *missing* `provenance_complete` (older emitters / the unifier byte-copy / legacy
    files) is "unknown", not incomplete — it is never counted here, so absence never
    inflates the incomplete tally (the backward-compat contract of ADR 0005).
    """
    return sum(
        1 for o in successes if o.get("metadata", {}).get("provenance_complete") is False
    )


def reconcile_pipeline(
    deliverable_specs: list[dict],
    expected_dates: list[str],
    outcomes_by_stage: dict[str, list[dict]],
) -> dict:
    """Reconcile one Product pipeline into its Run-summary section.

    `outcomes_by_stage` maps a stage name to its list of **Job outcome** dicts.
    """
    expected = set(expected_dates)
    deliverables: dict[str, dict] = {}
    anchor_accounted: dict[str, str | None] = {}  # date -> skip reason (None == produced)

    for spec in deliverable_specs:
        outcomes = outcomes_by_stage.get(spec["stage"], [])
        if not outcomes and spec["conditional"]:
            # e.g. the unifier on a non-unified run — not a deliverable of this run.
            continue

        successes = [o for o in outcomes if o.get("status") == SUCCESS]
        skips = [o for o in outcomes if o.get("status") == SKIPPED]
        outputs = [out for o in successes for out in o.get("outputs", [])]

        deliverables[spec["kind"]] = {
            "stage": spec["stage"],
            "produced": len(successes),
            "skipped": len(skips),
            "provenance_incomplete": _provenance_incomplete(successes),
            "outputs": outputs,
        }

        if spec["anchor"]:
            for o in successes:
                if o.get("date"):
                    anchor_accounted[o["date"]] = None
            for o in skips:
                if o.get("date"):
                    anchor_accounted.setdefault(
                        o["date"], o.get("metadata", {}).get("skip_reason", "skipped")
                    )

    missing = []
    for date in sorted(expected):
        if date not in anchor_accounted:
            missing.append({"date": date, "reason": "no outcome"})
        elif anchor_accounted[date] is not None:
            missing.append({"date": date, "reason": anchor_accounted[date]})

    return {
        "expected": len(expected),
        "deliverables": deliverables,
        "missing": missing,
    }


def build_summary(
    jobs_key: str,
    manifests: dict[str, list[dict]],
    outcomes: dict[str, dict[str, list[dict]]],
    *,
    completed_at: str | None = None,
) -> dict:
    """Assemble the Run summary artifact.

    `manifests` maps a Product-pipeline name to its Job specs; `outcomes` maps a
    Product-pipeline name to its `{stage: [outcome, ...]}`. Both are passed in (already
    loaded) so this stays pure and testable.
    """
    source, run_id = jobs_key_identity(jobs_key)
    unified_source = _unified_source(jobs_key)
    completed_at = completed_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    product_pipelines = {}
    for name, cfg in PRODUCT_PIPELINES.items():
        section = reconcile_pipeline(
            cfg["deliverables"],
            manifest_dates(manifests.get(name, [])),
            outcomes.get(name, {}),
        )
        section["manifest"] = cfg["manifest"](jobs_key)
        product_pipelines[name] = section

    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "source": source,
        "unified_source": unified_source,
        "completed_at": completed_at,
        "product_pipelines": product_pipelines,
    }


def _unified_source(jobs_key: str) -> str | None:
    """The unified product segment of a jobs key, or None.

    A unified run's manifest lives at ``pipeline_runs/{orig}/{run_id}/{unified}/jobs.json``
    (5 segments); a plain run at ``pipeline_runs/{orig}/{run_id}/jobs.json`` (4).
    """
    parts = jobs_key.split("/")
    return parts[3] if len(parts) >= 5 else None


# ─── S3 I/O (thin, stubbed in tests) ──────────────────────────────────────

def read_manifest(s3, bucket: str, key: str) -> list[dict]:
    """Load a manifest's Job specs. A missing manifest yields ``[]`` (e.g. a run with no
    gridded work) rather than an error — reconciliation simply reports zero expected."""
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception as e:
        logger.warning("Manifest %s not readable (%s); treating as empty.", key, e)
        return []
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        logger.warning("Manifest %s is not valid JSON; treating as empty.", key)
        return []
    return data if isinstance(data, list) else []


def read_outcomes(s3, bucket: str, prefix: str) -> list[dict]:
    """Read every Job outcome from the SUCCEEDED_*.json files under a ResultWriter prefix.

    Each ResultWriter entry carries the Lambda return in its ``Output`` field (a dict, or a
    JSON string Step Functions sometimes serializes); we parse it back to the Job outcome.
    """
    paginator = s3.get_paginator("list_objects_v2")
    outcomes: list[dict] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if "/SUCCEEDED_" not in obj["Key"]:
                continue
            try:
                entries = json.loads(s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read())
            except Exception as e:
                logger.warning("Could not read SUCCEEDED file %s: %s", obj["Key"], e)
                continue
            if not isinstance(entries, list):
                continue
            for entry in entries:
                outcome = _outcome_from_entry(entry)
                if outcome is not None:
                    outcomes.append(outcome)
    return outcomes


def _outcome_from_entry(entry: dict) -> dict | None:
    if not isinstance(entry, dict):
        return None
    output = entry.get("Output")
    if isinstance(output, str):
        try:
            output = json.loads(output)
        except (json.JSONDecodeError, TypeError):
            return None
    return output if isinstance(output, dict) else None


def gather(s3, bucket: str, jobs_key: str) -> tuple[dict, dict]:
    """Load all manifests + outcomes for a run, keyed by Product-pipeline name."""
    manifests: dict[str, list[dict]] = {}
    outcomes: dict[str, dict[str, list[dict]]] = {}
    for name, cfg in PRODUCT_PIPELINES.items():
        manifests[name] = read_manifest(s3, bucket, cfg["manifest"](jobs_key))
        outcomes[name] = {
            spec["stage"]: read_outcomes(s3, bucket, stage_results_prefix(jobs_key, spec["stage"]))
            for spec in cfg["deliverables"]
        }
    return manifests, outcomes


# ─── SNS rendering ────────────────────────────────────────────────────────

def render_notification(summary: dict) -> tuple[str, str]:
    """(subject, body) for the success SNS, rendered from the Run summary artifact."""
    source = summary["source"]
    unified = summary.get("unified_source")
    run_id = summary["run_id"]

    source_line = f"Source: {source} → {unified}" if unified else f"Source: {source}"
    lines = [source_line, f"Run ID: {run_id}", f"Completed: {summary['completed_at']}", ""]

    for name, section in summary["product_pipelines"].items():
        lines.append(f"{name} (expected {section['expected']}):")
        for kind, d in section["deliverables"].items():
            extra = ""
            if d.get("skipped"):
                extra += f", {d['skipped']} skipped"
            if d.get("provenance_incomplete"):
                extra += f", {d['provenance_incomplete']} incomplete-lineage"
            lines.append(f"  {kind} [{d['stage']}]: {d['produced']} produced{extra}")
        if section["missing"]:
            preview = ", ".join(
                f"{m['date']} ({m['reason']})" for m in section["missing"][:5]
            )
            if len(section["missing"]) > 5:
                preview += f", … ({len(section['missing'])} total)"
            lines.append(f"  missing: {preview}")
        lines.append("")

    subject = f"Pipeline success: {source} / {run_id}"[:100]
    return subject, "\n".join(lines).rstrip()


def summary_key(jobs_key: str) -> str:
    source, run_id = jobs_key_identity(jobs_key)
    return run_summary_key(source, run_id)
