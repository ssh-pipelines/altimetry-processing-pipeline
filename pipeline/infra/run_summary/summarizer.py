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
    run_params_key,
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


def summarize_bad_passes(results: list[dict]) -> dict:
    """Aggregate bad_pass Job results into the Run-summary diagnostics section.

    bad_pass is a **diagnostic** stage, not a deliverable — it produces no P3 and
    has nothing to reconcile against the manifest, so it lives outside
    `product_pipelines`. Each result is the bad_pass handler's return
    (`{date, source, count}`); `count` is how many (cycle, pass) passes were
    flagged for that date. We surface, per run, the flagged total and the dates
    it fell on (dates with zero flags are counted as reported but not listed).
    """
    per_date: dict[str, int] = {}
    for r in results:
        if not isinstance(r, dict):
            continue
        date = r.get("date")
        count = r.get("count")
        if not date or not isinstance(count, int):
            continue
        per_date[date] = per_date.get(date, 0) + count

    flagged = {d: c for d, c in per_date.items() if c > 0}
    return {
        "dates_reported": len(per_date),
        "dates_flagged": len(flagged),
        "total_flagged": sum(flagged.values()),
        "by_date": [{"date": d, "count": flagged[d]} for d in sorted(flagged)],
    }


def build_summary(
    jobs_key: str,
    manifests: dict[str, list[dict]],
    outcomes: dict[str, dict[str, list[dict]]],
    *,
    run_params: dict | None = None,
    bad_pass_results: list[dict] | None = None,
    completed_at: str | None = None,
) -> dict:
    """Assemble the Run summary artifact.

    `manifests` maps a Product-pipeline name to its Job specs; `outcomes` maps a
    Product-pipeline name to its `{stage: [outcome, ...]}`; `run_params` is the
    pipeline_init invocation sidecar (or `{}` for legacy runs without one);
    `bad_pass_results` are the bad_pass stage's raw Job results (or `[]`). All are
    passed in (already loaded) so this stays pure and testable.
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
        "parameters": run_params or {},
        "product_pipelines": product_pipelines,
        "bad_passes": summarize_bad_passes(bad_pass_results or []),
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
    if not isinstance(output, dict):
        return None
    # Defensive: a Map whose invoke task omits `Output: {% $states.result.Payload %}`
    # writes the raw Lambda invoke envelope ({Payload, StatusCode, ...}) instead of the
    # Job outcome (this once silently zeroed the unifier deliverable). Unwrap it so
    # reconciliation still sees the outcome even if a stage's ASL regresses.
    if "status" not in output and isinstance(output.get("Payload"), dict):
        output = output["Payload"]
    return output


def read_run_params(s3, bucket: str, jobs_key: str) -> dict:
    """Load the pipeline_init params sidecar for a run. A missing/invalid sidecar
    (legacy runs predating the feature) yields ``{}`` — the notification then shows
    "scheduled defaults" rather than erroring."""
    source, run_id = jobs_key_identity(jobs_key)
    try:
        body = s3.get_object(Bucket=bucket, Key=run_params_key(source, run_id))["Body"].read()
    except Exception as e:
        logger.info("No run params sidecar for %s (%s); treating as defaults.", jobs_key, e)
        return {}
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        logger.warning("Run params for %s is not valid JSON; ignoring.", jobs_key)
        return {}
    return data if isinstance(data, dict) else {}


def gather(s3, bucket: str, jobs_key: str) -> tuple[dict, dict, dict, list[dict]]:
    """Load all manifests, outcomes, the params sidecar, and bad_pass results.

    Manifests + outcomes are keyed by Product-pipeline name; run_params is the flat
    pipeline_init invocation sidecar (or ``{}`` when absent); bad_pass_results are the
    diagnostic bad_pass stage's raw Job results (``{date, source, count}``), read from
    its ResultWriter prefix. bad_pass has no `Output: $states.result.Payload` unwrap in
    its Map, so `read_outcomes`'s defensive envelope-unwrap is what yields the results."""
    manifests: dict[str, list[dict]] = {}
    outcomes: dict[str, dict[str, list[dict]]] = {}
    for name, cfg in PRODUCT_PIPELINES.items():
        manifests[name] = read_manifest(s3, bucket, cfg["manifest"](jobs_key))
        outcomes[name] = {
            spec["stage"]: read_outcomes(s3, bucket, stage_results_prefix(jobs_key, spec["stage"]))
            for spec in cfg["deliverables"]
        }
    run_params = read_run_params(s3, bucket, jobs_key)
    bad_pass_results = read_outcomes(s3, bucket, stage_results_prefix(jobs_key, "bad_pass"))
    return manifests, outcomes, run_params, bad_pass_results


# ─── SNS rendering ────────────────────────────────────────────────────────

# Human-friendly deliverable names for the notification. `nasa_ssh_p3` has no entry
# because it is folded into the `daily_file_p3` line as a unification annotation (the
# two are the same P3 file in two locations — see `_along_track_lines`).
_DELIVERABLE_LABELS = {
    "daily_file_p3": "p3 daily files",
    "simple_grid": "simple grids",
    "enso": "ENSO grids",
}

# Cap on filenames listed per deliverable before collapsing to a total (a force_update
# backfill can produce hundreds; a nominal weekly/monthly run lists in full).
_MAX_LISTED = 40


def _basename(key: str) -> str:
    return key.rsplit("/", 1)[-1]


def _params_line(summary: dict) -> str:
    """One line describing how the run was invoked. Nominal runs (no overrides) read
    'scheduled defaults'; absent sidecar (legacy runs) is treated the same."""
    params = summary.get("parameters") or {}
    overrides = []
    if params.get("start"):
        overrides.append(f"start={params['start']}")
    if params.get("end"):
        overrides.append(f"end={params['end']}")
    if params.get("force_update"):
        overrides.append("force_update=true")
    return "Parameters: " + (", ".join(overrides) if overrides else "none (scheduled defaults)")


def _deliverable_lines(label: str, d: dict, *, suffix: str = "") -> list[str]:
    """Header line for one deliverable (counts + any skip/lineage flags + optional
    unification suffix), followed by its produced filenames (capped)."""
    header = f"  {label} [{d['stage']}]: {d['produced']} produced{suffix}"
    flags = []
    if d.get("skipped"):
        flags.append(f"{d['skipped']} skipped")
    if d.get("provenance_incomplete"):
        flags.append(f"{d['provenance_incomplete']} incomplete-lineage")
    if flags:
        header += " (" + ", ".join(flags) + ")"

    lines = [header]
    names = [_basename(o["key"]) for o in d.get("outputs", []) if o.get("key")]
    for n in names[:_MAX_LISTED]:
        lines.append(f"    {n}")
    if len(names) > _MAX_LISTED:
        lines.append(f"    … ({len(names)} total)")
    return lines


def _along_track_lines(deliverables: dict, unified_source: str | None) -> list[str]:
    """Render the along-track deliverables, folding the unifier's `nasa_ssh_p3` into the
    finalizer's `daily_file_p3` line — they are the same P3 file, in the source prefix and
    (when unified) the NASA-SSH prefix. The fold surfaces a unification shortfall instead of
    hiding it as a separate, confusing '0 produced' row."""
    p3 = deliverables.get("daily_file_p3")
    unified = deliverables.get("nasa_ssh_p3")

    suffix = ""
    if p3 is not None and unified is not None:
        dest = unified_source or "NASA-SSH"
        uc = unified["produced"]
        suffix = f" → all unified to {dest}" if uc == p3["produced"] \
            else f" → {uc} of {p3['produced']} unified to {dest}"

    lines: list[str] = []
    for kind, d in deliverables.items():
        if kind == "nasa_ssh_p3":
            continue  # folded into the daily_file_p3 line
        label = _DELIVERABLE_LABELS.get(kind, kind)
        lines += _deliverable_lines(label, d, suffix=suffix if kind == "daily_file_p3" else "")
    return lines


def _missing_line(missing: list[dict]) -> str:
    preview = ", ".join(f"{m['date']} ({m['reason']})" for m in missing[:5])
    if len(missing) > 5:
        preview += f", … ({len(missing)} total)"
    return f"  missing: {preview}"


def _bad_pass_lines(bp: dict) -> list[str]:
    """Render the diagnostic bad-pass lines *within* the along_track section: the
    flagged total across the run and the per-date breakdown (dates with zero flags
    are not listed). Indented to match the section's deliverable lines. Empty when
    no bad_pass results were found (a run predating the stage, or none reported)."""
    if not bp or not bp.get("dates_reported"):
        return []

    total = bp.get("total_flagged", 0)
    if total == 0:
        return ["  bad passes [bad_pass]: none flagged"]

    lines = [
        f"  bad passes [bad_pass]: {total} flagged across {bp['dates_flagged']} "
        f"date{'s' if bp['dates_flagged'] != 1 else ''}"
    ]
    by_date = bp.get("by_date", [])
    for entry in by_date[:_MAX_LISTED]:
        lines.append(f"    {entry['date']}: {entry['count']}")
    if len(by_date) > _MAX_LISTED:
        lines.append(f"    … ({len(by_date)} dates total)")
    return lines


def render_notification(summary: dict) -> tuple[str, str]:
    """(subject, body) for the success SNS, rendered from the Run summary artifact."""
    source = summary["source"]
    unified = summary.get("unified_source")
    run_id = summary["run_id"]

    source_line = f"Source: {source} → {unified}" if unified else f"Source: {source}"
    lines = [
        source_line,
        f"Run ID: {run_id}",
        f"Completed: {summary['completed_at']}",
        _params_line(summary),
        "",
    ]

    for name, section in summary["product_pipelines"].items():
        lines.append(f"{name} (expected {section['expected']}):")
        if name == "along_track":
            lines += _along_track_lines(section["deliverables"], unified)
            # bad_pass runs inside the along-track chain (between xover_p2 and the
            # finalizer); its diagnostic counts belong under this section, not as a
            # trailing block.
            lines += _bad_pass_lines(summary.get("bad_passes", {}))
        else:
            for kind, d in section["deliverables"].items():
                lines += _deliverable_lines(_DELIVERABLE_LABELS.get(kind, kind), d)
        if section["missing"]:
            lines.append(_missing_line(section["missing"]))
        lines.append("")

    subject = f"Pipeline success: {source} / {run_id}"[:100]
    return subject, "\n".join(lines).rstrip()


def summary_key(jobs_key: str) -> str:
    source, run_id = jobs_key_identity(jobs_key)
    return run_summary_key(source, run_id)
