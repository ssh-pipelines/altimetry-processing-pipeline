"""Reconciliation core for the ``run_summary`` Lambda (see ADR 0005).

Reconciles, per **Product pipeline**, the **Job specs** a run expected (from its manifest)
against the **Job outcomes** its deliverable stages produced (from the Distributed Map
``ResultWriter``), and assembles the **Run summary** artifact.

A run is one **Published product**'s, not one **Source**'s. Which means the along-track
side reconciles across *every* contributor: one `{published_product: "NASA-SSH"}` run fans
out over GSFC and S6, each writing its own manifest and its own ResultWriter outputs under
its own leg of the run path. The gridded side has exactly one manifest, at the product run
path. That asymmetry is the **Source tier** / **Product tier** split, and it is why this
module is handed `(published_product, run_id, contributors)` rather than a single
`jobs_key` to parse an identity out of.

The pure functions here (`reconcile_pipeline`, `build_summary`) take already-loaded data so
they are unit-testable without S3; the thin I/O helpers (`read_manifest`, `read_outcomes`)
wrap boto3 and are exercised with a stubbed client. All S3-key derivation goes through
``utilities.pipeline_layout`` — this Lambda ships ``utilities`` precisely so it can.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from utilities.job_outcome import SKIPPED, SUCCESS
from utilities.pipeline_layout import (
    jobs_manifest_key,
    run_params_key,
    run_summary_key,
    sg_jobs_key,
    stage_results_prefix,
)

logger = logging.getLogger(__name__)

# The Run summary's own wire version, deliberately not ``job_outcome.SCHEMA_VERSION``.
# The two shapes evolve independently and only this one changed: v2 drops the
# top-level `source` for `published_product` + `contributors`, because a run is no
# longer one Source's. Job outcomes are untouched and stay at their own v1.
RUN_SUMMARY_SCHEMA_VERSION = 2

# Which tier owns each Product pipeline's run path. The along-track manifests and
# ResultWriter outputs live under each *contributor's* leg; the gridded ones under
# the published product's.
SOURCE_TIER = "source"
PRODUCT_TIER = "product"


# A deliverable the run summary reconciles. `anchor` deliverables define a Product
# pipeline's `missing` set (one manifest date -> one expected anchor output).
# `conditional` deliverables are reported only when they actually ran, so a run
# that legitimately skips one does not show it as all-missing. Nothing is
# conditional today: every source publishes, so the publisher always runs.
#
# `stage` is the ResultWriter segment, not the Job outcome's own `stage` field.
# One publisher Lambda serves both tiers, so each tier's promotion lands under
# its own `publisher_{family}` segment — the same shape as `xover_{df_version}`.
PRODUCT_PIPELINES = {
    "along_track": {
        "tier": SOURCE_TIER,
        "manifest": lambda identity, run_id: jobs_manifest_key(identity, run_id),
        "deliverables": [
            {"kind": "daily_file_p3", "stage": "finalizer", "anchor": True, "conditional": False},
            {
                "kind": "published_along_track",
                "stage": "publisher_along_track",
                "anchor": False,
                "conditional": False,
            },
        ],
    },
    "gridded": {
        "tier": PRODUCT_TIER,
        "manifest": lambda identity, run_id: sg_jobs_key(jobs_manifest_key(identity, run_id)),
        "deliverables": [
            {"kind": "simple_grid", "stage": "simple_grids", "anchor": True, "conditional": False},
            {
                "kind": "published_simple_grid",
                "stage": "publisher_simple_grid",
                "anchor": False,
                "conditional": False,
            },
            {"kind": "enso", "stage": "enso", "anchor": False, "conditional": False},
        ],
    },
}


def manifest_dates(job_specs: list[dict]) -> list[str]:
    """Sorted distinct ISO dates declared by a manifest's Job specs."""
    return sorted({j["date"] for j in job_specs if isinstance(j, dict) and j.get("date")})


def _provenance_incomplete(successes: list[dict]) -> int:
    """Count outcomes whose metadata explicitly declares incomplete lineage.

    A *missing* `provenance_complete` (older emitters / the publisher byte-copy / legacy
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
            # A stage that legitimately did not run this time — not a deliverable.
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


def manifest_keys(published_product: str, run_id: str, contributors: list[str]) -> dict[str, list[str]]:
    """Each Product pipeline's manifest key(s) for a run.

    A list per pipeline rather than one key, because the along-track side has one
    manifest *per contributor* — the plural is the tier split, not defensiveness.
    """
    keys: dict[str, list[str]] = {}
    for name, cfg in PRODUCT_PIPELINES.items():
        identities = contributors if cfg["tier"] == SOURCE_TIER else [published_product]
        keys[name] = [cfg["manifest"](identity, run_id) for identity in identities]
    return keys


def build_summary(
    published_product: str,
    run_id: str,
    contributors: list[str],
    manifests: dict[str, list[dict]],
    outcomes: dict[str, dict[str, list[dict]]],
    *,
    run_params: dict | None = None,
    bad_pass_results: list[dict] | None = None,
    completed_at: str | None = None,
) -> dict:
    """Assemble the Run summary artifact.

    `manifests` maps a Product-pipeline name to its Job specs — already unioned
    across contributors on the along-track side; `outcomes` maps a Product-pipeline
    name to its `{stage: [outcome, ...]}`, likewise concatenated; `run_params` is
    `start_run`'s invocation sidecar (or `{}` for legacy runs without one);
    `bad_pass_results` are the bad_pass stage's raw Job results (or `[]`). All are
    passed in (already loaded) so this stays pure and testable.
    """
    completed_at = completed_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    keys = manifest_keys(published_product, run_id, contributors)

    product_pipelines = {}
    for name, cfg in PRODUCT_PIPELINES.items():
        section = reconcile_pipeline(
            cfg["deliverables"],
            manifest_dates(manifests.get(name, [])),
            outcomes.get(name, {}),
        )
        # A list on the along-track side: naming only one of several contributors'
        # manifests would point a reader at an incomplete picture of the run.
        section["manifests"] = keys[name]
        product_pipelines[name] = section

    return {
        "schema_version": RUN_SUMMARY_SCHEMA_VERSION,
        "run_id": run_id,
        "published_product": published_product,
        "contributors": list(contributors),
        "completed_at": completed_at,
        "parameters": run_params or {},
        "product_pipelines": product_pipelines,
        "bad_passes": summarize_bad_passes(bad_pass_results or []),
    }


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
    # Job outcome (this once silently zeroed the promotion deliverable). Unwrap it so
    # reconciliation still sees the outcome even if a stage's ASL regresses.
    if "status" not in output and isinstance(output.get("Payload"), dict):
        output = output["Payload"]
    return output


def read_run_params(s3, bucket: str, published_product: str, run_id: str) -> dict:
    """Load `start_run`'s invocation sidecar from the product run path.

    The run-level one, not a contributor's: the question it answers — how was this
    run asked for — has one answer per run, while each pipeline_init's sidecar
    records a per-Source resolved window that a multi-contributor run has no single
    value for. A missing/invalid sidecar yields ``{}`` and the notification shows
    "scheduled defaults" rather than erroring.
    """
    key = run_params_key(published_product, run_id)
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception as e:
        logger.info("No run params sidecar at %s (%s); treating as defaults.", key, e)
        return {}
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        logger.warning("Run params %s is not valid JSON; ignoring.", key)
        return {}
    return data if isinstance(data, dict) else {}


def _tier_identities(cfg: dict, published_product: str, contributors: list[str]) -> list[str]:
    """The run-path identities one Product pipeline's artifacts live under."""
    return contributors if cfg["tier"] == SOURCE_TIER else [published_product]


def gather(
    s3, bucket: str, published_product: str, run_id: str, contributors: list[str]
) -> tuple[dict, dict, dict, list[dict]]:
    """Load all manifests, outcomes, the params sidecar, and bad_pass results.

    Manifests + outcomes are keyed by Product-pipeline name. On the along-track side
    both are read once per contributor and concatenated: the run expected the union
    of their planned dates and produced the union of their outcomes, so reconciling
    one contributor's leg would report a shortfall for every other contributor's
    dates. bad_pass_results are the diagnostic bad_pass stage's raw Job results
    (``{date, source, count}``), read per contributor from its ResultWriter prefix;
    bad_pass has no `Output: $states.result.Payload` unwrap in its Map, so
    `read_outcomes`'s defensive envelope-unwrap is what yields them.
    """
    manifests: dict[str, list[dict]] = {}
    outcomes: dict[str, dict[str, list[dict]]] = {}
    for name, cfg in PRODUCT_PIPELINES.items():
        identities = _tier_identities(cfg, published_product, contributors)
        specs: list[dict] = []
        for identity in identities:
            specs += read_manifest(s3, bucket, cfg["manifest"](identity, run_id))
        manifests[name] = specs
        outcomes[name] = {
            spec["stage"]: [
                o
                for identity in identities
                for o in read_outcomes(
                    s3, bucket, stage_results_prefix(identity, run_id, spec["stage"])
                )
            ]
            for spec in cfg["deliverables"]
        }

    run_params = read_run_params(s3, bucket, published_product, run_id)
    # bad_pass runs inside each contributor's along-track chain, so its results are
    # per-Source like the rest of the source tier.
    bad_pass_results = [
        r
        for source in contributors
        for r in read_outcomes(s3, bucket, stage_results_prefix(source, run_id, "bad_pass"))
    ]
    return manifests, outcomes, run_params, bad_pass_results


# ─── SNS rendering ────────────────────────────────────────────────────────

# Human-friendly deliverable names for the notification. `published_along_track`
# has no entry because it is folded into the `daily_file_p3` line as a publication
# annotation (the two are the same P3 file, staged and released — see
# `_along_track_lines`).
_DELIVERABLE_LABELS = {
    "daily_file_p3": "p3 daily files",
    "simple_grid": "simple grids",
    "published_simple_grid": "published simple grids",
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


def _along_track_lines(deliverables: dict, published_product: str | None) -> list[str]:
    """Render the along-track deliverables, folding the publisher's
    `published_along_track` into the finalizer's `daily_file_p3` line — they are the
    same P3 file, staged under its source prefix and released under `products/`.
    The fold surfaces a publication shortfall instead of hiding it as a separate,
    confusing '0 produced' row."""
    p3 = deliverables.get("daily_file_p3")
    published = deliverables.get("published_along_track")

    suffix = ""
    if p3 is not None and published is not None:
        dest = published_product or "its published product"
        pc = published["produced"]
        suffix = f" → all published to {dest}" if pc == p3["produced"] \
            else f" → {pc} of {p3['produced']} published to {dest}"

    lines: list[str] = []
    for kind, d in deliverables.items():
        if kind == "published_along_track":
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
    published = summary["published_product"]
    contributors = summary.get("contributors") or []
    run_id = summary["run_id"]

    # The product is the subject of the run; the contributors are how it was fed.
    # A single-contributor line still lists it, so the reference line's two-mission
    # handover weeks read the same as its one-mission weeks.
    product_line = f"Product: {published}"
    if contributors:
        product_line += f" ← {', '.join(contributors)}"
    lines = [
        product_line,
        f"Run ID: {run_id}",
        f"Completed: {summary['completed_at']}",
        _params_line(summary),
        "",
    ]

    for name, section in summary["product_pipelines"].items():
        lines.append(f"{name} (expected {section['expected']}):")
        if name == "along_track":
            lines += _along_track_lines(section["deliverables"], published)
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

    subject = f"Pipeline success: {published} / {run_id}"[:100]
    return subject, "\n".join(lines).rstrip()


def summary_key(published_product: str, run_id: str) -> str:
    """Written at the product run path: a run is one published product's, even
    when several Sources contributed to it."""
    return run_summary_key(published_product, run_id)
