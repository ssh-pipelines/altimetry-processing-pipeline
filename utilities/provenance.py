"""In-file provenance bus for the altimetry daily-file lifecycle.

Stages never pass payloads forward — each is an independent Distributed Map keyed off the
static jobs manifest, communicating only through S3 daily-file artifacts. So lineage cannot
be threaded stage-to-stage through Step Functions; it must ride *in the file*.

The daily file already accrues NetCDF global attributes (``source_files``,
``product_generation_step``, ``history``), but ``history`` is *overwritten* at each stage and
is treated as an externally-constrained field. We add a pipeline-owned attribute,
``processing_history``: a JSON-encoded list of step records that each file-writing stage
(``daily_files`` -> P1, ``oer`` -> P2, ``finalizer`` -> P3) **appends** to (never overwrites).
The unifier's byte-level ``copy_object`` carries it into the NASA-SSH product untouched.

**Backward compatibility is first-class.** Every daily file already in S3 predates this
attribute, and reprocessing an old date reads a pre-feature upstream version. Absence is a
*normal* state: :func:`read_steps` treats a missing/blank/corrupt value as ``[]`` and never
raises. To keep *legacy/incomplete* lineage distinguishable from *complete* lineage,
:func:`processing_complete` checks for a gap in ``product_generation_step`` coverage; the
deliverable stages surface the result as ``metadata.provenance_complete`` on the Job outcome.
Missing steps are never back-filled — they are unrecoverable.

The pure functions (:func:`read_steps`, :func:`append_step`, :func:`processing_complete`) hold
the logic and are unit-tested; the dataset adapters (:func:`append_to_xr`, :func:`append_to_nc`,
:func:`read_from_nc`) are thin glue over the two carriers the pipeline uses — xarray
``Dataset.attrs`` (daily_files, oer) and netCDF4 ``Dataset`` ncattrs (finalizer).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

PROCESSING_HISTORY_ATTR = "processing_history"


def read_steps(raw: str | None) -> list[dict]:
    """Parse a ``processing_history`` attribute value into a list of step records.

    Absent (``None``), blank, or unparseable values yield ``[]`` — never an error. This is
    the load-bearing backward-compat guarantee: pre-feature files simply have no lineage.
    """
    if not raw:
        return []
    try:
        steps = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return []
    return steps if isinstance(steps, list) else []


def append_step(
    raw: str | None,
    *,
    stage: str,
    generation_step: int | str,
    **fields,
) -> str:
    """Append one step record to a ``processing_history`` value, returning the new JSON.

    Pure: takes the current attribute value (or ``None``) and returns the updated JSON
    string. Appends, never overwrites — prior steps always survive.
    """
    steps = read_steps(raw)
    step = {
        "stage": stage,
        "product_generation_step": str(generation_step),
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        **fields,
    }
    steps.append(step)
    return json.dumps(steps)


def processing_complete(steps: list[dict], target_generation_step: int | str) -> bool:
    """True iff a step exists for every generation level ``1..target``.

    A legacy file carrying only later steps (a gap at an earlier level) returns ``False`` —
    this is how consumers tell *incomplete* lineage from *complete* lineage without
    back-filling the unrecoverable steps.
    """
    target = int(target_generation_step)
    present = {
        int(s["product_generation_step"])
        for s in steps
        if "product_generation_step" in s
    }
    return all(level in present for level in range(1, target + 1))


# ─── Dataset adapters ─────────────────────────────────────────────────────

def append_to_xr(ds, *, stage: str, generation_step: int | str, **fields) -> None:
    """Append a step to an xarray ``Dataset``'s ``processing_history`` attr (in place)."""
    raw = ds.attrs.get(PROCESSING_HISTORY_ATTR)
    ds.attrs[PROCESSING_HISTORY_ATTR] = append_step(
        raw, stage=stage, generation_step=generation_step, **fields
    )


def append_to_nc(ds, *, stage: str, generation_step: int | str, **fields) -> None:
    """Append a step to a netCDF4 ``Dataset``'s ``processing_history`` attr (in place)."""
    raw = (
        ds.getncattr(PROCESSING_HISTORY_ATTR)
        if PROCESSING_HISTORY_ATTR in ds.ncattrs()
        else None
    )
    ds.setncattr(
        PROCESSING_HISTORY_ATTR,
        append_step(raw, stage=stage, generation_step=generation_step, **fields),
    )


def read_from_nc(ds) -> list[dict]:
    """Read the ``processing_history`` step list from a netCDF4 ``Dataset`` (``[]`` if absent)."""
    raw = (
        ds.getncattr(PROCESSING_HISTORY_ATTR)
        if PROCESSING_HISTORY_ATTR in ds.ncattrs()
        else None
    )
    return read_steps(raw)
