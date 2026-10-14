"""S3 bucket layout for the altimetry pipeline.

Every Lambda asks this module for keys and prefixes instead of building f-strings.
Returns bucket-relative keys; callers prepend `s3://{bucket}/` via `s3_uri()`
when they need a full URI (boto3 SDK calls take bucket+key separately;
xarray / aws_manager paths often want the URI form).

Two areas of the bucket, and the split is the point. `daily_files/`,
`crossovers/`, `oer/`, `bad_passes/`, `simple_grids/`, `enso_grids/`, `maps/`
and `pipeline_runs/` are **internal staging** — working state, keyed on Source.
`products/` holds the released artifacts, keyed on **Published product**, and
every key under it comes from one `published_*` function here, so a later move
to a separate products bucket is this one module plus a config value.

Each path family owns its date format:
    Daily files: YYYYMMDD
    Crossovers, OER, bad_pass: ISO YYYY-MM-DD
"""

from __future__ import annotations

from datetime import date as date_type
from datetime import datetime
from typing import Literal

from utilities.products import (
    along_track_product,
    published_along_track_product,
    published_simple_grid_product,
)
from utilities.source_profile import Product, SourceCommon, get_product

DailyFileVersion = Literal["p1", "p2", "p3"]
CrossoverVersion = Literal["p1", "p2"]
EnsoMapKind = Literal["ortho", "plate"]

DEVELOPMENT_GRID_PRODUCT = "simple_grid_development"


def _to_date(d: date_type | datetime) -> date_type:
    return d.date() if isinstance(d, datetime) else d


def _internal_filename(product: Product, source: str, d: date_type | datetime) -> str:
    return product.filename_template.format(
        source=source,
        version=product.version,
        YYYYMMDD=_to_date(d).strftime("%Y%m%d"),
    )


def _published_filename(product: Product, identity: str, d: date_type | datetime) -> str:
    if not product.published_filename_template:
        raise ValueError(
            f"Product '{product.name}' declares no published_filename_template, "
            f"so it cannot be published under products/."
        )
    return product.published_filename_template.format(
        published_name=identity,
        version=product.version,
        YYYYMMDD=_to_date(d).strftime("%Y%m%d"),
    )


# ─── Along-track daily files ──────────────────────────────────────────────

def daily_file_filename(profile: SourceCommon, d: date_type | datetime) -> str:
    """Just the filename (no prefix). Used by callers that already have a
    directory and only need the leaf name."""
    return _internal_filename(along_track_product(profile), profile.source, d)


def daily_file_key(
    profile: SourceCommon,
    d: date_type | datetime,
    version: DailyFileVersion,
) -> str:
    """Bucket-relative key for a source's daily file at the given lifecycle stage.

    Example: daily_files/p2/S6/2025/S6_alt_ref_at_v1_1_20250107.nc
    """
    d = _to_date(d)
    return f"daily_files/{version}/{profile.source}/{d.year}/{daily_file_filename(profile, d)}"


def daily_file_prefix(
    source: str,
    year: int,
    version: DailyFileVersion,
) -> str:
    """Listing prefix for daily files for a source/year/version.

    Example: daily_files/p3/S6/2025/
    """
    return f"daily_files/{version}/{source}/{year}/"


# ─── Crossovers ───────────────────────────────────────────────────────────

def crossover_filename(source: str, d: date_type | datetime) -> str:
    """Crossover filename (ISO date convention).

    Example: xovers_S6-2025-01-07.nc
    """
    return f"xovers_{source}-{_to_date(d).isoformat()}.nc"


def crossover_key(source: str, d: date_type | datetime, version: CrossoverVersion) -> str:
    """Bucket-relative key for a crossover file. ISO date in filename.

    Example: crossovers/p2/S6/2025/xovers_S6-2025-01-07.nc
    """
    d = _to_date(d)
    return f"crossovers/{version}/{source}/{d.year}/{crossover_filename(source, d)}"


def crossover_prefix(source: str, year: int, version: CrossoverVersion) -> str:
    """Listing prefix for crossovers — used by OER's 10-day-window scan."""
    return f"crossovers/{version}/{source}/{year}/"


# ─── OER artifacts (polygons + corrections) ───────────────────────────────

def oer_polygon_key(source: str, d: date_type | datetime) -> str:
    """Bucket-relative key for an OER polygon fit. ISO date in filename.

    Example: oer/S6/2025/oerpoly_S6_2025-01-07.nc
    """
    d = _to_date(d)
    return f"oer/{source}/{d.year}/oerpoly_{source}_{d.isoformat()}.nc"


def oer_correction_key(source: str, d: date_type | datetime) -> str:
    """Bucket-relative key for an OER correction file. ISO date in filename.

    Example: oer/S6/2025/oer_correction_S6_2025-01-07.nc
    """
    d = _to_date(d)
    return f"oer/{source}/{d.year}/oer_correction_{source}_{d.isoformat()}.nc"


# ─── Bad passes ───────────────────────────────────────────────────────────

def bad_pass_key(source: str, d: date_type | datetime) -> str:
    """Bucket-relative key for a bad-pass JSON file. ISO date filename.

    Example: bad_passes/S6/2025-01-07.json
    """
    return f"bad_passes/{source}/{_to_date(d).isoformat()}.json"


# ─── Pipeline runs (jobs manifest + Distributed Map results) ──────────────

def jobs_manifest_key(source: str, run_id: str) -> str:
    """Bucket-relative key for the jobs manifest written by pipeline_init.

    Example: pipeline_runs/S6/20250219T120000/jobs.json
    """
    return f"pipeline_runs/{source}/{run_id}/jobs.json"


def jobs_key_identity(jobs_key: str) -> tuple[str, str]:
    """The ``(source, run_id)`` embedded in a jobs manifest key.

    ``source`` is the *original* source segment (e.g. ``S6``) even for a gridded-tier
    manifest living under a further ``{published_product}/`` segment — it is always
    ``parts[1]``, matching ``stage_results_prefix``'s first-three-segments rule.

    Example: jobs_key_identity("pipeline_runs/S6/20250528T120000/jobs.json")
             → ("S6", "20250528T120000")
    """
    parts = jobs_key.split("/")
    if len(parts) < 3:
        raise ValueError(f"jobs_key too short to derive identity: {jobs_key!r}")
    return parts[1], parts[2]


def sg_jobs_key(jobs_key: str) -> str:
    """The gridded (simple-grid/ENSO) manifest key for a run, derived from the
    along-track jobs manifest key. Mirrors the exact convention `set_sg_jobs`
    writes by, so the two stay coupled to one definition.

    Example: sg_jobs_key("pipeline_runs/S6/20250528T120000/NASA-SSH/jobs.json")
             → "pipeline_runs/S6/20250528T120000/NASA-SSH/sg_jobs.json"
    """
    return jobs_key.replace("/jobs.json", "/sg_jobs.json")


def run_summary_key(source: str, run_id: str) -> str:
    """Bucket-relative key for a run's Run summary artifact, written alongside
    the jobs manifest.

    Example: pipeline_runs/S6/20250528T120000/summary.json
    """
    return f"pipeline_runs/{source}/{run_id}/summary.json"


def run_params_key(source: str, run_id: str) -> str:
    """Bucket-relative key for a run's parameters sidecar, written by pipeline_init
    alongside the jobs manifest. Records the overrides the run was launched with
    (start/end/force_update); a nominal scheduled run records the resolved defaults.
    The Run summary reads it back so the notification can show how a run was invoked.

    Example: pipeline_runs/S6/20250528T120000/run_params.json
    """
    return f"pipeline_runs/{source}/{run_id}/run_params.json"


def stage_results_prefix(jobs_key: str, stage: str) -> str:
    """Bucket-relative prefix for a stage's Distributed Map ResultWriter output.

    Mirrors the JSONata used by every leaf ASL's ResultWriter:
        $p := $split(jobs_key, '/'); $p[0]/$p[1]/$p[2]/results/{stage}/
    Must take `jobs_key` rather than (source, run_id) because in the gridded tier
    the SM input is keyed on the published product (e.g. "NASA-SSH") while jobs_key
    still embeds the original source (e.g. "S6") — and the ResultWriter wrote
    under the original-source path. Listing by the published product would miss
    everything.

    The actual on-disk layout under this prefix is `{MapRunArn}/{FAILED,SUCCEEDED,...}_n.json`;
    callers list under this prefix to discover failed items without needing the MapRunArn
    in advance.

    Example:
        stage_results_prefix("pipeline_runs/S6/20250528T120000/NASA-SSH/sg_jobs.json", "enso")
        → "pipeline_runs/S6/20250528T120000/results/enso/"
    """
    parts = jobs_key.split("/")
    if len(parts) < 3:
        raise ValueError(f"jobs_key too short to derive results prefix: {jobs_key!r}")
    return f"{parts[0]}/{parts[1]}/{parts[2]}/results/{stage}/"


# ─── Simple grids (internal staging) ──────────────────────────────────────
#
# Keyed on the grid's own identity, not on a Source: a grid is composed from a
# contributor list and the output identity is an independent fact. ENSO and
# indicators read these staged grids, which is why promotion to `products/` is a
# copy rather than a move.

def simple_grid_key(publish_as: str, d: date_type | datetime) -> str:
    """Bucket-relative key for a published-destined simple grid's staged copy.

    Example: simple_grids/NASA-SSH/2025/NASA-SSH_alt_ref_simple_grid_v1_1_20250107.nc
    """
    d = _to_date(d)
    product = published_simple_grid_product(publish_as)
    return (
        f"simple_grids/{publish_as}/{d.year}/"
        f"{_internal_filename(product, publish_as, d)}"
    )


def development_simple_grid_key(identity: str, d: date_type | datetime) -> str:
    """Bucket-relative key for a grid run with no `publish_as`.

    Its own prefix segment and its own `_dev_` filename family, so a development
    grid can be mistaken for a product in neither the path nor the file.

    Example: simple_grids/development/NASA-SSH_S3B/2025/NASA-SSH_S3B_dev_simple_grid_v1_1_20250107.nc
    """
    d = _to_date(d)
    product = get_product(DEVELOPMENT_GRID_PRODUCT)
    return (
        f"simple_grids/development/{identity}/{d.year}/"
        f"{_internal_filename(product, identity, d)}"
    )


def simple_grid_prefix(identity: str, year: int) -> str:
    """Listing prefix for a grid identity's year of staged simple grids.

    Example: simple_grids/NASA-SSH/2025/
    """
    return f"simple_grids/{identity}/{year}/"


# ─── ENSO (internal: website assets, not a released product) ──────────────

def enso_filename(d: date_type | datetime) -> str:
    """ENSO grid filename (no prefix). ENSO carries no per-product identity."""
    d = _to_date(d)
    product = get_product("enso")
    return product.filename_template.format(
        version=product.version,
        YYYYMMDD=d.strftime("%Y%m%d"),
    )


def enso_grid_key(identity: str, d: date_type | datetime) -> str:
    """Bucket-relative key for an ENSO grid output. The segment is the published
    product the upstream simple grid belongs to — ENSO derives from the reference
    line only.

    Example: enso_grids/NASA-SSH/ENSO_20250107.nc
    """
    return f"enso_grids/{identity}/{enso_filename(d)}"


def enso_map_key(identity: str, d: date_type | datetime, kind: EnsoMapKind) -> str:
    """Bucket-relative key for an ENSO map (PNG).

    Example: maps/enso_maps/NASA-SSH/ortho/ENSO_ortho_20250107.png
    """
    date_str = _to_date(d).strftime("%Y%m%d")
    return f"maps/enso_maps/{identity}/{kind}/ENSO_{kind}_{date_str}.png"


# ─── Published products ───────────────────────────────────────────────────
#
# Product-first, so everything about one released line groups together. Every
# key under `products/` comes from here.

def published_along_track_filename(identity: str, d: date_type | datetime) -> str:
    """Published along-track filename (no prefix).

    Example: NASA-SSH_S3B_alt_hilat_at_v1_1_20250107.nc
    """
    return _published_filename(published_along_track_product(identity), identity, d)


def published_along_track_key(identity: str, d: date_type | datetime) -> str:
    """Bucket-relative key for a released along-track daily file.

    Example: products/NASA-SSH/along_track/2025/NASA-SSH_alt_ref_at_v1_1_20250107.nc
    """
    d = _to_date(d)
    return (
        f"products/{identity}/along_track/{d.year}/"
        f"{published_along_track_filename(identity, d)}"
    )


def published_simple_grid_filename(identity: str, d: date_type | datetime) -> str:
    """Published simple-grid filename (no prefix)."""
    return _published_filename(published_simple_grid_product(identity), identity, d)


def published_simple_grid_key(identity: str, d: date_type | datetime) -> str:
    """Bucket-relative key for a released simple grid.

    Example: products/NASA-SSH/simple_grid/2025/NASA-SSH_alt_ref_simple_grid_v1_1_20250107.nc
    """
    d = _to_date(d)
    return (
        f"products/{identity}/simple_grid/{d.year}/"
        f"{published_simple_grid_filename(identity, d)}"
    )


def published_indicators_key(identity: str) -> str:
    """Bucket-relative key for the indicators artifact. Indicators write through
    to `products/` rather than being promoted — nothing outside the stage reads
    them, and `indicators.nc` is self-read as a cache, so the published copy *is*
    the cache. Static filename per published product.

    Example: products/NASA-SSH/indicators/indicators.nc
    """
    return f"{published_indicators_prefix(identity)}indicators.nc"


def published_indicators_prefix(identity: str) -> str:
    """Prefix holding a published product's indicators: the `.nc` cache, the
    PO.DAAC-facing `.txt`/`.mp` pairs, and their `archive/`.

    Example: products/NASA-SSH/indicators/
    """
    return f"products/{identity}/indicators/"


# ─── Convenience: full s3:// URI ──────────────────────────────────────────

def s3_uri(bucket: str, key: str) -> str:
    """Compose an s3:// URI from a bucket name and a layout key."""
    return f"s3://{bucket}/{key}"
