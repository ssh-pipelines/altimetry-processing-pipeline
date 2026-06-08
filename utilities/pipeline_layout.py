"""S3 bucket layout for the altimetry pipeline.

Every Lambda asks this module for keys and prefixes instead of building f-strings.
Returns bucket-relative keys; callers prepend `s3://{bucket}/` via `s3_uri()`
when they need a full URI (boto3 SDK calls take bucket+key separately;
xarray / aws_manager paths often want the URI form).

Each path family owns its date format:
    Daily files: YYYYMMDD
    Crossovers, OER, bad_pass: ISO YYYY-MM-DD
"""

from __future__ import annotations

from datetime import date as date_type
from datetime import datetime
from typing import Literal

from utilities.source_profile import SourceCommon, get_product


DailyFileVersion = Literal["p1", "p2", "p3"]
CrossoverVersion = Literal["p1", "p2"]
EnsoMapKind = Literal["ortho", "plate"]


def _to_date(d: date_type | datetime) -> date_type:
    return d.date() if isinstance(d, datetime) else d


def _along_track_product_name(product_type: str) -> str:
    return f"along_track_{product_type}"


def _simple_grid_product_name(product_type: str) -> str:
    return f"simple_grid_{product_type}"


# ─── Along-track daily files ──────────────────────────────────────────────

def daily_file_filename(profile: SourceCommon, d: date_type | datetime) -> str:
    """Just the filename (no prefix). Used by callers that already have a
    directory and only need the leaf name."""
    d = _to_date(d)
    product = get_product(_along_track_product_name(profile.product_type))
    return product.filename_template.format(
        source=profile.source,
        version=product.version,
        YYYYMMDD=d.strftime("%Y%m%d"),
    )


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

    ``source`` is the *original* source segment (e.g. ``S6``) even for a unified
    run whose manifest lives under a further ``{unified}/`` segment — it is always
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


def stage_results_prefix(jobs_key: str, stage: str) -> str:
    """Bucket-relative prefix for a stage's Distributed Map ResultWriter output.

    Mirrors the JSONata used by every leaf ASL's ResultWriter:
        $p := $split(jobs_key, '/'); $p[0]/$p[1]/$p[2]/results/{stage}/
    Must take `jobs_key` rather than (source, run_id) because post-unifier the
    SM-input `source` is the unified product (e.g. "NASA-SSH") while jobs_key
    still embeds the original source (e.g. "S6") — and the ResultWriter wrote
    under the original-source path. Listing by the unified source would miss
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


# ─── Simple grids ─────────────────────────────────────────────────────────

def simple_grid_filename(profile: SourceCommon, d: date_type | datetime) -> str:
    """Simple-grid output filename (no prefix)."""
    d = _to_date(d)
    product = get_product(_simple_grid_product_name(profile.product_type))
    return product.filename_template.format(
        source=profile.source,
        version=product.version,
        YYYYMMDD=d.strftime("%Y%m%d"),
    )


def simple_grid_key(profile: SourceCommon, d: date_type | datetime) -> str:
    """Bucket-relative key for a simple-grid output.

    Example: simple_grids/S6/2025/S6_alt_ref_simple_grid_v1_1_20250107.nc
    """
    d = _to_date(d)
    return f"simple_grids/{profile.source}/{d.year}/{simple_grid_filename(profile, d)}"


def simple_grid_prefix(source: str, year: int) -> str:
    """Listing prefix for a source/year of simple grids.

    Example: simple_grids/S6/2025/
    """
    return f"simple_grids/{source}/{year}/"


# ─── ENSO ─────────────────────────────────────────────────────────────────

def enso_filename(d: date_type | datetime) -> str:
    """ENSO grid filename (no prefix). ENSO is a global product (no source)."""
    d = _to_date(d)
    product = get_product("enso")
    return product.filename_template.format(
        version=product.version,
        YYYYMMDD=d.strftime("%Y%m%d"),
    )


def enso_grid_key(source: str, d: date_type | datetime) -> str:
    """Bucket-relative key for an ENSO grid output. Layout encodes the
    upstream simple-grid source even though ENSO itself is a global product.

    Example: enso_grids/S6/ENSO_20250107.nc
    """
    return f"enso_grids/{source}/{enso_filename(d)}"


def enso_map_key(source: str, d: date_type | datetime, kind: EnsoMapKind) -> str:
    """Bucket-relative key for an ENSO map (PNG).

    Example: maps/enso_maps/S6/ortho/ENSO_ortho_20250107.png
    """
    date_str = _to_date(d).strftime("%Y%m%d")
    return f"maps/enso_maps/{source}/{kind}/ENSO_{kind}_{date_str}.png"


# ─── Indicators ───────────────────────────────────────────────────────────

def indicators_key(source: str) -> str:
    """Bucket-relative key for the indicators artifact. Currently a static
    filename per source.

    Example: indicators/S6/indicators.nc
    """
    return f"indicators/{source}/indicators.nc"


def indicators_prefix(source: str) -> str:
    """Listing prefix for a source's indicators directory.

    Example: indicators/S6/
    """
    return f"indicators/{source}/"


# ─── Convenience: full s3:// URI ──────────────────────────────────────────

def s3_uri(bucket: str, key: str) -> str:
    """Compose an s3:// URI from a bucket name and a layout key."""
    return f"s3://{bucket}/{key}"
