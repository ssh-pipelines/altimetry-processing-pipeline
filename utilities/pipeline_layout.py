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


def stage_results_prefix(stage: str) -> str:
    """Listing prefix for Distributed Map result writers.

    Example: pipeline_runs/results/daily_file/
    """
    return f"pipeline_runs/results/{stage}/"


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
