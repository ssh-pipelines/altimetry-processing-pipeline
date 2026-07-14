#!/usr/bin/env python3
"""
Compute the mean SSHA offset between a new source and a reference mission.

When adding a new altimetry source, this tool compares its simple grid products
against a reference mission over an overlapping time window and computes the
area-weighted global mean difference. That difference is the offset value to
configure in the finalizer so the new source is brought into alignment with
the reference.

Usage (S3 mode):
    python tools/compute_source_offset.py \\
        --source S6B \\
        --reference GSFC_6.1 \\
        --bucket your-bucket-name \\
        --duration 6

Usage (local mode):
    python tools/compute_source_offset.py \\
        --source S6B \\
        --reference GSFC_6.1 \\
        --source-dir /data/grids/S6B \\
        --reference-dir /data/grids/GSFC_6.1 \\
        --duration 6

    # Save the mean difference map for visual inspection:
    python tools/compute_source_offset.py ... --output-dir ./offset_analysis

--duration selects the most recent N calendar months of overlapping data.
Omit it to use all available overlap.

Offset convention:
    offset = area_weighted_mean(reference_ssha - source_ssha)

    This value is *added* to the source ssha in the finalizer (p2 → p3), so:
      - Positive offset: source reads lower than reference → add to bring it up
      - Negative offset: source reads higher than reference → subtract to bring it down

    Enter the result under the new source in:
        pipeline/daily_file_gen/finalizer/finalization/config/sources.yaml

Prerequisites:
    pip install boto3 xarray numpy netcdf4
"""

import argparse
import calendar
import os
import sys
import tempfile
from datetime import date
from pathlib import Path
from typing import Optional

import numpy as np
import xarray as xr

# Grid cell areas file, used by indicators for the same area-weighting
_REPO_ROOT = Path(__file__).parent.parent
_AREA_WEIGHTS_PATH = (
    _REPO_ROOT
    / "pipeline/other_products/indicators/ref_files/half_deg_grid_cell_areas.nc"
)

# Same latitude bounds used by indicators for global mean
_LAT_BOUNDS = (-66.0, 66.0)


# ---------------------------------------------------------------------------
# Grid discovery
# ---------------------------------------------------------------------------


def _parse_grid_date(filename: str) -> Optional[date]:
    """Parse the date from a simple grid filename, or return None."""
    if "quart" in filename:
        return None
    try:
        date_str = filename.split("_")[-1].replace(".nc", "")
        return date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
    except (ValueError, IndexError):
        return None


def list_s3_grids(s3_client, bucket: str, source: str) -> dict[date, str]:
    """Return {date: s3_key} for all half-degree simple grids for source in bucket."""
    keys: dict[date, str] = {}
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"simple_grids/{source}/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            d = _parse_grid_date(key.split("/")[-1])
            if d is not None:
                keys[d] = key
    return keys


def list_local_grids(directory: str) -> dict[date, str]:
    """Return {date: local_path} for all half-degree simple grids in a directory."""
    keys: dict[date, str] = {}
    for path in Path(directory).glob("*.nc"):
        d = _parse_grid_date(path.name)
        if d is not None:
            keys[d] = str(path)
    return keys


def select_dates(
    common_dates: list[date], duration_months: Optional[int]
) -> list[date]:
    """
    Return the most recent `duration_months` calendar months of common_dates,
    or all of them if duration_months is None.
    """
    if not common_dates or duration_months is None:
        return common_dates

    latest = max(common_dates)
    cutoff_month = latest.month - duration_months % 12
    cutoff_year = latest.year - duration_months // 12
    if cutoff_month <= 0:
        cutoff_month += 12
        cutoff_year -= 1
    max_day = calendar.monthrange(cutoff_year, cutoff_month)[1]
    cutoff = date(cutoff_year, cutoff_month, min(latest.day, max_day))
    return [d for d in common_dates if d >= cutoff]


# ---------------------------------------------------------------------------
# Grid loading
# ---------------------------------------------------------------------------


def load_ssha(path: str) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Load ssha from a simple grid NetCDF, masking land and excluded cells.

    Returns (ssha, lats, lons) where invalid cells are NaN.
    """
    ds = xr.open_dataset(path)
    ssha = ds["ssha"].values.astype(np.float64)
    basin_flag = ds["basin_flag"].values
    lats = ds["latitude"].values
    lons = ds["longitude"].values
    ds.close()

    # basin_flag: 0 = land, 1–999 = valid ocean, ≥1000 = excluded
    invalid = (basin_flag <= 0) | (basin_flag >= 1000)
    ssha[invalid] = np.nan
    return ssha, lats, lons


def fetch_s3_grid(
    s3_client, bucket: str, key: str, tmp_dir: str
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Download a grid from S3 to a temp file, load it, then delete the temp file."""
    local_path = os.path.join(tmp_dir, key.replace("/", "_"))
    s3_client.download_file(bucket, key, local_path)
    result = load_ssha(local_path)
    os.remove(local_path)
    return result


# ---------------------------------------------------------------------------
# Computation
# ---------------------------------------------------------------------------


def load_area_weights(lats: np.ndarray) -> np.ndarray:
    """
    Load grid cell area weights restricted to LAT_BOUNDS.

    Returns a 2D array (n_lats_in_bounds, n_lons). Falls back to cos(lat)
    weighting if the reference file is not found.
    """
    if _AREA_WEIGHTS_PATH.exists():
        ds = xr.open_dataset(_AREA_WEIGHTS_PATH)
        weights = ds.sel(latitude=slice(*_LAT_BOUNDS), drop=True)["area"].values
        ds.close()
        return weights
    else:
        print(
            f"Warning: area weights file not found at {_AREA_WEIGHTS_PATH}.\n"
            "Falling back to cos(lat) weighting.",
            file=sys.stderr,
        )
        lat_mask = (lats >= _LAT_BOUNDS[0]) & (lats <= _LAT_BOUNDS[1])
        cos_weights = np.cos(np.deg2rad(lats[lat_mask]))
        return np.outer(cos_weights, np.ones(720))


def area_weighted_mean(
    diff_map: np.ndarray, area_weights: np.ndarray, lats: np.ndarray
) -> float:
    """Compute area-weighted global mean of diff_map within LAT_BOUNDS."""
    lat_mask = (lats >= _LAT_BOUNDS[0]) & (lats <= _LAT_BOUNDS[1])
    subset = diff_map[lat_mask, :]
    valid = ~np.isnan(subset)
    if not np.any(valid):
        return np.nan
    weighted_sum = np.nansum(subset * area_weights)
    total_area = np.nansum(area_weights[valid])
    return float(weighted_sum / total_area)


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def save_difference_map(
    mean_diff: np.ndarray,
    lats: np.ndarray,
    lons: np.ndarray,
    source: str,
    reference: str,
    dates_used: list[date],
    output_dir: str,
) -> str:
    """Save the mean difference map as a NetCDF file. Returns the saved path."""
    start, end = min(dates_used), max(dates_used)
    ds = xr.Dataset(
        {
            "ssha_diff": (
                ["latitude", "longitude"],
                mean_diff,
                {
                    "long_name": f"Mean SSHA difference ({reference} minus {source})",
                    "units": "m",
                    "comment": (
                        f"Mean of {len(dates_used)} daily difference maps over "
                        f"{start} to {end}. "
                        f"Positive values indicate {reference} > {source}."
                    ),
                },
            )
        },
        coords={"latitude": lats, "longitude": lons},
        attrs={
            "title": f"SSHA offset difference map: {reference} minus {source}",
            "source": source,
            "reference": reference,
            "time_coverage_start": str(start),
            "time_coverage_end": str(end),
            "n_dates": len(dates_used),
            "conventions": "CF-1.9",
        },
    )

    filename = f"offset_diff_{source}_vs_{reference}_{start}_{end}.nc"
    out_path = os.path.join(output_dir, filename)
    ds.to_netcdf(out_path)
    return out_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute the SSHA offset between a new source and a reference mission.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Offset convention:
  offset = area_weighted_mean(reference_ssha - source_ssha)

  Positive: source reads lower than reference (offset will raise it)
  Negative: source reads higher than reference (offset will lower it)

Enter the result in:
  pipeline/daily_file_gen/finalizer/finalization/config/sources.yaml
        """,
    )
    parser.add_argument(
        "--source",
        required=True,
        help="New source name (e.g. S6B)",
    )
    parser.add_argument(
        "--reference",
        required=True,
        help="Reference source name (e.g. GSFC_6.1)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=None,
        metavar="MONTHS",
        help="Use the most recent N calendar months of overlap (default: all available)",
    )

    # Data source: S3 or local
    parser.add_argument(
        "--bucket",
        default=None,
        help="S3 bucket containing simple grids (S3 mode)",
    )
    parser.add_argument(
        "--source-dir",
        default=None,
        metavar="DIR",
        help="Local directory containing source simple grids (local mode)",
    )
    parser.add_argument(
        "--reference-dir",
        default=None,
        metavar="DIR",
        help="Local directory containing reference simple grids (local mode)",
    )

    parser.add_argument(
        "--output-dir",
        default=None,
        metavar="DIR",
        help="Save the mean difference map NetCDF here (optional)",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="AWS profile name (S3 mode only)",
    )
    args = parser.parse_args()

    # Validate mode selection
    using_s3 = args.bucket is not None
    using_local = args.source_dir is not None or args.reference_dir is not None
    if using_s3 and using_local:
        parser.error("Use either --bucket (S3 mode) or --source-dir/--reference-dir (local mode), not both.")
    if not using_s3 and not using_local:
        parser.error("Provide either --bucket (S3 mode) or both --source-dir and --reference-dir (local mode).")
    if using_local and not (args.source_dir and args.reference_dir):
        parser.error("--source-dir and --reference-dir must both be provided for local mode.")

    # Discover available grids
    if using_s3:
        import boto3

        session = boto3.Session(profile_name=args.profile)
        s3 = session.client("s3")
        print(f"Listing grids in s3://{args.bucket}/simple_grids/ ...")
        source_grids = list_s3_grids(s3, args.bucket, args.source)
        ref_grids = list_s3_grids(s3, args.bucket, args.reference)
    else:
        print("Scanning local directories ...")
        source_grids = list_local_grids(args.source_dir)
        ref_grids = list_local_grids(args.reference_dir)

    all_common = sorted(source_grids.keys() & ref_grids.keys())
    selected = select_dates(all_common, args.duration)

    duration_label = f"{args.duration}-month" if args.duration else "full"
    print(f"  {args.source}:    {len(source_grids)} grids available")
    print(f"  {args.reference}: {len(ref_grids)} grids available")
    print(f"  Overlapping dates: {len(all_common)}  →  using {len(selected)} ({duration_label} window)")

    if not selected:
        print("Error: no dates selected — cannot compute offset.", file=sys.stderr)
        sys.exit(1)

    if selected:
        print(f"  Window: {min(selected)} to {max(selected)}")

    # Compute difference maps
    diff_maps: list[np.ndarray] = []
    lats = lons = None

    if using_s3:
        tmp_ctx = tempfile.TemporaryDirectory()
        tmp_dir = tmp_ctx.__enter__()
    else:
        tmp_ctx = None
        tmp_dir = None

    try:
        for i, d in enumerate(selected, 1):
            print(f"  [{i:>3}/{len(selected)}] {d}", end="\r", flush=True)

            if using_s3:
                src_ssha, lats, lons = fetch_s3_grid(s3, args.bucket, source_grids[d], tmp_dir)
                ref_ssha, _, _ = fetch_s3_grid(s3, args.bucket, ref_grids[d], tmp_dir)
            else:
                src_ssha, lats, lons = load_ssha(source_grids[d])
                ref_ssha, _, _ = load_ssha(ref_grids[d])

            diff_maps.append(ref_ssha - src_ssha)
    finally:
        if tmp_ctx is not None:
            tmp_ctx.__exit__(None, None, None)

    print(f"\nProcessed {len(diff_maps)} date pairs.")

    diff_stack = np.stack(diff_maps, axis=0)
    mean_diff = np.nanmean(diff_stack, axis=0)

    area_weights = load_area_weights(lats)
    offset = area_weighted_mean(mean_diff, area_weights, lats)

    print("\nResults:")
    print(f"  Offset ({args.reference} - {args.source}): {offset:+.4f} m  ({offset * 100:+.2f} cm)")
    print("\nAdd to pipeline/daily_file_gen/finalizer/finalization/config/sources.yaml:")
    print(f"  {args.source}:")
    print(f"    offset: {offset:.4f}")

    if args.output_dir:
        os.makedirs(args.output_dir, exist_ok=True)
        out_path = save_difference_map(
            mean_diff, lats, lons, args.source, args.reference, selected, args.output_dir
        )
        print(f"\nDifference map saved to: {out_path}")


if __name__ == "__main__":
    main()
