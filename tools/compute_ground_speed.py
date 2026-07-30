#!/usr/bin/env python3
"""
Compute the along-track ground speed of a satellite for OER knot placement.

A satellite's along-track ground speed is one canonical per-source quantity,
configured as `common.ground_speed`. This tool measures it for any source from
its along-trackbdaily files: for a long, nearly-complete pass, the ground speed
is the median of the first-difference of geodesic distances between successive
1 Hz measurements.

Usage (S3 mode):
    python tools/compute_ground_speed.py \\
        --source S3B \\
        --bucket your-bucket-name \\
        --version p2 \\
        --max-passes 20

Usage (local mode):
    python tools/compute_ground_speed.py \\
        --source S3B \\
        --daily-dir /data/daily_files/p2/S3B \\
        --max-passes 20

--max-passes caps how many daily files are scanned (each holds ~a day of passes);
omit it to scan every available file.

Result:
    Enter the printed value under `common` for the source in:
        utilities/sources/{source}.yaml

        common:
          ground_speed: <value>   # km/s, median 1 Hz ground speed over N long passes

Caveats:
  - This is the satellite's along-track ground speed — a SINGLE canonical value
    (`common.ground_speed`) read by BOTH oerfit knot placement and the
    daily-file smoothing filter.

Prerequisites:
    pip install boto3 xarray numpy netcdf4 h5netcdf
"""

import argparse
import os
import sys
import tempfile
from datetime import date
from pathlib import Path
from typing import Optional

import numpy as np
import xarray as xr

# tools/ is a sibling of utilities/ at the repo root.
_REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from utilities.pipeline_layout import daily_file_filename  # noqa: E402
from utilities.source_profile import get_source_profile  # noqa: E402

# Mean Earth radius (km) — same value NASA/IUGG use; haversine at 1 Hz spacing
# (~7 km) is accurate to well within the precision knot placement needs.
_EARTH_RADIUS_KM = 6371.0088

# A "nearly-complete" pass is within this fraction of the longest observed pass,
# and has at least this many points. Filters out partial/edge passes at day
# boundaries that would otherwise bias the speed low.
_COMPLETENESS_FRACTION = 0.9
_MIN_PASS_POINTS = 1000

# Successive 1 Hz points are ~1 s apart. Drop pairs whose dt strays far from
# 1 s (data gaps) before taking the median, and reject non-positive dt
# (duplicate / out-of-order timestamps).
_DT_LOW_S = 0.5
_DT_HIGH_S = 1.5


# ---------------------------------------------------------------------------
# Distance
# ---------------------------------------------------------------------------


def haversine_km(
    lat1: np.ndarray, lon1: np.ndarray, lat2: np.ndarray, lon2: np.ndarray
) -> np.ndarray:
    """Great-circle distance (km) between successive lat/lon points.

    Pure function over numpy arrays of degrees; returns an array the same shape
    as the inputs. Used elementwise on successive-point pairs.
    """
    lat1r, lon1r, lat2r, lon2r = map(np.deg2rad, (lat1, lon1, lat2, lon2))
    dlat = lat2r - lat1r
    dlon = lon2r - lon1r
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1r) * np.cos(lat2r) * np.sin(dlon / 2.0) ** 2
    return 2.0 * _EARTH_RADIUS_KM * np.arcsin(np.sqrt(a))


# ---------------------------------------------------------------------------
# Computation
# ---------------------------------------------------------------------------


def pass_ground_speed(
    time: np.ndarray, lat: np.ndarray, lon: np.ndarray
) -> Optional[float]:
    """Median 1 Hz ground speed (km/s) for a single pass, or None if unusable.

    Sorts by time, computes successive geodesic distance / time delta, drops
    samples with non-positive or non-~1 s dt, and returns the median. Robust to
    gaps and outliers.
    """
    order = np.argsort(time)
    t = time[order].astype("datetime64[ns]").astype("float64") / 1e9  # seconds
    la = lat[order]
    lo = lon[order]

    dt = np.diff(t)
    dist = haversine_km(la[:-1], lo[:-1], la[1:], lo[1:])

    good = (dt >= _DT_LOW_S) & (dt <= _DT_HIGH_S)
    if not np.any(good):
        return None

    speeds = dist[good] / dt[good]
    return float(np.median(speeds))


def compute_ground_speed(
    time: np.ndarray,
    lat: np.ndarray,
    lon: np.ndarray,
    cycle: np.ndarray,
    pass_: np.ndarray,
) -> tuple[float, list[int]]:
    """Compute the per-source ground speed from concatenated 1 Hz arrays.

    Groups points by (cycle, pass), keeps only long/nearly-complete passes,
    computes each pass's median speed, and returns the median across passes
    together with the point counts of the passes used (for sanity-checking).

    Raises ValueError if no pass qualifies.
    """
    trackids = cycle.astype("int64") * 100000 + pass_.astype("int64")
    unique_tracks, counts = np.unique(trackids, return_counts=True)

    if counts.size == 0:
        raise ValueError("No passes found in the provided data.")

    max_count = int(counts.max())
    threshold = max(_MIN_PASS_POINTS, int(_COMPLETENESS_FRACTION * max_count))

    pass_speeds: list[float] = []
    used_counts: list[int] = []
    for tid, count in zip(unique_tracks, counts):
        if count < threshold:
            continue
        sel = trackids == tid
        speed = pass_ground_speed(time[sel], lat[sel], lon[sel])
        if speed is not None and np.isfinite(speed):
            pass_speeds.append(speed)
            used_counts.append(int(count))

    if not pass_speeds:
        raise ValueError(
            f"No pass met the completeness threshold "
            f"(>= {threshold} points; longest pass had {max_count})."
        )

    return float(np.median(pass_speeds)), used_counts


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def load_daily(path: str) -> tuple[np.ndarray, ...]:
    """Load geometry (time, lat, lon, cycle, pass) from a daily file.

    Geometry only — keeps every 1 Hz point (no ssha_smoothed drop), since the
    ground speed depends solely on coordinates.
    """
    ds = xr.open_dataset(path, engine="h5netcdf")
    time = ds["time"].values
    lat = ds["latitude"].values
    lon = ds["longitude"].values
    cycle = ds["cycle"].values
    pass_ = ds["pass"].values
    ds.close()
    return time, lat, lon, cycle, pass_


def list_s3_daily_files(s3_client, bucket: str, source: str, version: str) -> list[str]:
    """Return sorted s3 keys for a source's daily files at the given version."""
    keys: list[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    prefix = f"daily_files/{version}/{source}/"
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".nc"):
                keys.append(obj["Key"])
    return sorted(keys)


def list_local_daily_files(directory: str) -> list[str]:
    """Return sorted local paths for daily files under a directory (recursive)."""
    return sorted(str(p) for p in Path(directory).rglob("*.nc"))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute a satellite's along-track ground speed for OER knot placement.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Enter the printed value under `common` for the source in:
  utilities/sources/{source}.yaml

    common:
      ground_speed: <value>   # km/s

Note: this is the SINGLE canonical value read by both OER and daily-file
smoothing (it replaces the legacy oerfit 5.7 and smoothing.speed 5.745).
        """,
    )
    parser.add_argument("--source", required=True, help="Source name (e.g. S3B)")
    parser.add_argument(
        "--version",
        default="p2",
        help="Daily-file lifecycle version to read (default: p2). Geometry is "
        "identical across versions, so this rarely matters.",
    )
    parser.add_argument(
        "--max-passes",
        type=int,
        default=None,
        metavar="N",
        help="Cap the number of daily files scanned (default: all available).",
    )

    # Data source: S3 or local
    parser.add_argument(
        "--bucket",
        default=None,
        help="S3 bucket containing daily files (S3 mode)",
    )
    parser.add_argument(
        "--daily-dir",
        default=None,
        metavar="DIR",
        help="Local directory containing daily files (local mode, searched recursively)",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="AWS profile name (S3 mode only)",
    )
    args = parser.parse_args()

    using_s3 = args.bucket is not None
    using_local = args.daily_dir is not None
    if using_s3 == using_local:
        parser.error("Provide exactly one of --bucket (S3 mode) or --daily-dir (local mode).")

    # Confirm the source is configured (raises a helpful error otherwise) and
    # match filenames the pipeline actually writes.
    profile = get_source_profile(args.source)

    # Discover files
    if using_s3:
        import boto3

        session = boto3.Session(profile_name=args.profile)
        s3 = session.client("s3")
        print(f"Listing daily files in s3://{args.bucket}/daily_files/{args.version}/{args.source}/ ...")
        files = list_s3_daily_files(s3, args.bucket, args.source, args.version)
    else:
        print(f"Scanning {args.daily_dir} ...")
        all_files = list_local_daily_files(args.daily_dir)
        # Keep only files whose leaf name matches this source's daily-file
        # convention, so a mixed directory doesn't pull in the wrong source.
        suffix = daily_file_filename(profile, date(2000, 1, 1))
        prefix = suffix.split("2000")[0]  # e.g. "S3B_alt_hilat_at_v1_1_"
        files = [f for f in all_files if Path(f).name.startswith(prefix)]

    if args.max_passes is not None:
        files = files[: args.max_passes]

    print(f"  {len(files)} daily file(s) to scan.")
    if not files:
        print("Error: no daily files found — cannot compute ground speed.", file=sys.stderr)
        sys.exit(1)

    # Load and concatenate geometry
    time_chunks, lat_chunks, lon_chunks, cycle_chunks, pass_chunks = [], [], [], [], []

    tmp_ctx = tempfile.TemporaryDirectory() if using_s3 else None
    tmp_dir = tmp_ctx.name if tmp_ctx else None
    try:
        for i, f in enumerate(files, 1):
            print(f"  [{i:>4}/{len(files)}] {Path(f).name}", end="\r", flush=True)
            if using_s3:
                local_path = os.path.join(tmp_dir, f.replace("/", "_"))
                s3.download_file(args.bucket, f, local_path)
                t, la, lo, cy, ps = load_daily(local_path)
                os.remove(local_path)
            else:
                t, la, lo, cy, ps = load_daily(f)
            time_chunks.append(t)
            lat_chunks.append(la)
            lon_chunks.append(lo)
            cycle_chunks.append(cy)
            pass_chunks.append(ps)
    finally:
        if tmp_ctx is not None:
            tmp_ctx.cleanup()

    time = np.concatenate(time_chunks)
    lat = np.concatenate(lat_chunks)
    lon = np.concatenate(lon_chunks)
    cycle = np.concatenate(cycle_chunks)
    pass_ = np.concatenate(pass_chunks)
    print(f"\nLoaded {len(time)} 1 Hz points.")

    ground_speed, used_counts = compute_ground_speed(time, lat, lon, cycle, pass_)

    print("\nResults:")
    print(f"  Ground speed ({args.source}): {ground_speed:.4f} km/s")
    print(
        f"  Passes used: {len(used_counts)}  "
        f"(point counts {min(used_counts)}–{max(used_counts)})"
    )
    print("\nAdd under `common` in utilities/sources/{}.yaml:".format(args.source))
    print("  common:")
    print(
        f"    ground_speed: {ground_speed:.4f}   "
        f"# km/s, median 1 Hz ground speed over {len(used_counts)} long passes"
    )


if __name__ == "__main__":
    main()
