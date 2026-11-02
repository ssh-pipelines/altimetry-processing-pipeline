#!/usr/bin/env python3
"""
Build P1 daily files for a THREDDS (AVISO L2P) Source on local disk, without AWS.

This is onboarding pass 1 (docs/HIGH_LAT_ONBOARDING.md, step 3) run locally:
pass 1 exists only to measure `ground_speed` and `sample_period`, which depend
on coordinates and timestamps the daily file carries unchanged, so its output
never needs to reach the bucket. Discovery is the same `ThreddsEnumerator`
pipeline_init uses and processing is the same `DailyFileJob` the daily_files
Lambda runs; only the upload is replaced by a local write.

Usage:
    uv run python tools/local_daily_files.py \\
        --source C2 --start 2018-07-01 --end 2018-07-14 --out-dir /tmp/c2_pass1

    uv run python tools/compute_ground_speed.py --source C2 --daily-dir /tmp/c2_pass1

Files land at <out-dir>/daily_files/p1/<SOURCE>/<year>/<filename>, mirroring the
bucket key. Dates already written are skipped unless --overwrite, so an
interrupted run resumes. Only dates with granules are built: the empty files
pipeline_init plans for interior gaps carry no geometry to measure.

AVISO credentials resolve as in the Lambdas (utilities/aviso_auth.py): the
AVISO_auth secret if AWS credentials happen to be present, else AVISO_USER /
AVISO_PASS, else ~/.netrc for aviso.altimetry.fr.

The source YAML must already have `pipeline_init:` and `daily_files:` sections
and a SOURCE_REGISTRY entry. Placeholder `ground_speed` / `sample_period` are
expected here; the smoothing they produce is wrong by design and discarded.
"""

import argparse
import logging
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

import numpy as np
import xarray as xr

# A tool, not a stage: it imports two stage roots side by side. Their top-level
# names do not collide except `app`, which is never imported here.
_REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "pipeline" / "daily_file_gen" / "pipeline_init"))
sys.path.insert(0, str(_REPO_ROOT / "pipeline" / "daily_file_gen" / "daily_files"))

from config.source_config import get_source_config as get_init_config  # noqa: E402
from daily_files.daily_file_job import DailyFileJob, build_daily_ds, save_ds  # noqa: E402
from enumeration.thredds import ThreddsEnumerator  # noqa: E402

from utilities.pipeline_layout import daily_file_key  # noqa: E402


def granules_by_date(refs) -> dict[date, list[str]]:
    """Group enumerated GranuleRefs into the per-date URI lists a manifest carries,
    ordered as plan_jobs orders them."""
    by_date = defaultdict(list)
    for r in refs:
        by_date[r.date].append(r)
    return {
        d: [r.uri for r in sorted(rs, key=lambda r: (r.sort_key, r.uri))]
        for d, rs in sorted(by_date.items())
    }


def local_p1_path(out_dir: Path, profile, d: date) -> Path:
    return out_dir / daily_file_key(profile, d, "p1")


def summarize(ds: xr.Dataset) -> dict:
    """The step 3 sanity checks for one P1 file.

    `ssha_median` is over points with nasa_flag == 0 and finite ssha, so flagged
    land and ice points do not move it.
    """
    n = ds.sizes["time"]
    if n == 0:
        return {"points": 0}
    ssha = ds["ssha"].values
    good = (ds["nasa_flag"].values == 0) & np.isfinite(ssha)
    lat = ds["latitude"].values
    return {
        "points": n,
        "smoothed_finite": float(np.isfinite(ds["ssha_smoothed"].values).mean()),
        "lat_min": float(np.nanmin(lat)),
        "lat_max": float(np.nanmax(lat)),
        "ssha_median": float(np.median(ssha[good])) if good.any() else float("nan"),
    }


def format_summary(d: date, s: dict) -> str:
    if s["points"] == 0:
        return f"  {d}  empty"
    return (
        f"  {d}  {s['points']:>6d} pts  smoothed finite {s['smoothed_finite']:.1%}  "
        f"lat {s['lat_min']:.1f}..{s['lat_max']:.1f}  ssha median {s['ssha_median']:+.3f} m"
    )


def build(source: str, start: date, end: date, out_dir: Path, overwrite: bool = False) -> list[Path]:
    init_config = get_init_config(source)
    if init_config.discovery_type != "thredds":
        raise SystemExit(
            f"{source} uses discovery_type {init_config.discovery_type!r}; this tool "
            "drives THREDDS sources only."
        )
    start = max(start, init_config.start_date)
    if init_config.end_date is not None:
        end = min(end, init_config.end_date)
    if start > end:
        raise SystemExit(f"Window is outside {source}'s configured date range.")

    by_date = granules_by_date(ThreddsEnumerator(init_config).enumerate(start, end))
    if not by_date:
        raise SystemExit(f"No {source} granules upstream in [{start}, {end}].")

    written: list[Path] = []
    for d, uris in by_date.items():
        job = DailyFileJob(d.isoformat(), source)
        path = local_p1_path(out_dir, job.source_config, d)
        if path.exists() and not overwrite:
            logging.info(f"{path.name} exists; skipping")
            with xr.open_dataset(path) as ds:
                print(format_summary(d, summarize(ds)))
            written.append(path)
            continue
        daily_ds = build_daily_ds(job, uris, bucket=None)
        path.parent.mkdir(parents=True, exist_ok=True)
        save_ds(daily_ds, str(path))
        print(format_summary(d, summarize(daily_ds)))
        daily_ds.close()
        written.append(path)
    return written


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build P1 daily files for a THREDDS Source on local disk (onboarding pass 1).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--source", required=True)
    parser.add_argument("--start", required=True, type=date.fromisoformat)
    parser.add_argument("--end", required=True, type=date.fromisoformat)
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--overwrite", action="store_true", help="Rebuild dates already written.")
    args = parser.parse_args()

    logging.basicConfig(level="WARNING", format="[%(levelname)s] %(message)s")
    written = build(args.source, args.start, args.end, args.out_dir, args.overwrite)
    print(f"\n{len(written)} P1 file(s) under {args.out_dir}")
    print(f"Next: uv run python tools/compute_ground_speed.py --source {args.source} --daily-dir {args.out_dir}")


if __name__ == "__main__":
    main()
