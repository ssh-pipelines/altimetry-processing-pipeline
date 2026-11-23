#!/usr/bin/env python3
"""
Check that the smoother's count-based gap fill holds for a source's sampling.

`ssha_smoothing` does not assume a uniform clock (ADR 0011). It measures each
gap in local sample periods, a rolling median of the surrounding steps, rounds
that to a count of omitted samples, and spaces the filter taps
`ground_speed x sample_period` apart. That works for a non-1 Hz source as long
as two things hold over its whole record:

  - Filter width: the local period stays close to the single configured
    `sample_period`, so the taps are the right physical distance apart.
  - Gap counts: few gaps inside a 19-point window fall far from a whole number
    of local periods, so few windows have a slot count the timestamps do not
    determine.

This tool reads a source's daily files and measures both with the smoother's
own functions (`local_periods`, `ambiguous_gaps`, `sample_offsets`), on whole
days as the smoother sees them. Per pass it reports the median step and its
IQR, the correlation of the step with position along the pass, the local
period's 1st-99th percentile range, and the share of samples whose window
contains an ambiguous gap. Across passes and cycles it reports whether any of
these drift. It ends with a verdict for each check.

Usage (S3 mode):
    python tools/compute_sampling_drift.py --source AL --bucket your-bucket-name

Usage (local mode):
    uv run python tools/compute_sampling_drift.py --source AL --daily-dir /tmp/AL_pass1

--csv PATH also writes the per-pass statistics, one row per pass.
--sample-period S checks against S instead of the configured value, for a
source whose YAML still holds a placeholder.

Prerequisites:
    pip install boto3 xarray numpy pandas netcdf4 h5netcdf
"""

import argparse
import csv
import os
import sys
import tempfile
from dataclasses import asdict, dataclass, fields
from datetime import date
from pathlib import Path
from typing import Iterable, Iterator, Optional

import numpy as np

# tools/ is a sibling of utilities/ and pipeline/ at the repo root.
_REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(_REPO_ROOT / "pipeline" / "daily_file_gen" / "daily_files"))

from compute_ground_speed import (  # noqa: E402
    list_local_daily_files,
    list_s3_daily_files,
    load_daily,
)
from daily_files.processing.smoothing import (  # noqa: E402
    _HALF_WINDOW,
    ambiguous_gaps,
    local_periods,
    sample_offsets,
)

from utilities.pipeline_layout import daily_file_filename  # noqa: E402
from utilities.source_profile import get_source_profile  # noqa: E402

# Filter-width check: the 1st-99th percentile local period of every pass within
# this fraction of the record's median step. Before `sample_period` existed the
# filter was off by up to 6% (CryoSat-2) and science judged that not
# significant; 2% was ERS-1's error then. ERS-1's worst pass is now 0.90%
# (1992-10-25..1996-06-01), CryoSat-2's 0.0012% (2010-07-16..2026-07-11).
FILTER_WIDTH_TOLERANCE = 0.02

# Gap-count check: the share of samples whose 19-point window holds an ambiguous
# gap, over the record and in its worst cycle. An ambiguous count shifts the far
# side of the gap by one slot, where the outer taps weigh 2e-4 to 1e-2 against
# a 0.25 peak (ADR 0011). ERS-1 is 0.53% over the same record and 1.67% in its
# worst cycle; the limits are about three times that. CryoSat-2 has none.
AMBIGUOUS_SAMPLE_TOLERANCE = 0.02
AMBIGUOUS_SAMPLE_CYCLE_TOLERANCE = 0.05

# Passes with fewer regular steps than this are reported in the CSV but left out
# of the summary and the verdict: a fragment says nothing about drift along a pass.
MIN_PASS_STEPS = 100

# IQR is "growing" when the last third of the record's passes has a median IQR
# more than this factor above the first third's, "shrinking" below its inverse.
IQR_GROWTH_FACTOR = 1.5


@dataclass
class PassStats:
    cycle: int
    pass_: int
    start: np.datetime64
    n_points: int
    n_steps: int  # regular steps, under 1.5 x sample_period
    median_dt: float
    iqr_dt: float
    corr_position_dt: float
    local_period_p1: float
    local_period_p99: float
    n_window_gaps: int  # gaps short enough to fall inside a 19-point window
    n_ambiguous_gaps: int
    n_ambiguous_samples: int  # samples whose window holds an ambiguous gap
    grid_misalignment: float  # fraction of a period; for reference, not a check


# ---------------------------------------------------------------------------
# What the smoother sees, per day
# ---------------------------------------------------------------------------


def ambiguous_window_samples(offsets: np.ndarray, ambiguous: np.ndarray) -> np.ndarray:
    """True for each sample whose 19-point window spans an ambiguous gap.

    `offsets` are the samples' slots in the padded series (`sample_offsets`) and
    `ambiguous` flags the step after each sample but the last. A window centred
    at slot o covers o - 9 to o + 9, so it spans the gap between samples i and
    i + 1 when o[i + 1] - 9 <= o <= o[i] + 9.
    """
    mark = np.zeros(offsets.size + 1, dtype=np.int64)
    gaps = np.flatnonzero(ambiguous)
    lo = np.searchsorted(offsets, offsets[gaps + 1] - _HALF_WINDOW, side="left")
    hi = np.searchsorted(offsets, offsets[gaps] + _HALF_WINDOW, side="right")
    np.add.at(mark, lo, 1)
    np.add.at(mark, hi, -1)
    return np.cumsum(mark[:-1]) > 0


def day_arrays(time: np.ndarray, sample_period: float) -> dict[str, np.ndarray]:
    """Per-sample smoother quantities for one daily file, in time order.

    Step quantities belong to the step after each sample; the last sample's are
    NaN / False. The smoother runs over whole days, so the step across midnight
    is never measured.
    """
    order = np.argsort(time)
    t = time[order].astype("datetime64[ns]")
    n = t.size
    out = {
        "order": order,
        "time": t,
        "local_period": np.full(n, np.nan),
        "window_gap": np.zeros(n, dtype=bool),
        "ambiguous": np.zeros(n, dtype=bool),
        "ambiguous_sample": np.zeros(n, dtype=bool),
    }
    if n < 2:
        return out
    d, dt_local, periods = local_periods(t, sample_period)
    ambiguous = ambiguous_gaps(periods)
    regular = d < 1.5 * sample_period
    out["local_period"][:-1] = np.where(regular, dt_local, np.nan)
    out["window_gap"][:-1] = ~regular & (periods < _HALF_WINDOW + 1)
    out["ambiguous"][:-1] = ambiguous
    out["ambiguous_sample"] = ambiguous_window_samples(sample_offsets(t, sample_period), ambiguous)
    return out


# ---------------------------------------------------------------------------
# Reassembling passes across daily files
# ---------------------------------------------------------------------------


def iter_passes(
    chunks: Iterable[tuple[np.ndarray, np.ndarray, dict[str, np.ndarray]]],
) -> Iterator[tuple[int, int, dict[str, np.ndarray]]]:
    """Yield (cycle, pass, arrays) for each whole pass in time-ordered chunks.

    Each chunk is one daily file's cycle and pass per sample, with per-sample
    arrays in the same order. A pass crossing midnight is split across two
    files; it is held until a file arrives that does not contain it, so only
    about a day of points is in memory at once.
    """
    held: dict[tuple[int, int], list[dict[str, np.ndarray]]] = {}
    for cycle, pass_, arrays in chunks:
        keys = cycle.astype("int64") * 100000 + pass_.astype("int64")
        present: set[tuple[int, int]] = set()
        for key in np.unique(keys):
            ident = (int(key // 100000), int(key % 100000))
            sel = keys == key
            present.add(ident)
            held.setdefault(ident, []).append({k: v[sel] for k, v in arrays.items()})
        for ident in [k for k in held if k not in present]:
            yield ident[0], ident[1], _join(held.pop(ident))
    for ident, parts in held.items():
        yield ident[0], ident[1], _join(parts)


def _join(parts: list[dict[str, np.ndarray]]) -> dict[str, np.ndarray]:
    return {k: np.concatenate([p[k] for p in parts]) for k in parts[0]}


# ---------------------------------------------------------------------------
# Per-pass statistics
# ---------------------------------------------------------------------------


def max_grid_misalignment(t_ns: np.ndarray, period_s: float) -> float:
    """Smallest achievable worst-case distance of samples from a uniform grid.

    The measure ADR 0011 used to reject a uniform grid; kept for reference.
    Each sample's phase is its offset from the nearest multiple of the period.
    The best grid offset leaves the largest gap between adjacent phases on the
    circle uncovered, so the worst sample sits half the remaining arc away.
    Returns a fraction of one period, 0 to 0.5.
    """
    rel = (t_ns - t_ns[0]).astype("float64") / (period_s * 1e9)
    phase = np.sort(rel - np.floor(rel))
    gaps = np.diff(np.concatenate([phase, [phase[0] + 1.0]]))
    return float((1.0 - gaps.max()) / 2.0)


def pass_stats(cycle: int, pass_: int, a: dict[str, np.ndarray], sample_period: float) -> Optional[PassStats]:
    """Statistics for one pass, or None if it has no regular step.

    Steps are differenced in integer nanoseconds: float seconds since 1970
    resolve only a few 1e-7 s, coarser than CryoSat-2's step IQR.
    """
    order = np.argsort(a["time"])
    t_ns = a["time"][order].astype("int64")
    dt_ns = np.diff(t_ns)
    # Zero steps are duplicate timestamps, which daily files drop; not a period.
    regular = (dt_ns > 0) & (dt_ns < 1.5 * sample_period * 1e9)
    if not np.any(regular):
        return None

    steps = dt_ns[regular] / 1e9
    median = float(np.median(steps))
    q25, q75 = np.percentile(steps, [25, 75])

    # Position is elapsed time at each step's midpoint, so gaps keep their length.
    # Constancy is tested in integer ns; float std of equal steps is ~1e-16, not 0.
    position = ((t_ns[:-1] + t_ns[1:]) / 2.0 - t_ns[0])[regular]
    corr = float(np.corrcoef(position, steps)[0, 1]) if np.ptp(dt_ns[regular]) > 0 else float("nan")

    local = a["local_period"][np.isfinite(a["local_period"])]
    p1, p99 = np.percentile(local, [1, 99]) if local.size else (np.nan, np.nan)

    return PassStats(
        cycle=int(cycle),
        pass_=int(pass_),
        start=np.datetime64(int(t_ns[0]), "ns"),
        n_points=int(t_ns.size),
        n_steps=int(steps.size),
        median_dt=median,
        iqr_dt=float(q75 - q25),
        corr_position_dt=corr,
        local_period_p1=float(p1),
        local_period_p99=float(p99),
        n_window_gaps=int(a["window_gap"].sum()),
        n_ambiguous_gaps=int(a["ambiguous"].sum()),
        n_ambiguous_samples=int(a["ambiguous_sample"].sum()),
        grid_misalignment=max_grid_misalignment(t_ns, median),
    )


# ---------------------------------------------------------------------------
# Cross-pass summary
# ---------------------------------------------------------------------------


def trend_per_year(start: np.ndarray, values: np.ndarray) -> float:
    """Least-squares slope of values against pass start time, per year.

    NaN when the passes span less than a day: a slope over hours is noise
    extrapolated to a year.
    """
    years = (start.astype("datetime64[ns]") - start.min()).astype("int64") / (365.25 * 86400e9)
    if years.max() < 1.0 / 365.25:
        return float("nan")
    return float(np.polyfit(years, values, 1)[0])


def iqr_trend_label(iqr_time_ordered: np.ndarray) -> tuple[str, float]:
    """Compare the median IQR of the last third of passes with the first third.

    Returns the label and the ratio last/first. Fewer than three passes, or a
    first third with zero IQR, is "insufficient".
    """
    n = iqr_time_ordered.size // 3
    if n == 0:
        return "insufficient", float("nan")
    first = float(np.median(iqr_time_ordered[:n]))
    last = float(np.median(iqr_time_ordered[-n:]))
    if first <= 0:
        return "insufficient", float("nan")
    ratio = last / first
    if ratio > IQR_GROWTH_FACTOR:
        return "growing", ratio
    if ratio < 1.0 / IQR_GROWTH_FACTOR:
        return "shrinking", ratio
    return "stable", ratio


@dataclass
class Checks:
    reference_period: float
    filter_width_worst: float  # largest |local period / reference - 1| of any pass's p1/p99
    ambiguous_share: float  # over the record
    ambiguous_share_worst_cycle: float
    worst_cycle: int

    @property
    def filter_width_ok(self) -> bool:
        return self.filter_width_worst <= FILTER_WIDTH_TOLERANCE

    @property
    def gap_counts_ok(self) -> bool:
        return (
            self.ambiguous_share <= AMBIGUOUS_SAMPLE_TOLERANCE
            and self.ambiguous_share_worst_cycle <= AMBIGUOUS_SAMPLE_CYCLE_TOLERANCE
        )


def evaluate(stats: list[PassStats]) -> Checks:
    """The two checks over passes already filtered to `MIN_PASS_STEPS`."""
    reference = float(np.median([s.median_dt for s in stats]))
    bounds = np.array([[s.local_period_p1, s.local_period_p99] for s in stats])
    worst_width = float(np.nanmax(np.abs(bounds / reference - 1.0)))

    cycles = np.array([s.cycle for s in stats])
    amb = np.array([s.n_ambiguous_samples for s in stats])
    pts = np.array([s.n_points for s in stats])
    per_cycle = {int(c): amb[cycles == c].sum() / pts[cycles == c].sum() for c in np.unique(cycles)}
    worst_cycle = max(per_cycle, key=per_cycle.get)
    return Checks(
        reference_period=reference,
        filter_width_worst=worst_width,
        ambiguous_share=float(amb.sum() / pts.sum()),
        ambiguous_share_worst_cycle=float(per_cycle[worst_cycle]),
        worst_cycle=worst_cycle,
    )


def summarize(stats: list[PassStats], configured_period: float) -> list[str]:
    """The report, as lines. `stats` must be non-empty and already filtered."""
    stats = sorted(stats, key=lambda s: s.start)
    checks = evaluate(stats)
    ref = checks.reference_period
    start = np.array([s.start for s in stats], dtype="datetime64[ns]")
    median = np.array([s.median_dt for s in stats])
    iqr = np.array([s.iqr_dt for s in stats])
    corr = np.array([s.corr_position_dt for s in stats])
    lo = np.array([s.local_period_p1 for s in stats]) / ref - 1.0
    hi = np.array([s.local_period_p99 for s in stats]) / ref - 1.0
    cycles = np.array([s.cycle for s in stats])
    pts = np.array([s.n_points for s in stats])
    amb_pts = np.array([s.n_ambiguous_samples for s in stats])
    win_gaps = np.array([s.n_window_gaps for s in stats])
    amb_gaps = np.array([s.n_ambiguous_gaps for s in stats])

    p = np.percentile
    lines = [
        f"Passes: {len(stats)}, cycles {cycles.min()}-{cycles.max()}, "
        f"{str(start.min())[:10]} to {str(start.max())[:10]}",
        f"Record median step {ref:.6f} s; configured common.sample_period {configured_period} s",
    ]
    if abs(configured_period - ref) > 1e-5:
        lines.append("  WARNING: they differ; the filter is sized from the configured value.")

    lines += [
        "",
        "Within a pass:",
        f"  median step   p5 {p(median, 5):.6f}  p50 {np.median(median):.6f}  p95 {p(median, 95):.6f} s",
        f"  step IQR      p5 {p(iqr, 5):.2e}  p50 {np.median(iqr):.2e}  p95 {p(iqr, 95):.2e} s",
    ]
    finite = corr[np.isfinite(corr)]
    if finite.size:
        lines.append(
            f"  corr(position, step)  p5 {p(finite, 5):+.3f}  p50 {np.median(finite):+.3f}  "
            f"p95 {p(finite, 95):+.3f}  ({finite.size} passes with a varying step)"
        )
    else:
        lines.append("  corr(position, step)  undefined: the step is constant in every pass")
    lines.append(
        f"  local period, p1-p99 per pass, vs record median: {np.nanmin(lo):+.3%} to {np.nanmax(hi):+.3%}"
    )

    slope = trend_per_year(start, median)
    label, ratio = iqr_trend_label(iqr)
    iqr_slope = trend_per_year(start, iqr)
    lines += [
        "",
        "Between passes:",
        f"  per-pass median step {median.min():.6f}-{median.max():.6f} s "
        f"(spread {(median.max() - median.min()) / ref:.2e} of the median)",
        f"  trend {slope:+.2e} s/year" if np.isfinite(slope) else "  trend: window under a day",
        f"  IQR {label}"
        + (f" (last third / first third of passes = {ratio:.2f})" if np.isfinite(ratio) else "")
        + (f", trend {iqr_slope:+.2e} s/year" if np.isfinite(iqr_slope) else ""),
        "",
        "Between cycles:",
        "  cycle  passes  median step (s)  median IQR (s)  local period range  "
        "ambiguous gaps  samples near one",
    ]
    for c in np.unique(cycles):
        sel = cycles == c
        gaps = win_gaps[sel].sum()
        lines.append(
            f"  {c:>5}  {sel.sum():>6}  {np.median(median[sel]):>15.6f}  {np.median(iqr[sel]):>14.2e}  "
            f"{np.nanmin(lo[sel]):>+8.3%} {np.nanmax(hi[sel]):>+8.3%}  "
            f"{(amb_gaps[sel].sum() / gaps if gaps else 0.0):>14.1%}  "
            f"{amb_pts[sel].sum() / pts[sel].sum():>16.2%}"
        )

    gaps = win_gaps.sum()
    lines += [
        "",
        f"Gaps inside a 19-point window: {gaps}, {amb_gaps.sum()} ambiguous "
        f"({(amb_gaps.sum() / gaps if gaps else 0.0):.1%})",
        "",
        "Checks against the count-based smoother (ADR 0011):",
        f"  filter width  {'PASS' if checks.filter_width_ok else 'FAIL'}  "
        f"worst pass local period {checks.filter_width_worst:.3%} from the record median "
        f"(limit {FILTER_WIDTH_TOLERANCE:.0%})",
        f"  gap counts    {'PASS' if checks.gap_counts_ok else 'FAIL'}  "
        f"{checks.ambiguous_share:.2%} of samples have an ambiguous gap in their window "
        f"(limit {AMBIGUOUS_SAMPLE_TOLERANCE:.0%}); worst cycle {checks.worst_cycle} "
        f"{checks.ambiguous_share_worst_cycle:.2%} (limit {AMBIGUOUS_SAMPLE_CYCLE_TOLERANCE:.0%})",
        "",
        "Verdict: "
        + (
            "the count-based smoother holds; one sample_period is adequate"
            if checks.filter_width_ok and checks.gap_counts_ok
            else "the count-based smoother does not hold for this sampling; take it to science"
        ),
    ]
    return lines


def write_csv(path: str, stats: list[PassStats]) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[fl.name for fl in fields(PassStats)])
        writer.writeheader()
        for s in sorted(stats, key=lambda s: s.start):
            row = asdict(s)
            row["start"] = str(s.start)
            writer.writerow(row)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check that the smoother's count-based gap fill holds for a source's sampling.",
    )
    parser.add_argument("--source", required=True, help="Source name (e.g. AL)")
    parser.add_argument(
        "--version",
        default="p1",
        help="Daily-file lifecycle version to read (default: p1). Timestamps are identical across versions.",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        metavar="N",
        help="Cap the number of daily files scanned, earliest first (default: all).",
    )
    parser.add_argument(
        "--sample-period",
        type=float,
        default=None,
        metavar="S",
        help="Check against this sample period (s) instead of the configured common.sample_period.",
    )
    parser.add_argument("--bucket", default=None, help="S3 bucket containing daily files (S3 mode)")
    parser.add_argument(
        "--daily-dir",
        default=None,
        metavar="DIR",
        help="Local directory containing daily files (local mode, searched recursively)",
    )
    parser.add_argument("--profile", default=None, help="AWS profile name (S3 mode only)")
    parser.add_argument("--csv", default=None, metavar="PATH", help="Also write per-pass statistics to PATH")
    args = parser.parse_args()

    using_s3 = args.bucket is not None
    if using_s3 == (args.daily_dir is not None):
        parser.error("Provide exactly one of --bucket (S3 mode) or --daily-dir (local mode).")

    profile = get_source_profile(args.source)
    sample_period = args.sample_period if args.sample_period is not None else profile.sample_period

    if using_s3:
        import boto3

        s3 = boto3.Session(profile_name=args.profile).client("s3")
        print(f"Listing daily files in s3://{args.bucket}/daily_files/{args.version}/{args.source}/ ...")
        files = list_s3_daily_files(s3, args.bucket, args.source, args.version)
    else:
        print(f"Scanning {args.daily_dir} ...")
        prefix = daily_file_filename(profile, date(2000, 1, 1)).split("2000")[0]
        files = [f for f in list_local_daily_files(args.daily_dir) if Path(f).name.startswith(prefix)]

    # Pass reassembly needs the files in date order; the date is the leaf name's suffix.
    files = sorted(files, key=lambda f: Path(f).name)
    if args.max_files is not None:
        files = files[: args.max_files]
    print(f"  {len(files)} daily file(s) to scan.")
    if not files:
        print("Error: no daily files found.", file=sys.stderr)
        sys.exit(1)

    tmp_ctx = tempfile.TemporaryDirectory() if using_s3 else None

    def chunks() -> Iterator[tuple[np.ndarray, np.ndarray, dict[str, np.ndarray]]]:
        for i, f in enumerate(files, 1):
            print(f"  [{i:>4}/{len(files)}] {Path(f).name}", end="\r", flush=True)
            if using_s3:
                local_path = os.path.join(tmp_ctx.name, Path(f).name)
                s3.download_file(args.bucket, f, local_path)
                t, _, _, cy, ps = load_daily(local_path)
                os.remove(local_path)
            else:
                t, _, _, cy, ps = load_daily(f)
            arrays = day_arrays(t, sample_period)
            order = arrays.pop("order")
            yield cy[order], ps[order], arrays

    all_stats: list[PassStats] = []
    try:
        for cycle, pass_, arrays in iter_passes(chunks()):
            s = pass_stats(cycle, pass_, arrays, sample_period)
            if s is not None:
                all_stats.append(s)
    finally:
        if tmp_ctx is not None:
            tmp_ctx.cleanup()
    print()

    if args.csv:
        write_csv(args.csv, all_stats)
        print(f"Wrote {len(all_stats)} passes to {args.csv}")

    used = [s for s in all_stats if s.n_steps >= MIN_PASS_STEPS]
    print(f"{len(all_stats) - len(used)} pass(es) with under {MIN_PASS_STEPS} regular steps left out.")
    if not used:
        print(f"Error: no pass has {MIN_PASS_STEPS} or more regular steps.", file=sys.stderr)
        sys.exit(1)

    print(f"\nSampling drift ({args.source}, checked at sample_period {sample_period} s):")
    for line in summarize(used, profile.sample_period):
        print(line)


if __name__ == "__main__":
    main()
