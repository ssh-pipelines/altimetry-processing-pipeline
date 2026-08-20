#!/usr/bin/env python3
"""
Compute the intermission bias between a high-latitude source and its reference
mission, measured at their reference crossovers.

A high-latitude source (e.g. S3B) does not overlap the reference ground track,
so it is crossed against a finalized reference mission (NASA-SSH). At each
crossover the two SSH values are stored (``ssh1`` = high-lat, ``ssh2`` = the
reference SSH time-interpolated to the high-lat crossover time). Their
difference ``dssh = ssh1 - ssh2`` is a roughly-constant offset with a wide,
noisy spread. This tool reduces that difference to a single constant
(``common.intermission_bias``) that the OER stage subtracts from ``dssh`` before
fitting the orbit-error spline, and that the finalizer later applies to the
absolute ``ssha`` level.

The estimator is robust: the primary value is the **median** of ``dssh``, with a
MAD-based outlier-rejected mean reported alongside the simple mean, std, and
count.

Usage (S3 mode):
    python tools/compute_intermission_bias.py \\
        --source S3B \\
        --bucket your-bucket-name \\
        --version p1

Usage (local mode):
    python tools/compute_intermission_bias.py \\
        --source S3B \\
        --xover-dir /data/high_lat_crossovers/crossovers/p1/S3B \\
        --plot

--version selects the crossover lifecycle version to read (default p1 — the
version OER consumes; see oer.py fetch_xovers).

The tool also prints a robust (Theil-Sen) trend of the daily difference in
mm/yr with a 95% CI and the total drift over the record, as context — it does
not label the trend significant. Judge stationarity from the plots.

--plot writes diagnostic PNGs (to tools/diagnostics/<source>/ by default, or a
per-source subdirectory of --output-dir) so you can judge whether a single
constant bias is adequate. The three plots probe orthogonal ways the assumption
could break: temporal drift (low-pass curve + trend fit), distribution shape /
outliers (histogram), and spatial structure (difference vs latitude). If the
low-pass curve or trend line slopes, or the latitude bins trend, a constant is
insufficient. --lowpass-cutoff-days sets the low-pass cutoff (90 d validated for S3B).

Result:
    Enter the printed value under `common` for the source in:
        utilities/sources/{source}.yaml

        common:
          intermission_bias: <value>   # m

"""

import argparse
import os
import sys
import tempfile
from datetime import date
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import xarray as xr
from scipy.signal import butter, filtfilt
from scipy.stats import theilslopes

# tools/ is a sibling of utilities/ at the repo root.
_REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from utilities.pipeline_layout import crossover_filename  # noqa: E402
from utilities.source_profile import SOURCES_DIR, get_source_profile  # noqa: E402


def _reference_source(source: str) -> str:
    """Best-effort read of ``xover.reference_source`` from the source YAML.

    Used only for plot/label text; falls back to ``"reference"`` if absent.
    """
    import yaml

    try:
        with open(SOURCES_DIR / f"{source}.yaml") as f:
            raw = yaml.safe_load(f) or {}
        return (raw.get("xover") or {}).get("reference_source") or "reference"
    except Exception:
        return "reference"


# MAD-to-sigma scale for a normal distribution; used so the outlier-rejection
# threshold is expressed in robust "sigma" units.
_MAD_TO_SIGMA = 1.4826

# Reject |dssh - median| > _OUTLIER_K robust-sigma when computing the
# outlier-rejected mean. 5 sigma keeps the bulk while dropping gross blunders.
_OUTLIER_K = 5.0

# Default low-pass cutoff (days) for the drift diagnostic, and the Butterworth
# order. 90 days was validated by the science team for S3B; other high-lat
# sources may want a different cutoff (see --lowpass-cutoff-days).
_DEFAULT_LOWPASS_CUTOFF_DAYS = 90
_LOWPASS_ORDER = 4

# Default parent directory for --plot output. The tool appends a per-source
# subdirectory at runtime (e.g. tools/diagnostics/S3B/).
_DEFAULT_DIAGNOSTICS_DIR = _REPO_ROOT / "tools" / "diagnostics"


# ---------------------------------------------------------------------------
# Estimator
# ---------------------------------------------------------------------------


def compute_intermission_bias(dssh: np.ndarray) -> dict:
    """Reduce per-crossover differences ``dssh`` to a single bias estimate.

    Returns a dict with the robust ``median`` (the recommended value), a
    MAD-based ``outlier_rejected_mean``, the plain ``mean``, ``std``, and the
    ``count`` of finite samples. Also returns the robust spread (``mad``,
    ``robust_sigma``) and the rejection band (``reject_lower``, ``reject_upper``)
    so callers can reproduce exactly which points were rejected (see
    ``rejected_mask``) for reporting/plotting. Raises ``ValueError`` if no finite
    data.

    A point is rejected when it lies more than ``_OUTLIER_K`` robust-sigma from
    the median, where robust-sigma = 1.4826 * MAD (MAD is immune to the outliers
    it is meant to catch, unlike std). When the spread is degenerate
    (``robust_sigma == 0``, e.g. all values identical) the band is infinite and
    nothing is rejected.
    """
    dssh = np.asarray(dssh, dtype="float64")
    finite = dssh[np.isfinite(dssh)]
    if finite.size == 0:
        raise ValueError("No finite crossover differences to average.")

    median = float(np.median(finite))
    mad = float(np.median(np.abs(finite - median)))
    sigma = _MAD_TO_SIGMA * mad

    if sigma > 0:
        reject_lower = median - _OUTLIER_K * sigma
        reject_upper = median + _OUTLIER_K * sigma
    else:
        # Degenerate spread (all values identical): reject nothing.
        reject_lower, reject_upper = -np.inf, np.inf

    kept = finite[(finite >= reject_lower) & (finite <= reject_upper)]

    return {
        "median": median,
        "outlier_rejected_mean": float(np.mean(kept)) if kept.size else median,
        "mean": float(np.mean(finite)),
        "std": float(np.std(finite)),
        "count": int(finite.size),
        "n_rejected": int(finite.size - kept.size),
        "mad": mad,
        "robust_sigma": sigma,
        "reject_lower": reject_lower,
        "reject_upper": reject_upper,
    }


def rejected_mask(dssh: np.ndarray, stats: dict) -> np.ndarray:
    """Boolean mask (aligned to ``dssh``) of points the estimator rejected.

    A point is rejected if it is finite and falls outside
    ``[reject_lower, reject_upper]``. NaNs are never flagged as rejected (they
    are simply not usable). Lets reporting and plotting mark the same points the
    outlier-rejected mean dropped.
    """
    dssh = np.asarray(dssh, dtype="float64")
    finite = np.isfinite(dssh)
    outside = (dssh < stats["reject_lower"]) | (dssh > stats["reject_upper"])
    return finite & outside


def smooth_bias(bias: np.ndarray, cutoff_days: float, order: int = _LOWPASS_ORDER) -> np.ndarray:
    """Zero-phase Butterworth low-pass of a daily-sampled bias series.

    ``bias`` must be gap-free (one sample per day, no NaNs) — ``filtfilt``
    cannot cross NaNs. Ported from crossover_timeseries.ipynb.
    """
    fs = 1.0  # one sample per day
    Wn = (1 / cutoff_days) / (fs / 2)
    b, a = butter(order, Wn, btype="low")
    return filtfilt(b, a, bias)


# ---------------------------------------------------------------------------
# Daily aggregation
# ---------------------------------------------------------------------------


def daily_means(
    time1: np.ndarray,
    ssh1: np.ndarray,
    ssh2: np.ndarray,
    keep: Optional[np.ndarray] = None,
) -> pd.DataFrame:
    """Daily means of high-lat and reference SSH on a gap-free daily grid.

    Bins crossovers by calendar day, means ``ssh1``/``ssh2`` within each day,
    reindexes onto a complete daily grid (so missing days become NaN), and
    time-interpolates the gaps (a 10-day gap becomes a 10-day ramp, not a step),
    back/forward-filling the endpoints. Returns a DataFrame indexed by day with
    columns ``high_lat_daily_mean`` and ``ref_daily_mean``. Ported from
    crossover_timeseries.ipynb.

    ``keep`` is an optional boolean mask (aligned to the inputs) of crossovers to
    include; pass ``~rejected`` so gross outliers don't drag a day's mean (and
    thus the low-pass curve) around. A day left with no kept crossovers becomes a
    NaN that the interpolation then fills from its neighbours.
    """
    time1 = np.asarray(time1)
    ssh1 = np.asarray(ssh1, dtype="float64")
    ssh2 = np.asarray(ssh2, dtype="float64")
    if keep is not None:
        keep = np.asarray(keep, dtype=bool)
        time1, ssh1, ssh2 = time1[keep], ssh1[keep], ssh2[keep]

    days = pd.to_datetime(time1).normalize()
    frame = pd.DataFrame(
        {"high_lat": ssh1, "ref": ssh2},
        index=days,
    )
    grouped = frame.groupby(level=0).mean()

    full_index = pd.date_range(grouped.index.min(), grouped.index.max(), freq="D")
    grouped = grouped.reindex(full_index)
    interp = grouped.interpolate(method="time", limit_direction="both")

    return pd.DataFrame(
        {
            "high_lat_daily_mean": interp["high_lat"],
            "ref_daily_mean": interp["ref"],
        },
        index=full_index,
    )


def fit_trend(days: np.ndarray, diff: np.ndarray) -> Optional[dict]:
    """Robust linear trend of the daily difference, as decision-support context.

    ``days`` is a DatetimeIndex-like array (one point per day, gap-free);
    ``diff`` is the daily ``high_lat - ref`` difference. Fits a Theil–Sen slope
    (robust to the residual outliers a least-squares fit would chase) and returns
    the slope in **m/yr** with a 95% confidence interval and the span in years.
    Returns ``None`` if there are fewer than two years of data — a shorter span
    can't distinguish a trend from the low-frequency wobble.

    The tool intentionally does NOT flag the trend as "significant": with years
    of daily points a bare CI-excludes-zero test trips on scientifically trivial
    slopes. The slope, CI, and total drift over the span are reported so the
    reader judges from the plot whether a static constant is adequate.
    """
    t = pd.to_datetime(np.asarray(days))
    years = (t - t[0]).total_seconds().to_numpy() / (365.25 * 86400.0)
    diff = np.asarray(diff, dtype="float64")
    good = np.isfinite(years) & np.isfinite(diff)
    years, diff = years[good], diff[good]
    span = float(years.max() - years.min()) if years.size else 0.0
    if years.size < 3 or span < 2.0:
        return None

    # Theil–Sen: slope is the median of pairwise slopes; theilslopes also returns
    # a 95% CI on the slope (Sen's method). Units: m per year (years is in yr).
    slope, intercept, lo_slope, hi_slope = theilslopes(diff, years)
    return {
        "slope_m_per_yr": float(slope),
        "slope_ci_low": float(lo_slope),
        "slope_ci_high": float(hi_slope),
        "intercept": float(intercept),
        "span_years": span,
        "total_drift_m": float(slope * span),
    }


def binned_by_latitude(
    lat: np.ndarray, dssh: np.ndarray, bin_width_deg: float = 2.0
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Median (and spread) of ``dssh`` in latitude bins.

    A single constant bias assumes ``dssh`` has no spatial structure. A high-lat
    source crosses the reference only at high latitudes, so a latitude-dependent
    bias (e.g. a residual grid/ellipsoid effect) is a distinct failure mode from
    temporal drift. Returns ``(bin_centers, bin_median, bin_mad, bin_count)`` for
    bins that contain at least one finite sample; empty arrays if no finite data.
    """
    lat = np.asarray(lat, dtype="float64")
    dssh = np.asarray(dssh, dtype="float64")
    finite = np.isfinite(lat) & np.isfinite(dssh)
    lat, dssh = lat[finite], dssh[finite]
    if lat.size == 0:
        empty = np.array([], dtype="float64")
        return empty, empty, empty, empty

    lo = np.floor(lat.min() / bin_width_deg) * bin_width_deg
    hi = np.ceil(lat.max() / bin_width_deg) * bin_width_deg
    if hi <= lo:  # all points on a single bin boundary -> guarantee one bin
        hi = lo + bin_width_deg
    edges = np.arange(lo, hi + bin_width_deg, bin_width_deg)
    idx = np.clip(np.digitize(lat, edges) - 1, 0, len(edges) - 2)

    centers, medians, mads, counts = [], [], [], []
    for b in range(len(edges) - 1):
        sel = idx == b
        if not np.any(sel):
            continue
        vals = dssh[sel]
        med = float(np.median(vals))
        centers.append(0.5 * (edges[b] + edges[b + 1]))
        medians.append(med)
        mads.append(float(np.median(np.abs(vals - med))))
        counts.append(int(vals.size))
    return (
        np.array(centers),
        np.array(medians),
        np.array(mads),
        np.array(counts, dtype="int64"),
    )


# ---------------------------------------------------------------------------
# Plotting (lazy matplotlib import; only used with --plot)
# ---------------------------------------------------------------------------


def make_plots(
    time1: np.ndarray,
    lat: np.ndarray,
    ssh1: np.ndarray,
    ssh2: np.ndarray,
    stats: dict,
    source: str,
    reference: str,
    output_dir: str,
    lowpass_cutoff_days: int = _DEFAULT_LOWPASS_CUTOFF_DAYS,
    trend: Optional[dict] = None,
) -> list[str]:
    """Write diagnostic PNGs; return the paths written.

    Each plot targets one way the "single constant bias" assumption could break:
    (1) the low-pass curve + robust trend fit test temporal stationarity — the
    trend slope (± CI) is the quantitative static-vs-trend call; (2) the dssh
    histogram shows the distribution shape and where the median/mean and
    outlier-rejection band fall; (3) dssh binned by latitude tests spatial
    stationarity — a distinct failure mode, since a high-lat source only crosses
    the reference at high latitudes.

    Outlier-rejected crossovers are excluded from the daily aggregation behind
    the low-pass plot, so a handful of gross blunders can't drag a day's mean and
    blow out the y-axis. ``lowpass_cutoff_days`` sets the single low-pass cutoff
    (90 d validated for S3B; other sources may differ). ``trend`` is the dict from
    ``fit_trend`` (or None if the series is too short); its slope line is overlaid.
    """
    import matplotlib

    matplotlib.use("Agg")  # headless
    import matplotlib.pyplot as plt

    os.makedirs(output_dir, exist_ok=True)
    written: list[str] = []
    median = stats["median"]
    mean = stats["mean"]
    lo, hi = stats["reject_lower"], stats["reject_upper"]

    dssh = np.asarray(ssh1, dtype="float64") - np.asarray(ssh2, dtype="float64")
    rejected = rejected_mask(dssh, stats)

    # Daily aggregation excludes rejected crossovers so outliers don't skew the
    # daily means (and thus the y-axis) of the temporal plot.
    df = daily_means(time1, ssh1, ssh2, keep=~rejected)
    diff = df["high_lat_daily_mean"] - df["ref_daily_mean"]

    # (1) low-pass curve + trend fit — the primary temporal non-stationarity check
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(df.index, diff, color="gray", alpha=0.3, label="daily")
    if diff.size > 3 * lowpass_cutoff_days:  # filtfilt needs enough samples for the filter length
        ax.plot(
            df.index,
            smooth_bias(diff.values, cutoff_days=lowpass_cutoff_days),
            color="C0",
            lw=2,
            label=f"{lowpass_cutoff_days} d low-pass",
        )
    else:
        ax.text(
            0.5,
            0.5,
            f"series too short for a {lowpass_cutoff_days} d low-pass",
            transform=ax.transAxes,
            ha="center",
            va="center",
            color="crimson",
        )
    ax.axhline(median, color="black", linestyle="--", label=f"median {median:.6f} m")
    if trend is not None:
        # Draw the Theil–Sen fit line across the full span. years are measured
        # from the first day, matching fit_trend's parameterization.
        t0 = df.index[0]
        years = (df.index - t0).total_seconds().to_numpy() / (365.25 * 86400.0)
        fit_line = trend["intercept"] + trend["slope_m_per_yr"] * years
        mm_yr = trend["slope_m_per_yr"] * 1000.0
        ci = 0.5 * (trend["slope_ci_high"] - trend["slope_ci_low"]) * 1000.0
        drift_mm = trend["total_drift_m"] * 1000.0
        ax.plot(
            df.index,
            fit_line,
            color="crimson",
            lw=2,
            linestyle="-",
            label=f"trend {mm_yr:+.2f} ± {ci:.2f} mm/yr ({drift_mm:+.1f} mm over {trend['span_years']:.1f} yr)",
        )
    ax.set_title(f"Low-pass filtered daily difference ({source} - {reference})")
    ax.set_ylabel("m")
    ax.grid()
    ax.legend()
    p = os.path.join(output_dir, f"intermission_bias_{source}_lowpass.png")
    fig.savefig(p, dpi=120, bbox_inches="tight")
    plt.close(fig)
    written.append(p)

    # (3) histogram of dssh with median/mean and the outlier-rejection band
    finite = dssh[np.isfinite(dssh)]
    fig, ax = plt.subplots(figsize=(12, 6))
    # Clip the plotting range to the rejection band (+margin) so the bulk is
    # readable; rejected tails are summarized in the annotation instead.
    if np.isfinite(lo) and np.isfinite(hi):
        margin = 0.15 * (hi - lo)
        ax.set_xlim(lo - margin, hi + margin)
    ax.hist(finite, bins=200, color="steelblue", alpha=0.7)
    ax.axvline(median, color="black", label=f"median {median:.6f} m")
    ax.axvline(mean, color="crimson", linestyle="--", label=f"mean {mean:.6f} m")
    if np.isfinite(lo):
        ax.axvline(lo, color="gray", linestyle=":", label=f"reject band ±{_OUTLIER_K}σ")
        ax.axvline(hi, color="gray", linestyle=":")
    ax.set_title(
        f"Distribution of crossover differences ({source} - {reference})\n"
        f"{stats['n_rejected']} of {stats['count']} rejected beyond ±{_OUTLIER_K}σ "
        f"(robust σ = {stats['robust_sigma']:.4f} m)"
    )
    ax.set_xlabel("ssh1 - ssh2 (m)")
    ax.set_ylabel("count")
    ax.grid()
    ax.legend()
    p = os.path.join(output_dir, f"intermission_bias_{source}_histogram.png")
    fig.savefig(p, dpi=120, bbox_inches="tight")
    plt.close(fig)
    written.append(p)

    # (4) dssh binned by latitude — spatial stationarity check
    centers, medians, mads, counts = binned_by_latitude(lat, dssh)
    fig, ax = plt.subplots(figsize=(12, 6))
    keep = ~rejected & np.isfinite(dssh) & np.isfinite(np.asarray(lat, dtype="float64"))
    lat_arr = np.asarray(lat, dtype="float64")
    ax.scatter(lat_arr[keep], dssh[keep], s=4, alpha=0.15, color="steelblue", label="kept")
    if centers.size:
        ax.errorbar(
            centers, medians, yerr=mads, color="black", marker="o", ms=4, lw=1.5, capsize=3, label="binned median ± MAD"
        )
    ax.axhline(median, color="green", linestyle="--", label=f"overall median {median:.6f} m")
    if np.isfinite(lo) and np.isfinite(hi):
        y_top = hi + 0.15 * (hi - lo)
        ax.set_ylim(lo - 0.15 * (hi - lo), y_top)
        # Rejected points sit outside the band (often far outside); clamp them to
        # the top edge as a rug so their *latitude* clustering is visible without
        # rescaling the bulk. Their magnitude is in the printed summary.
        if np.any(rejected):
            ax.scatter(
                lat_arr[rejected],
                np.full(int(rejected.sum()), y_top),
                s=18,
                marker="v",
                color="crimson",
                clip_on=False,
                label=f"rejected (n={int(rejected.sum())}, clamped to edge)",
            )
    elif np.any(rejected):
        ax.scatter(lat_arr[rejected], dssh[rejected], s=10, alpha=0.6, color="crimson", label="rejected")
    ax.set_title(f"Crossover difference vs latitude ({source} - {reference})\nis the bias stationary in space?")
    ax.set_xlabel("latitude (deg)")
    ax.set_ylabel("ssh1 - ssh2 (m)")
    ax.grid()
    ax.legend()
    p = os.path.join(output_dir, f"intermission_bias_{source}_by_latitude.png")
    fig.savefig(p, dpi=120, bbox_inches="tight")
    plt.close(fig)
    written.append(p)

    return written


def lowpass_drift(
    time1: np.ndarray,
    ssh1: np.ndarray,
    ssh2: np.ndarray,
    cutoff_days: int = _DEFAULT_LOWPASS_CUTOFF_DAYS,
    keep: Optional[np.ndarray] = None,
) -> Optional[float]:
    """Peak-to-peak range (m) of the low-pass daily difference, or None if the
    series is too short to filter. A large value relative to the median hints
    the bias is not constant. ``keep`` (pass ``~rejected``) excludes outliers
    from the daily aggregation, matching the plotted curve."""
    df = daily_means(time1, ssh1, ssh2, keep=keep)
    diff = (df["high_lat_daily_mean"] - df["ref_daily_mean"]).values
    if diff.size <= 3 * cutoff_days:
        return None
    smoothed = smooth_bias(diff, cutoff_days=cutoff_days)
    return float(np.nanmax(smoothed) - np.nanmin(smoothed))


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def load_crossover(path: str) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Load (time1, lat, ssh1, ssh2) from a reference crossover file.

    Returns empty arrays for a file with no crossovers. ``lat`` is used for the
    spatial-stationarity diagnostic.
    """
    ds = xr.open_dataset(path, engine="h5netcdf")
    if ds["time1"].size == 0:
        ds.close()
        empty = np.array([], dtype="float64")
        return np.array([], dtype="datetime64[ns]"), empty, empty, empty
    time1 = ds["time1"].values
    lat = ds["lat"].values
    ssh1 = ds["ssh1"].values
    ssh2 = ds["ssh2"].values
    ds.close()
    return time1, lat, ssh1, ssh2


def list_s3_crossovers(s3_client, bucket: str, source: str, version: str) -> list[str]:
    """Return sorted s3 keys for a source's crossover files at the given version."""
    keys: list[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    prefix = f"crossovers/{version}/{source}/"
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".nc"):
                keys.append(obj["Key"])
    return sorted(keys)


def list_local_crossovers(directory: str) -> list[str]:
    """Return sorted local crossover paths under a directory (recursive)."""
    return sorted(str(p) for p in Path(directory).rglob("*.nc"))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _within_date_bounds(path: str, source: str, start: Optional[date], end: Optional[date]) -> bool:
    """Filter crossover files by the ISO date embedded in their filename."""
    if start is None and end is None:
        return True
    name = Path(path).name
    # xovers_{source}-{YYYY-MM-DD}.nc
    stem = name[len(f"xovers_{source}-") :].removesuffix(".nc")
    try:
        d = date.fromisoformat(stem)
    except ValueError:
        return True  # unparseable name: don't silently drop it
    if start is not None and d < start:
        return False
    if end is not None and d > end:
        return False
    return True


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute a high-lat source's intermission bias vs its reference mission at crossovers.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Enter the printed value under `common` for the source in:
  utilities/sources/{source}.yaml

    common:
      intermission_bias: <value>   # m

The value is subtracted from crossover differences before the reference OER
spline fit, and applied to the absolute ssha level in the finalizer.
        """,
    )
    parser.add_argument("--source", required=True, help="High-latitude source name (e.g. S3B)")
    parser.add_argument(
        "--version",
        default="p1",
        help="Crossover lifecycle version to read (default: p1, the version OER consumes).",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        metavar="YYYY-MM-DD",
        help="Only use crossovers on or after this date (baseline window start).",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        metavar="YYYY-MM-DD",
        help="Only use crossovers on or before this date (baseline window end).",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        metavar="N",
        help="Cap the number of crossover files scanned (default: all available).",
    )

    # Data source: S3 or local
    parser.add_argument("--bucket", default=None, help="S3 bucket containing crossovers (S3 mode)")
    parser.add_argument(
        "--xover-dir",
        default=None,
        metavar="DIR",
        help="Local directory containing crossover files (local mode, searched recursively)",
    )
    parser.add_argument("--profile", default=None, help="AWS profile name (S3 mode only)")

    # Plotting
    parser.add_argument(
        "--plot",
        action="store_true",
        help="Write diagnostic PNGs (low-pass + trend, histogram, by-latitude).",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        metavar="DIR",
        help=(
            "Parent directory for --plot PNGs. A per-source subdirectory is "
            "appended (default: tools/diagnostics/<source>/)."
        ),
    )
    parser.add_argument(
        "--lowpass-cutoff-days",
        type=int,
        default=_DEFAULT_LOWPASS_CUTOFF_DAYS,
        metavar="DAYS",
        help=(
            "Low-pass filter cutoff for the temporal-stationarity check "
            f"(default: {_DEFAULT_LOWPASS_CUTOFF_DAYS} d, validated for S3B; "
            "other high-lat sources may want a different value)."
        ),
    )
    args = parser.parse_args()

    using_s3 = args.bucket is not None
    using_local = args.xover_dir is not None
    if using_s3 == using_local:
        parser.error("Provide exactly one of --bucket (S3 mode) or --xover-dir (local mode).")

    start = date.fromisoformat(args.start_date) if args.start_date else None
    end = date.fromisoformat(args.end_date) if args.end_date else None

    # Confirm the source is configured (raises a helpful error otherwise).
    get_source_profile(args.source)
    # reference_source lives in the xover stage section, not SourceCommon, and
    # its config dataclass lives in a sibling stage package that isn't importable
    # from repo root — read it straight from the YAML for plot/labels only.
    reference = _reference_source(args.source)

    # Discover files
    if using_s3:
        import boto3

        session = boto3.Session(profile_name=args.profile)
        s3 = session.client("s3")
        print(f"Listing crossovers in s3://{args.bucket}/crossovers/{args.version}/{args.source}/ ...")
        files = list_s3_crossovers(s3, args.bucket, args.source, args.version)
    else:
        print(f"Scanning {args.xover_dir} ...")
        # Keep only files matching this source's crossover naming convention.
        prefix = crossover_filename(args.source, date(2000, 1, 1)).split("2000")[0]
        files = [f for f in list_local_crossovers(args.xover_dir) if Path(f).name.startswith(prefix)]

    files = [f for f in files if _within_date_bounds(f, args.source, start, end)]
    if args.max_files is not None:
        files = files[: args.max_files]

    print(f"  {len(files)} crossover file(s) to scan.")
    if not files:
        print("Error: no crossover files found — cannot compute intermission bias.", file=sys.stderr)
        sys.exit(1)

    # Load and concatenate
    time_chunks, lat_chunks, ssh1_chunks, ssh2_chunks = [], [], [], []

    tmp_ctx = tempfile.TemporaryDirectory() if using_s3 else None
    tmp_dir = tmp_ctx.name if tmp_ctx else None
    try:
        for i, f in enumerate(files, 1):
            print(f"  [{i:>4}/{len(files)}] {Path(f).name}", end="\r", flush=True)
            if using_s3:
                local_path = os.path.join(tmp_dir, f.replace("/", "_"))
                s3.download_file(args.bucket, f, local_path)
                t, la, s1, s2 = load_crossover(local_path)
                os.remove(local_path)
            else:
                t, la, s1, s2 = load_crossover(f)
            if t.size:
                time_chunks.append(t)
                lat_chunks.append(la)
                ssh1_chunks.append(s1)
                ssh2_chunks.append(s2)
    finally:
        if tmp_ctx is not None:
            tmp_ctx.cleanup()

    if not time_chunks:
        print("\nError: crossover files contained no crossovers.", file=sys.stderr)
        sys.exit(1)

    time1 = np.concatenate(time_chunks)
    lat = np.concatenate(lat_chunks)
    ssh1 = np.concatenate(ssh1_chunks)
    ssh2 = np.concatenate(ssh2_chunks)
    dssh = ssh1 - ssh2
    print(f"\nLoaded {dssh.size} crossovers.")

    stats = compute_intermission_bias(dssh)

    print("\nResults:")
    print(f"  Median (recommended):     {stats['median']:.6f} m")
    print(f"  Outlier-rejected mean:    {stats['outlier_rejected_mean']:.6f} m  ({stats['n_rejected']} rejected)")
    print(f"  Mean:                     {stats['mean']:.6f} m")
    print(f"  Std:                      {stats['std']:.6f} m")
    print(f"  Robust sigma (1.48*MAD):  {stats['robust_sigma']:.6f} m")
    print(f"  Count:                    {stats['count']}")

    # Detail the outlier rejection: how many, what fraction, the band they fell
    # outside, and where they sit in latitude (a spatially-clustered rejection
    # rate is a red flag distinct from scattered noise).
    rejected = rejected_mask(dssh, stats)
    n_rej = int(np.count_nonzero(rejected))
    if n_rej:
        frac = 100.0 * n_rej / stats["count"]
        rej_vals = dssh[rejected]
        print(
            f"\n  Rejected points:          {n_rej} ({frac:.2f}%) outside "
            f"[{stats['reject_lower']:.4f}, {stats['reject_upper']:.4f}] m"
        )
        print(f"    dssh range (rejected):  [{rej_vals.min():.4f}, {rej_vals.max():.4f}] m")
        rej_lat = np.asarray(lat, dtype="float64")[rejected]
        rej_lat = rej_lat[np.isfinite(rej_lat)]
        if rej_lat.size:
            print(f"    latitude range:         [{rej_lat.min():.2f}, {rej_lat.max():.2f}] deg")
        if frac > 5.0:
            print("    <-- NOTE: >5% rejected; the distribution has heavy tails — inspect the histogram.")

    # Decision-support context (no verdict — judge stationarity from the plots).
    cutoff = args.lowpass_cutoff_days
    drift = lowpass_drift(time1, ssh1, ssh2, cutoff_days=cutoff, keep=~rejected)
    if drift is not None:
        print(f"\n  {cutoff}-day low-pass range:   {drift:.6f} m")

    # Robust (Theil–Sen) trend of the daily difference. Reported as context; the
    # slope, CI, and total drift over the span let the reader gauge whether a
    # static constant is adequate from the low-pass plot.
    daily = daily_means(time1, ssh1, ssh2, keep=~rejected)
    trend = fit_trend(daily.index, daily["high_lat_daily_mean"] - daily["ref_daily_mean"])
    if trend is not None:
        mm_yr = trend["slope_m_per_yr"] * 1000.0
        ci_low = trend["slope_ci_low"] * 1000.0
        ci_high = trend["slope_ci_high"] * 1000.0
        drift_mm = trend["total_drift_m"] * 1000.0
        print(f"  Trend (Theil-Sen):        {mm_yr:+.3f} mm/yr [95% CI {ci_low:+.3f}, {ci_high:+.3f}]")
        print(f"    total drift over span:  {drift_mm:+.2f} mm over {trend['span_years']:.1f} yr")
    else:
        print("  Trend (Theil-Sen):        not computed (need >= 2 years of data)")

    if args.plot:
        # Resolve the per-source output directory: <parent>/<source>/, defaulting
        # the parent to tools/diagnostics/ .
        parent = Path(args.output_dir) if args.output_dir else _DEFAULT_DIAGNOSTICS_DIR
        out_dir = str(parent / args.source)
        paths = make_plots(
            time1,
            lat,
            ssh1,
            ssh2,
            stats,
            args.source,
            reference,
            out_dir,
            lowpass_cutoff_days=cutoff,
            trend=trend,
        )
        print("\nWrote plots:")
        for p in paths:
            print(f"  {p}")

    print("\nAdd under `common` in utilities/sources/{}.yaml:".format(args.source))
    print("  common:")
    print(
        f"    intermission_bias: {stats['median']:.4f}   "
        f"# m, median (ssh1 - ssh2) over {stats['count']} reference crossovers vs {reference}"
    )


if __name__ == "__main__":
    main()
