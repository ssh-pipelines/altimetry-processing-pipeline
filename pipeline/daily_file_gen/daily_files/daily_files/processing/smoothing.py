import logging

import numpy as np
import pandas as pd
import xarray as xr

from daily_files.config.source_config import get_source_config

# Samples either side of the window centre. A gap wider than this cannot be seen
# from inside a 19-point window, so omitted-sample counts are capped here.
_HALF_WINDOW = 9

# Steps used to estimate the local sample period.
_DT_LOCAL_STEPS = 21

# A gap whose length in local periods is further than this from a whole number
# has no well-determined omitted-sample count.
_AMBIGUOUS_FRACTION = 0.25


def create_filter(source: str) -> np.ndarray:
    """
    Generate 19 point Gaussian-like normalized filter specific to a given source's speed.
    Filter parameters are loaded from the source configuration.
    """
    source_config = get_source_config(source)
    speed = source_config.ground_speed
    sigma = source_config.smoothing.sigma

    # Apply the Gaussian-like filter. Taps are one sample apart, which is
    # sample_period seconds of travel, not one second.
    filter_values = np.exp(
        -(((np.arange(-9, 19 - 9) * speed * source_config.sample_period) / sigma) ** 2)
    )
    # Normalize the filtered values
    normalized_filter = filter_values / np.sum(filter_values)
    return normalized_filter


def smooth_point(ssha_vals: np.ndarray) -> np.float32:
    """
    Compute smoothed value using precomputed filter weights
    """
    m = np.ma.masked_array(ssha_vals, np.isnan(ssha_vals))
    return np.ma.average(m, weights=FILTER_WEIGHTS)


def make_windows(ssha_vals: np.ndarray) -> np.ndarray:
    padded_vals = np.pad(ssha_vals, (9, 9), mode="constant", constant_values=np.nan)
    windows = np.empty((len(ssha_vals), 19), dtype=ssha_vals.dtype)
    for i in range(len(ssha_vals)):
        windows[i] = padded_vals[i : i + 19]
    return windows


def smooth(ssha_vals: np.ndarray) -> np.float64:
    """
    Interpolate NaNs, mirror NaNs, compute smoothed value. Smoothed value is set to NaN if entire window is NaN
    """
    # Point is NaN'd if all window to left and right are NaNs regardless of point value
    if np.isnan(ssha_vals[:9]).all() and np.isnan(ssha_vals[10:]).all():
        return np.nan

    # If any NaNs we need to interpolate
    if np.isnan(ssha_vals).any():
        nan_i = np.isnan(ssha_vals)
        nnan_i = ~nan_i

        ssha_vals[nan_i] = np.interp(
            nan_i.nonzero()[0],
            nnan_i.nonzero()[0],
            ssha_vals[nnan_i],
            left=np.nan,
            right=np.nan,
        )

        # Any remaining NaNs get mirrored across window
        if np.isnan(ssha_vals).any():
            ssha_vals[np.isnan(ssha_vals)[::-1]] = np.nan

        # Again check for all NaNs in left and right of window
        if np.isnan(ssha_vals[:9]).all() and np.isnan(ssha_vals[10:]).all():
            return np.nan

    return smooth_point(ssha_vals)


def sample_offsets(time: np.ndarray, sample_period: float) -> np.ndarray:
    """
    Position of each sample in a series where every omitted sample has a slot.

    Sources omit records over land rather than writing NaN rows, so a window run
    over the samples alone would reach across a land crossing. Counting the omitted
    samples, rather than placing them on a clock grid, keeps the window 19 sample
    periods wide at any sampling rate and with any timestamp alignment.

    The count is the gap length in local sample periods, where the local period is
    a rolling median of the surrounding regular steps: some sources' clocks drift
    too far within a day for the configured `sample_period` to place every sample.
    `sample_period` decides which steps are regular and stands in where there are
    none nearby.
    """
    if len(time) < 2:
        return np.zeros(len(time), dtype=np.int64)

    t = time.astype("datetime64[ns]").astype(np.int64)
    d = np.diff(t).astype(np.float64) / 1e9

    regular = np.where(d < 1.5 * sample_period, d, np.nan)
    dt_local = (
        pd.Series(regular)
        .rolling(_DT_LOCAL_STEPS, center=True, min_periods=1)
        .median()
        .fillna(sample_period)
        .to_numpy()
    )

    periods = d / dt_local
    n_missing = np.clip(np.round(periods) - 1, 0, _HALF_WINDOW).astype(np.int64)

    ambiguous = (
        (periods < _HALF_WINDOW + 1)
        & (np.abs(periods - np.round(periods)) > _AMBIGUOUS_FRACTION)
    )
    if ambiguous.any():
        logging.debug(
            f"{ambiguous.sum()} gaps with ambiguous omitted-sample counts "
            f"(local periods: {np.round(periods[ambiguous], 3).tolist()})"
        )

    return np.concatenate([[0], np.cumsum(1 + n_missing)])


def ssha_smoothing(ds: xr.Dataset, source: str) -> xr.Dataset:
    logging.info("Beginning smoothing...")

    if len(ds.time) == 0:
        ds["ssha_smoothed"] = (("time"), np.array([], dtype="float64"))
        return ds

    global FILTER_WEIGHTS
    FILTER_WEIGHTS = create_filter(source)

    # Omitted samples stay NaN; flagged samples are NaN'd in place.
    offsets = sample_offsets(
        ds["time"].values, get_source_config(source).sample_period
    )
    padded = np.full(offsets[-1] + 1, np.nan)
    padded[offsets] = np.where(
        ds["nasa_flag"].values.astype(bool), np.nan, ds["ssha"].values
    )

    windows = make_windows(padded)[offsets]
    smoothed_vals = np.apply_along_axis(smooth, axis=1, arr=windows)
    ds["ssha_smoothed"] = (("time"), smoothed_vals.astype("float64"))

    return ds
