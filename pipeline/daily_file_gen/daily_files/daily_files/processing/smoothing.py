from datetime import datetime, timedelta
import xarray as xr
import pandas as pd
import numpy as np
import logging


def create_filter(satellite: str) -> np.ndarray:
    """
    Generate 19 point Gaussian-like normalized filter specific to a given satellite's speed
    """
    match satellite:
        case "reference":
            speed = 5.745
            sigma = 15
        case _:
            raise RuntimeError(f"{satellite} is an invalid satellite type.")

    # Apply the Gaussian-like filter
    filter_values = np.exp(-(((np.arange(-9, 19 - 9) * speed) / sigma) ** 2))
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


def ssha_smoothing(ds: xr.Dataset, date: datetime) -> xr.Dataset:
    logging.info("Beginning smoothing...")

    if len(ds.time) == 0:
        ds["ssha_smoothed"] = (("time"), np.array([], dtype="float64"))
        return ds

    global FILTER_WEIGHTS
    FILTER_WEIGHTS = create_filter("reference")

    # Pad ssh values with NaNs
    df = pd.DataFrame(
        {"ssha": ds["ssha"].values, "flag": ds["nasa_flag"].values}, ds["time"].values
    )
    padded_df = df.reindex(np.arange(date, date + timedelta(1), dtype="datetime64[s]"))

    # Apply nasa_flag to ssha
    padded_df.values[padded_df.flag.values.astype(bool)] = np.nan

    # Generate rolling windows
    windows = make_windows(padded_df.ssha.values)

    # Compute smoothed values
    smoothed_vals = np.apply_along_axis(smooth, axis=1, arr=windows)

    # Index smoothed values to full day and then select original time values
    ssha_smoothed = pd.Series(smoothed_vals, index=padded_df.index)[
        pd.DatetimeIndex(ds["time"].values)
    ]
    ds["ssha_smoothed"] = (("time"), ssha_smoothed.values.astype("float64"))

    return ds
