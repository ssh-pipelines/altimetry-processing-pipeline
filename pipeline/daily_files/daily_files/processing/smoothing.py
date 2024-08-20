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


def smooth_point(ssh_vals: np.ndarray) -> np.float32:
    """
    Compute smoothed value using precomputed filter weights
    """
    m = np.ma.masked_array(ssh_vals, np.isnan(ssh_vals))
    return np.ma.average(m, weights=FILTER_WEIGHTS)


def make_windows(ssh_vals: np.ndarray) -> np.ndarray:
    padded_vals = np.pad(ssh_vals, (9, 9), mode="constant", constant_values=np.nan)
    windows = np.empty((len(ssh_vals), 19), dtype=ssh_vals.dtype)
    for i in range(len(ssh_vals)):
        windows[i] = padded_vals[i : i + 19]
    return windows


def smooth(ssh_vals: np.ndarray) -> np.float64:
    """
    Interpolate NaNs, mirror NaNs, compute smoothed value. Smoothed value is set to NaN if entire window is NaN
    """
    if np.isnan(ssh_vals).all():
        return np.nan

    if np.isnan(ssh_vals).any():
        nan_i = np.isnan(ssh_vals)
        nnan_i = ~nan_i
        ssh_vals[nan_i] = np.interp(
            nan_i.nonzero()[0],
            nnan_i.nonzero()[0],
            ssh_vals[nnan_i],
            left=np.nan,
            right=np.nan,
        )
        if np.isnan(ssh_vals).any():
            ssh_vals[np.isnan(ssh_vals)[::-1]] = np.nan

    if np.isnan(ssh_vals).all():
        return np.nan
    return smooth_point(ssh_vals)


def ssh_smoothing(ds: xr.Dataset, date: datetime) -> xr.Dataset:
    logging.info("Beginning smoothing...")

    if len(ds.time) == 0:
        ds["ssh_smoothed"] = (("time"), np.array([], dtype="float64"))
        return ds

    global FILTER_WEIGHTS
    FILTER_WEIGHTS = create_filter("reference")

    # Pad ssh values with NaNs
    df = pd.DataFrame(
        {"ssh": ds.ssh.values, "flag": ds.nasa_flag.values}, ds.time.values
    )
    padded_df = df.reindex(np.arange(date, date + timedelta(1), dtype="datetime64[s]"))

    # Apply nasa_flag to ssh
    padded_df.values[padded_df.flag.values.astype(bool)] = np.nan

    # Generate rolling windows
    windows = make_windows(padded_df.ssh.values)

    # Compute smoothed values
    smoothed_vals = np.apply_along_axis(smooth, axis=1, arr=windows)

    # Index smoothed values to full day and then select original time values
    ssh_smoothed = pd.Series(smoothed_vals, index=padded_df.index)[
        pd.DatetimeIndex(ds.time.values)
    ]
    ds["ssh_smoothed"] = (("time"), ssh_smoothed.values.astype("float64"))

    return ds
