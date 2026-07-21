"""I/O adapter: stream daily files from S3 and decode them into a TrackWindow.

The only impure piece of the crossover pipeline. Keeps all S3/xarray contact in
one place so ``search`` and ``track_window`` stay pure and testable. A thin
adapter over ``aws_manager`` + ``pipeline_layout``.
"""

from __future__ import annotations

import logging
from typing import Iterable

import numpy as np
import xarray as xr
from crossover.track_window import TrackWindow

from utilities.aws_utils import aws_manager
from utilities.pipeline_layout import daily_file_key, s3_uri
from utilities.source_profile import SourceCommon

# Variables the crossover compute never reads — dropped at decode time.
DROP_VARIABLES = [
    "basin_flag",
    "median_filter_flag",
    "nasa_flag",
    "source_flag",
    "ssha",
    "dac",
]


def stream_files(
    profile: SourceCommon,
    window_start: np.datetime64,
    window_end: np.datetime64,
    df_version: str,
    bucket: str,
) -> list:
    """Return open streams for every existing daily file in the inclusive window.

    ``profile`` is any source-identity profile (a ``SourceConfig`` for the
    high-lat/self source, or a bare ``SourceCommon`` for the reference mission).
    Missing daily files are logged and skipped (an incomplete window still
    produces a valid, possibly-empty crossover file).
    """
    streams = []
    date = window_start
    while date <= window_end:
        date_dt = date.astype("datetime64[D]").astype(object)
        key = s3_uri(bucket, daily_file_key(profile, date_dt, df_version))

        if aws_manager.key_exists(key):
            streams.append(aws_manager.stream_obj(key))
        else:
            logging.info(f"No daily file for {date_dt}, skipping")

        date += np.timedelta64(1, "D")

    return streams


def load_track_window(
    streams: Iterable,
    drop_variables: list[str] = DROP_VARIABLES,
) -> TrackWindow:
    """Decode daily-file streams into a single TrackWindow.

    Concatenates all streams, drops rows with NaN ssha_smoothed, and hands the
    valid arrays to ``TrackWindow.from_arrays`` (which owns the grouping and the
    time-representation invariant).
    """
    time_chunks = []
    lon_chunks = []
    lat_chunks = []
    ssh_chunks = []
    cycle_chunks = []
    pass_chunks = []

    for stream in streams:
        ds = xr.open_dataset(stream, engine="h5netcdf", drop_variables=drop_variables)
        ssh = ds["ssha_smoothed"].values
        valid = ~np.isnan(ssh)
        time_chunks.append(ds["time"].values[valid])
        lon_chunks.append(ds["longitude"].values[valid])
        lat_chunks.append(ds["latitude"].values[valid])
        ssh_chunks.append(ssh[valid])
        cycle_chunks.append(ds["cycle"].values[valid])
        pass_chunks.append(ds["pass"].values[valid])
        ds.close()

    if not time_chunks:
        logging.info("No daily files in window, will produce empty crossover file")
        empty = np.array([])
        return TrackWindow.from_arrays(
            empty.astype("datetime64[ns]"), empty, empty, empty, empty, empty
        )

    time = np.concatenate(time_chunks)
    longitude = np.concatenate(lon_chunks)
    latitude = np.concatenate(lat_chunks)
    ssh = np.concatenate(ssh_chunks)
    cycle = np.concatenate(cycle_chunks)
    pass_ = np.concatenate(pass_chunks)

    if len(time) == 0:
        logging.info("No valid data in window, will produce empty crossover file")

    return TrackWindow.from_arrays(time, longitude, latitude, ssh, cycle, pass_)
