from datetime import datetime, timedelta
import logging
import numpy as np
import xarray as xr
import os
from dateutil.rrule import rrule, DAILY

from oer.compute_polygon_correction import (
    create_polygon,
    evaluate_correction,
    apply_correction,
)
from utilities.aws_utils import aws_manager

_DTYPE_OVERRIDES = {
    "source_flag": {"dtype": "int8", "_FillValue": np.iinfo(np.int8).max},
    "nasa_flag": {"dtype": "int8", "_FillValue": np.iinfo(np.int8).max},
    "median_filter_flag": {"dtype": "int8", "_FillValue": np.iinfo(np.int8).max},
    "basin_flag": {"dtype": "int32", "_FillValue": np.iinfo(np.int32).max},
    "pass": {"dtype": "int32", "_FillValue": np.iinfo(np.int32).max},
    "cycle": {"dtype": "int32", "_FillValue": np.iinfo(np.int32).max},
    "ssha": {"dtype": "float64", "_FillValue": np.finfo(np.float64).max},
    "dac": {"dtype": "float64", "_FillValue": np.finfo(np.float64).max},
    "oer": {"dtype": "float64", "_FillValue": np.finfo(np.float64).max},
}


class OerCorrection:
    """
    Class for handling each step required to generate daily file processing level 2,
    from pulling the required crossover and daily file files to uploading
    the polygon, oer, and daily file p2 netCDFs.
    """

    def __init__(self, source: str, date: datetime) -> None:
        self.source: str = source
        self.date: datetime = date
        self.daily_file_filename = (
            f'{source}-SSH_alt_ref_at_v1_{date.strftime("%Y%m%d")}.nc'
        )
        self.window_len: int = (
            10  # set window, since xover files "look forward" in time
        )
        self.window_pad: int = 1  # padding to avoid edge effects at window end
        self._tmp_files: list[str] = []
        logging.info(f"Starting job for {self.source} {self.date}")

    def save_ds(
        self, ds: xr.Dataset, local_filename: str, encoding: dict = None
    ) -> str:
        """
        Save xarray dataset as netCDF to /tmp
        """
        out_path = os.path.join("/tmp", local_filename)
        ds.to_netcdf(out_path, engine="h5netcdf", encoding=encoding)
        self._tmp_files.append(out_path)
        return out_path

    def fetch_xovers(self, window_start: datetime, window_end: datetime, bucket: str) -> xr.Dataset:
        date_range = list(rrule(DAILY, dtstart=window_start, until=window_end))
        streams = []
        for d in date_range:
            filename = f'xovers_{self.source}-{d.strftime("%Y-%m-%d")}.nc'
            key = os.path.join(
                f"s3://{bucket}/crossovers/p1/",
                self.source,
                str(d.year),
                filename,
            )
            if aws_manager.key_exists(key):
                stream = aws_manager.stream_obj(key)
                streams.append(stream)
            else:
                logging.warning(f"Unable to stream {key} as it does not exist")
        if len(streams) == 0:
            raise RuntimeError("Unable to open any crossover files!")
        logging.info(f"Openining {len(streams)} xover files.")
        ds = xr.open_mfdataset(
            streams, concat_dim="time1", combine="nested", decode_times=False,
            drop_variables=["lon", "lat"],
        )
        if ds["time1"].size == 0 and len(streams) > 1:
            ds = xr.open_dataset(streams[0], decode_times=False,
                                 drop_variables=["lon", "lat"])
        return ds

    def fetch_daily_file(self, bucket: str) -> xr.Dataset:
        """
        Streams the p1 daily file
        """
        prefix = os.path.join(
            f"s3://{bucket}/daily_files/p1",
            self.source,
            str(self.date.year),
            self.daily_file_filename,
        )
        if aws_manager.key_exists(prefix):
            stream = aws_manager.stream_obj(prefix)
        else:
            raise ValueError(f"Key {prefix} does not exist!")
        return xr.open_dataset(stream)

    def make_polygon(self, bucket: str) -> xr.Dataset:
        window_start = max(
            self.date - timedelta(self.window_len) - timedelta(self.window_pad),
            datetime(1992, 9, 25),
        )
        window_end = self.date + timedelta(self.window_pad)

        xover_ds = self.fetch_xovers(window_start, window_end, bucket)

        polygon_ds = create_polygon(xover_ds, self.date, self.source)

        # Save the polygon as netCDF and upload to S3
        polygon_filename = f'oerpoly_{self.source}_{self.date.strftime("%Y-%m-%d")}.nc'
        out_path = self.save_ds(polygon_ds, polygon_filename)
        target_path = os.path.join(
            f"s3://{bucket}/oer",
            self.source,
            str(self.date.year),
            polygon_filename,
        )
        aws_manager.upload_obj(out_path, target_path)
        return polygon_ds

    def make_correction(
        self, polygon_ds: xr.Dataset, daily_file_ds: xr.Dataset, bucket: str
    ) -> xr.Dataset:
        correction_ds = evaluate_correction(
            polygon_ds, daily_file_ds, self.date, self.source
        )

        # Save the correction and upload to S3
        correction_filename = (
            f'oer_correction_{self.source}_{self.date.strftime("%Y-%m-%d")}.nc'
        )
        out_path = self.save_ds(correction_ds, correction_filename)
        target_path = os.path.join(
            f"s3://{bucket}/oer",
            self.source,
            str(self.date.year),
            correction_filename,
        )
        aws_manager.upload_obj(out_path, target_path)
        return correction_ds

    def apply_oer(
        self, daily_file_ds: xr.Dataset, correction_ds: xr.Dataset, bucket: str
    ) -> xr.Dataset:
        ds = apply_correction(daily_file_ds, correction_ds)

        if "time" in ds["basin_names_table"].dims:
            if ds["basin_names_table"].time.size > 0:
                ds["basin_names_table"] = ds["basin_names_table"].isel(time=0)
            else:
                ds["basin_names_table"] = ds["basin_names_table"].squeeze("time")

        ds = ds.set_coords(["latitude", "longitude"])
        encoding = {
            "time": {
                "units": "seconds since 1990-01-01 00:00:00",
                "dtype": "float64",
                "_FillValue": None,
            }
        }
        for var in ds.variables:
            if var == "time":
                continue
            elif var in ("latitude", "longitude"):
                encoding[var] = {"complevel": 5, "zlib": True, "dtype": "float32", "_FillValue": None}
            elif var != "basin_names_table":
                encoding[var] = {"complevel": 5, "zlib": True}

            for key, overrides in _DTYPE_OVERRIDES.items():
                if key in var:
                    encoding.setdefault(var, {}).update(overrides)
                    break

        # Save the correction and upload to S3
        out_path = self.save_ds(ds, self.daily_file_filename, encoding)
        target_path = os.path.join(
            f"s3://{bucket}/daily_files/p2",
            self.source,
            str(self.date.year),
            self.daily_file_filename,
        )
        aws_manager.upload_obj(out_path, target_path)
        return ds

    def run(self, bucket: str):
        """
        Executes the three steps for OER correction:
        1. Make the polygon
        2. Compute corrections using polygon and daily file
        3. Apply corrections to daily file

        Each step includes uploading netCDF to relevant bucket location
        """
        polygon_ds = self.make_polygon(bucket)

        daily_file_ds = self.fetch_daily_file(bucket)

        correction_ds = self.make_correction(polygon_ds, daily_file_ds, bucket)

        self.apply_oer(daily_file_ds, correction_ds, bucket)

        # Cleanup files saved to /tmp
        for f in self._tmp_files:
            if os.path.exists(f):
                os.remove(f)
        self._tmp_files.clear()

        logging.info(f"OER complete for {self.source} {self.date}")
