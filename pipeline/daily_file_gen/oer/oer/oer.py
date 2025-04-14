from datetime import datetime, timedelta
import logging
import numpy as np
import xarray as xr
import os
from glob import glob
from dateutil.rrule import rrule, DAILY

from oer.compute_polygon_correction import (
    create_polygon,
    evaluate_correction,
    apply_correction,
)
from utilities.aws_utils import aws_manager


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
        logging.info(f"Starting job for {self.source} {self.date}")

    def save_ds(
        self, ds: xr.Dataset, local_filename: str, encoding: dict = None
    ) -> str:
        """
        Save xarray dataset as netCDF to /tmp
        """
        out_path = os.path.join("/tmp", local_filename)
        if encoding:
            ds.to_netcdf(out_path, encoding=encoding)
        else:
            ds.to_netcdf(out_path, engine="h5netcdf")
        return out_path

    def fetch_xovers(self, window_start: datetime, window_end: datetime) -> xr.Dataset:
        date_range = list(rrule(DAILY, dtstart=window_start, until=window_end))
        streams = []
        for d in date_range:
            filename = f'xovers_{self.source}-{d.strftime("%Y-%m-%d")}.nc'
            key = os.path.join(
                "s3://example-bucket/crossovers/p1/",
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
        try:
            ds = xr.open_mfdataset(
                streams, concat_dim="time1", combine="nested", decode_times=False
            )
        except ValueError:
            # If all xovers are empty, just open one
            ds = xr.open_mfdataset(
                streams[0], concat_dim="time1", combine="nested", decode_times=False
            )
        return ds

    def fetch_daily_file(self) -> xr.Dataset:
        """
        Streams the p1 daily file from the example-bucket bucket.
        """
        prefix = os.path.join(
            "s3://example-bucket/daily_files/p1",
            self.source,
            str(self.date.year),
            self.daily_file_filename,
        )
        if aws_manager.key_exists(prefix):
            stream = aws_manager.stream_obj(prefix)
        else:
            raise ValueError(f"Key {prefix} does not exist!")
        return xr.open_dataset(stream)

    def make_polygon(self) -> xr.Dataset:
        window_start = max(
            self.date - timedelta(self.window_len) - timedelta(self.window_pad),
            datetime(1992, 9, 25),
        )
        window_end = self.date + timedelta(self.window_pad)

        xover_ds = self.fetch_xovers(window_start, window_end)

        polygon_ds = create_polygon(xover_ds, self.date, self.source)

        # Save the polygon as netCDF and upload to S3
        polygon_filename = f'oerpoly_{self.source}_{self.date.strftime("%Y-%m-%d")}.nc'
        out_path = self.save_ds(polygon_ds, polygon_filename)
        target_path = os.path.join(
            "s3://example-bucket/oer",
            self.source,
            str(self.date.year),
            polygon_filename,
        )
        aws_manager.upload_obj(out_path, target_path)
        return polygon_ds

    def make_correction(
        self, polygon_ds: xr.Dataset, daily_file_ds: xr.Dataset
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
            "s3://example-bucket/oer",
            self.source,
            str(self.date.year),
            correction_filename,
        )
        aws_manager.upload_obj(out_path, target_path)
        return correction_ds

    def apply_oer(
        self, daily_file_ds: xr.Dataset, correction_ds: xr.Dataset
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
            if var not in ["latitude", "longitude", "time", "basin_names_table"]:
                encoding[var] = {"complevel": 5, "zlib": True}
            elif "lat" in var or "lon" in var:
                encoding[var] = {
                    "complevel": 5,
                    "zlib": True,
                    "dtype": "float32",
                    "_FillValue": None,
                }

            if any(
                x in var for x in ["source_flag", "nasa_flag", "median_filter_flag"]
            ):
                encoding[var]["dtype"] = "int8"
                encoding[var]["_FillValue"] = np.iinfo(np.int8).max
            if any(x in var for x in ["basin_flag", "pass", "cycle"]):
                encoding[var]["dtype"] = "int32"
                encoding[var]["_FillValue"] = np.iinfo(np.int32).max
            if any(x in var for x in ["ssha", "dac", "oer"]):
                encoding[var]["dtype"] = "float64"
                encoding[var]["_FillValue"] = np.finfo(np.float64).max

        # Save the correction and upload to S3
        out_path = self.save_ds(ds, self.daily_file_filename, encoding)
        target_path = os.path.join(
            "s3://example-bucket/daily_files/p2",
            self.source,
            str(self.date.year),
            self.daily_file_filename,
        )
        aws_manager.upload_obj(out_path, target_path)
        return ds

    def run(self):
        """
        Executes the three steps for OER correction:
        1. Make the polygon
        2. Compute corrections using polygon and daily file
        3. Apply corrections to daily file

        Each step includes uploading netCDF to relevant bucket location
        """
        polygon_ds = self.make_polygon()

        daily_file_ds = self.fetch_daily_file()

        correction_ds = self.make_correction(polygon_ds, daily_file_ds)

        self.apply_oer(daily_file_ds, correction_ds)

        # Cleanup files saved to /tmp
        for f in glob("/tmp/*.nc"):
            os.remove(f)

        logging.info(f"OER complete for {self.source} {self.date}")
