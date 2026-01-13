from io import TextIOWrapper
import os
import logging
import numpy as np
import xarray as xr
from datetime import datetime, timedelta
from typing import Tuple, Optional

from simple_gridder.gridding import Gridder
from utilities.aws_utils import aws_manager


class SimpleGridderJob:
    def __init__(self, date: str, source: Optional[str], resolution: Optional[str]):
        logging.info(f"Starting {source} job for {date}")
        self.center_date: datetime = datetime.strptime(date, "%Y-%m-%d")
        self.start_date: datetime = self.center_date - timedelta(5)
        self.end_date: datetime = self.start_date + timedelta(9)
        self.source: Optional[str] = source
        self.resolution: Optional[str] = resolution
        if self.source:
            base_filename = f"{self.source}_alt_ref_simple_grid_v1" 
        else:
            base_filename = "NASA-SSH_alt_ref_simple_grid_v1"
            
        if resolution == "quart":
            base_filename = base_filename.replace("simple_grid_v1", "simple_grid_quart_v1")

        self.filename = f'{base_filename}_{self.center_date.strftime("%Y%m%d")}.nc'

    def fetch_daily_files(self, bucket: str) -> Tuple[list[TextIOWrapper], list[str]]:
        """
        Stream daily files from s3
        """

        streamed_objects = []
        streamed_filenames = []
        window_keys = self.generate_keys(bucket)
        for key in window_keys:
            try:
                if aws_manager.key_exists(key):
                    logging.debug(f"Streaming {key}")
                    obj = aws_manager.stream_obj(key)
                    streamed_objects.append(obj)
                    streamed_filenames.append(os.path.basename(key))
                else:
                    logging.warning(f"Unable to stream {key} as it does not exist")
            except Exception as e:
                logging.exception(f"Unable to process {key}: {e}")

        return streamed_objects, streamed_filenames

    def generate_keys(self, bucket: str):
        if self.source is None:
            prefix = f"s3://{bucket}/daily_files/p3"
        else:
            prefix = os.path.join(f"s3://{bucket}/daily_files/p2", self.source)

        dates_in_window = np.arange(
            self.start_date.strftime("%Y-%m-%d"),
            (self.end_date + timedelta(1)).strftime("%Y-%m-%d"),
            dtype="datetime64[D]",
        )
        keys = []
        for date in dates_in_window:
            date_dt = datetime.strptime(str(date), "%Y-%m-%d")
            if self.source is None:
                filename = f'NASA-SSH_alt_ref_at_v1_{date_dt.strftime("%Y%m%d")}.nc'
            else:
                filename = f'{self.source}-SSH_alt_ref_at_v1_{date_dt.strftime("%Y%m%d")}.nc'
            key = os.path.join(prefix, str(date_dt.year), filename)
            keys.append(key)
        logging.info(f"Generated {len(keys)} keys")
        return keys

    def ds_encoding(self, ds: xr.Dataset) -> dict:
        """
        Generates encoding dictionary used for saving the cycle netCDF file.
        """
        encoding = {"time": {"units": "seconds since 1990-01-01 00:00:00", "dtype": "float64", "_FillValue": None}}

        for var in ds.variables:
            if var not in ["latitude", "longitude", "time", "basin_names_table"]:
                encoding[var] = {"zlib": True, "complevel": 5}
            elif "lat" in var or "lon" in var:
                encoding[var] = {"complevel": 5, "zlib": True, "dtype": "float32", "_FillValue": None}

            if any(x in var for x in ["basin_flag", "counts"]):
                encoding[var]["dtype"] = "int32"
                encoding[var]["_FillValue"] = np.iinfo(np.int32).max
            if any(x in var for x in ["ssha"]):
                encoding[var]["dtype"] = "float64"
                encoding[var]["_FillValue"] = np.finfo(np.float64).max
        return encoding

    def save_grid(self, ds: xr.Dataset, dir: str = "/tmp"):
        filepath = os.path.join(dir, self.filename)
        logging.info(f"Saving grid to {filepath}")
        encoding = self.ds_encoding(ds)
        ds.to_netcdf(filepath, encoding=encoding)

    def upload_grid(self, bucket: str):
        filepath = os.path.join("/tmp", self.filename)

        bucket_path = f"s3://{bucket}/simple_grids"
        if self.source is None:
            if self.resolution == "quart":
                dst = os.path.join(bucket_path, "quart", str(self.center_date.year), self.filename)
            else:
                dst = os.path.join(bucket_path, "p3", str(self.center_date.year), self.filename)
        else:
            dst = os.path.join(bucket_path, "p2", self.source, str(self.center_date.year), self.filename)

        logging.info(f"Uploading {filepath} to {dst}")
        aws_manager.upload_obj(filepath, dst)


def start_job(date: str, source: str, resolution: Optional[str], bucket: str):
    """
    - date: str (%Y-%m-%d) The center of the ten day window
    - source: str | Iterable[str] The name(s) of along track sources to include in the grid
    """

    simple_gridder_job = SimpleGridderJob(date, source, resolution)
    df_objs, filenames = simple_gridder_job.fetch_daily_files(bucket)

    if not filenames:
        logging.info(f"No daily files found or opened for {source} on {date}")
        return

    gridder = Gridder(
        simple_gridder_job.center_date,
        simple_gridder_job.start_date,
        simple_gridder_job.end_date,
        filenames,
        df_objs,
        resolution,
    )

    ds = gridder.make_grid(simple_gridder_job.filename)

    simple_gridder_job.save_grid(ds)
    simple_gridder_job.upload_grid(bucket)
