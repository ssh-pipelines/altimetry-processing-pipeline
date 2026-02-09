from datetime import datetime, timedelta
import logging
import numpy as np
import tempfile
import xarray as xr
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
    "ssha_smoothed": {"dtype": "float64", "_FillValue": np.finfo(np.float64).max},
    "dac": {"dtype": "float64", "_FillValue": np.finfo(np.float64).max},
    "oer": {"dtype": "float64", "_FillValue": np.finfo(np.float64).max},
}


class OerCorrection:
    """
    Class for handling each step required to generate daily file processing level 2,
    from pulling the required crossover and daily file files to uploading
    the polygon, oer, and daily file p2 netCDFs.
    """

    def __init__(self, source: str, date: datetime, bucket: str) -> None:
        self.source: str = source
        self.date: datetime = date
        self.bucket: str = bucket
        self.daily_file_filename = f"{source}-SSH_alt_ref_at_v1_{date.strftime('%Y%m%d')}.nc"
        self.window_len: int = 10  # set window, since xover files "look forward" in time
        self.window_pad: int = 1  # padding to avoid edge effects at window end
        logging.info(f"Starting job for {self.source} {self.date}")

    def save_ds(self, ds: xr.Dataset, local_filename: str, encoding: dict | None = None) -> str:
        """Save a dataset as netCDF to the temporary working directory."""
        out_path = f"{self._tmp_dir}/{local_filename}"
        ds.to_netcdf(out_path, engine="h5netcdf", encoding=encoding)
        return out_path

    def fetch_xovers(self, window_start: datetime, window_end: datetime) -> xr.Dataset:
        """Stream and concatenate crossover files from S3 for the given date range."""
        date_range = list(rrule(DAILY, dtstart=window_start, until=window_end))
        streams = []
        for d in date_range:
            filename = f"xovers_{self.source}-{d.strftime('%Y-%m-%d')}.nc"
            key = f"s3://{self.bucket}/crossovers/p1/{self.source}/{d.year}/{filename}"
            if aws_manager.key_exists(key):
                stream = aws_manager.stream_obj(key)
                streams.append(stream)
            else:
                logging.warning(f"Unable to stream {key} as it does not exist")
        if len(streams) == 0:
            raise RuntimeError("Unable to open any crossover files!")
        logging.info(f"Opening {len(streams)} xover files.")
        ds = xr.open_mfdataset(
            streams,
            concat_dim="time1",
            combine="nested",
            decode_times=False,
            drop_variables=["lon", "lat"],
        )
        if ds["time1"].size == 0 and len(streams) > 1:
            ds = xr.open_dataset(streams[0], decode_times=False, drop_variables=["lon", "lat"])
        return ds

    def fetch_daily_file(self) -> xr.Dataset:
        """Stream the processing-level-1 daily file from S3."""
        key = f"s3://{self.bucket}/daily_files/p1/{self.source}/{self.date.year}/{self.daily_file_filename}"
        if aws_manager.key_exists(key):
            stream = aws_manager.stream_obj(key)
        else:
            raise ValueError(f"Key {key} does not exist!")
        return xr.open_dataset(stream)

    def _save_and_upload(self, ds: xr.Dataset, filename: str, s3_prefix: str, encoding: dict | None = None) -> None:
        out_path = self.save_ds(ds, filename, encoding)
        target = f"s3://{self.bucket}/{s3_prefix}/{self.source}/{self.date.year}/{filename}"
        aws_manager.upload_obj(out_path, target)

    def make_polygon(self) -> xr.Dataset:
        """Fetch crossovers, fit the spline polygon, and upload to S3."""
        window_start = max(
            self.date - timedelta(self.window_len) - timedelta(self.window_pad),
            datetime(1992, 9, 25),
        )
        window_end = self.date + timedelta(self.window_pad)

        xover_ds = self.fetch_xovers(window_start, window_end)

        polygon_ds = create_polygon(xover_ds, self.date, self.source)
        xover_ds.close()

        polygon_filename = f"oerpoly_{self.source}_{self.date.strftime('%Y-%m-%d')}.nc"
        self._save_and_upload(polygon_ds, polygon_filename, "oer")
        return polygon_ds

    def make_correction(self, polygon_ds: xr.Dataset, daily_file_ds: xr.Dataset) -> xr.Dataset:
        """Evaluate the polygon correction at daily-file times and upload to S3."""
        correction_ds = evaluate_correction(polygon_ds, daily_file_ds, self.date, self.source)

        correction_filename = f"oer_correction_{self.source}_{self.date.strftime('%Y-%m-%d')}.nc"
        self._save_and_upload(correction_ds, correction_filename, "oer")
        return correction_ds

    def apply_oer(self, daily_file_ds: xr.Dataset, correction_ds: xr.Dataset) -> xr.Dataset:
        """Apply the OER correction to the daily file and upload the p2 result to S3."""
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

            if var in _DTYPE_OVERRIDES:
                encoding.setdefault(var, {}).update(_DTYPE_OVERRIDES[var])

        self._save_and_upload(ds, self.daily_file_filename, "daily_files/p2", encoding)
        return ds

    def run(self) -> None:
        """Run the full OER pipeline: polygon, correction, and daily-file update.

        Each intermediate and final netCDF is uploaded to ``self.bucket``.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            self._tmp_dir = tmp_dir

            polygon_ds = self.make_polygon()

            daily_file_ds = self.fetch_daily_file()

            correction_ds = self.make_correction(polygon_ds, daily_file_ds)
            polygon_ds.close()

            self.apply_oer(daily_file_ds, correction_ds)
            correction_ds.close()
            daily_file_ds.close()

        logging.info(f"OER complete for {self.source} {self.date}")
