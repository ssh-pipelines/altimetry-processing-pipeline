from datetime import datetime
import logging
import os
from typing import Iterable
import numpy as np
import xarray as xr

from utilities.aws_utils import aws_manager

from daily_files.fetching.fetcher import Fetcher
from daily_files.fetching.cmr_query import CMRGranule
from daily_files.fetching.gsfc_fetch import GSFCFetch
from daily_files.fetching.s6_fetch import S6Fetch

from daily_files.processing.daily_file import DailyFile
from daily_files.processing.gsfc_daily_file import GSFCDailyFile
from daily_files.processing.s6_daily_file import S6DailyFile


class SourceNotSupported(Exception):
    pass


class DailyFileJob:
    SOURCE_MAPPINGS = {
        "GSFC": {"fetcher": GSFCFetch, "processor": GSFCDailyFile},
        "S6": {"fetcher": S6Fetch, "processor": S6DailyFile},
    }

    def __init__(self, date: str, source: str, satellite: str):
        logging.info(f"Starting {source} job for {date}")
        self.date: datetime = datetime.strptime(date, "%Y-%m-%d")
        self.source: str = source
        self.satellite: str = satellite
        self.fetch_type: Fetcher = self.get_fetcher(source)
        self.processor: DailyFile = self.get_processor(source)

    @classmethod
    def get_fetcher(cls, source: str) -> Fetcher:
        try:
            fetcher = cls.SOURCE_MAPPINGS[source]["fetcher"]
            logging.debug(f"Using {fetcher} fetcher")
        except KeyError:
            raise SourceNotSupported(f"{source} is not currently supported")
        return fetcher

    @classmethod
    def get_processor(cls, source: str) -> DailyFile:
        try:
            processor = cls.SOURCE_MAPPINGS[source]["processor"]
            logging.debug(f"Using {processor} processor")
        except KeyError:
            raise SourceNotSupported(f"{source} is not currently supported")
        return processor

    def fetch_granules(self):
        logging.info("Fetching granules...")
        self.fetcher = self.fetch_type(self.date)
        self.granules: Iterable[CMRGranule] = self.fetcher.granules


def save_ds(ds: xr.Dataset, output_path: str):
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

        if any(x in var for x in ["source_flag", "nasa_flag", "median_filter_flag"]):
            encoding[var]["dtype"] = "int8"
            encoding[var]["_FillValue"] = np.iinfo(np.int8).max
        if any(x in var for x in ["basin_flag", "pass", "cycle"]):
            encoding[var]["dtype"] = "int32"
            encoding[var]["_FillValue"] = np.iinfo(np.int32).max
        if any(x in var for x in ["ssha", "dac"]):
            encoding[var]["dtype"] = "float64"
            encoding[var]["_FillValue"] = np.finfo(np.float64).max

    logging.info(f"Saving netCDF to {output_path}")
    ds.to_netcdf(output_path, encoding=encoding)


def work(job: DailyFileJob):
    """
    Opens and processes granules via direct S3 paths
    """
    file_objs = [job.fetcher.fetch(granule.s3_url) for granule in job.granules]
    collection_ids = [granule.collection_id for granule in job.granules]
    daily_ds = job.processor(file_objs, job.date, collection_ids).ds
    daily_ds.attrs["source_files"] = ", ".join(
        [granule.title for granule in job.granules]
    )

    filename = f'{job.satellite}-SSH_alt_ref_at_v1_{job.date.strftime("%Y%m%d")}.nc'
    out_path = f"/tmp/{filename}"
    save_ds(daily_ds, out_path)

    s3_output_path = os.path.join(
        "s3://example-bucket/daily_files/p1",
        job.satellite,
        str(job.date.year),
        filename,
    )
    aws_manager.upload_obj(out_path, s3_output_path)
    logging.info("Job complete.")
    daily_ds.close()


def make_empty(job: DailyFileJob):
    """
    In the event no data is found we still want an empty daily file with the expected metadata.
    """
    logging.info(f"No {job.source} data found for {job.date}. Using template file.")
    daily_ds = xr.open_dataset(
        os.path.join(
            "daily_files",
            "ref_files",
            "empty_templates",
            f"{job.source.lower()}_empty_template.nc",
        )
    )
    creation_time = datetime.now().isoformat(timespec="seconds")
    daily_ds.attrs["date_created"] = creation_time
    daily_ds.attrs["history"] = f"Created on {creation_time}"
    daily_ds.attrs["id"] = "10.5067/NSREF-AT0V1"
    daily_ds.attrs["source"] = ""
    daily_ds.attrs["source_files"] = ""
    daily_ds.attrs["source_url"] = ""
    daily_ds.attrs["time_coverage_start"] = job.date.strftime("%Y-%m-%dT00:00:00Z")
    daily_ds.attrs["time_coverage_end"] = job.date.strftime("%Y-%m-%dT23:59:59Z")
    daily_ds.attrs["comment"] = "No data available from source"

    filename = (
        f'{job.satellite}-SSH_alt_ref_at_v1_{str(job.date)[:10].replace("-","")}.nc'
    )
    out_path = f"/tmp/{filename}"
    save_ds(daily_ds, out_path)

    s3_output_path = os.path.join(
        "s3://example-bucket/daily_files/p1",
        job.satellite,
        str(job.date.year),
        filename,
    )
    aws_manager.upload_obj(out_path, s3_output_path)
    logging.info("Job complete.")


def start_job(date: str, source: str, satellite: str):
    daily_file_job = DailyFileJob(date, source, satellite)
    daily_file_job.fetch_granules()
    if len(daily_file_job.granules) > 0:
        work(daily_file_job)
    else:
        make_empty(daily_file_job)
