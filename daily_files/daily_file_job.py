from datetime import datetime
import logging
import os
from typing import Iterable
import numpy as np
import xarray as xr

from daily_files.utils.logconfig import configure_logging
from daily_files.utils.aws_utils import aws_manager

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

    DAILY_FILE_BUCKET = "example-bucket"

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
        except:
            raise SourceNotSupported
        return fetcher

    @classmethod
    def get_processor(cls, source: str) -> DailyFile:
        try:
            processor = cls.SOURCE_MAPPINGS[source]["processor"]
            logging.debug(f"Using {processor} processor")
        except:
            raise SourceNotSupported
        return processor

    def fetch_granules(self):
        logging.info("Fetching granules...")
        self.fetcher = self.fetch_type(self.date)
        self.granules: Iterable[CMRGranule] = self.fetcher.granules


def save_ds(ds: xr.Dataset, output_path: str):
    logging.info(f"Setting netCDF encoding")
    ds = ds.set_coords(["latitude", "longitude"])
    encoding = {
        "time": {"units": "seconds since 1990-01-01 00:00:00", "dtype": "float64"}
    }
    for var in ds.variables:
        if var not in ["latitude", "longitude", "time", "basin_names"]:
            encoding[var] = {"complevel": 5, "zlib": True}
        elif "lat" in var or "lon" in var:
            encoding[var] = {"complevel": 5, "zlib": True, "dtype": "float32"}
        elif "basin_names" in var:
            encoding[var] = {
                "complevel": 5,
                "zlib": True,
                "char_dim_name": "basin_name_len",
            }

        if any(x in var for x in ["source_flag", "nasa_flag", "median_filter_flag"]):
            encoding[var]["dtype"] = "int8"
            encoding[var]["_FillValue"] = np.iinfo(np.int8).max
        if any(x in var for x in ["basin_flag", "pass", "cycle"]):
            encoding[var]["dtype"] = "int32"
            encoding[var]["_FillValue"] = np.iinfo(np.int32).max
        if any(x in var for x in ["ssh", "dac"]):
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

    filename = f'{job.satellite}-alt_ssh{str(job.date)[:10].replace("-","")}.nc'
    out_path = f"/tmp/{filename}"
    save_ds(daily_ds, out_path)

    s3_output_path = f"daily_files/{job.satellite}/{job.date.year}/{filename}"
    aws_manager.upload_s3(out_path, job.DAILY_FILE_BUCKET, s3_output_path)
    logging.info("Job complete.")
    daily_ds.close()


def make_empty(job: DailyFileJob):
    '''
    In the event no data is found we still want an empty daily file with the expected metadata.
    '''
    logging.info(
        f"No {job.source} data found for {job.date}. Using template file.")
    daily_ds = xr.open_dataset(
        os.path.join(
            "daily_files",
            "ref_files",
            "empty_templates",
            f"{job.source.lower()}_empty_template.nc",
        )
    )
    daily_ds.attrs["history"] = (
        f"Created on {datetime.now().isoformat(timespec='seconds')}"
    )
    daily_ds.attrs["source_files"] = ""

    filename = f'{job.satellite}-alt_ssh{str(job.date)[:10].replace("-","")}.nc'
    out_path = f"/tmp/{filename}"
    save_ds(daily_ds, out_path)

    s3_output_path = f"daily_files/{job.satellite}/{job.date.year}/{filename}"
    aws_manager.upload_s3(out_path, job.DAILY_FILE_BUCKET, s3_output_path)
    logging.info("Job complete.")


def start_job(event: dict):
    date = event.get("date")
    source: str = event.get("source")
    satellite = event.get("satellite")

    if None in [date, source, satellite]:
        raise RuntimeError(
            "One of date, source, or satellite job parameters missing. Job failure."
        )

    configure_logging(file_timestamp=False,
                      log_level=event.get("log_level", "INFO"))

    daily_file_job = DailyFileJob(date, source, satellite)
    daily_file_job.fetch_granules()
    if len(daily_file_job.granules) > 0:
        work(daily_file_job)
    else:
        make_empty(daily_file_job)
