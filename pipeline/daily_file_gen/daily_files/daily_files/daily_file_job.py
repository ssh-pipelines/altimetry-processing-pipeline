from dataclasses import dataclass
from datetime import datetime
import logging
import os
from typing import Type
import numpy as np
import xarray as xr

from utilities.aws_utils import aws_manager

from daily_files.config.paths import REF_FILES_DIR
from daily_files.config.source_config import SourceConfig, get_source_config
from daily_files.config.dataset_schema import assert_valid_dataset
from daily_files.fetching.enumerator import Enumerator
from daily_files.fetching.cmr_enumerator import GSFCEnumerator, S6Enumerator
from daily_files.fetching.downloader import (
    Downloader,
    S3Downloader,
    get_podaac_s3_credentials,
)

from daily_files.ingestion.ingest import IngestedData, Ingestor
from daily_files.ingestion.gsfc_ingest import GSFCIngestor
from daily_files.ingestion.s6_ingest import S6Ingestor

from daily_files.processing.daily_file import DailyFile, get_base_global_attrs
from daily_files.processing.gsfc_daily_file import GSFCDailyFile
from daily_files.processing.s6_daily_file import S6DailyFile


@dataclass(frozen=True)
class SourcePipeline:
    enumerator: Type[Enumerator]
    downloader: Type[Downloader]
    downloader_kwargs: dict
    ingestor: Type[Ingestor]
    processor: Type[DailyFile]


SOURCE_REGISTRY: dict[str, SourcePipeline] = {
    "GSFC": SourcePipeline(
        enumerator=GSFCEnumerator,
        downloader=S3Downloader,
        downloader_kwargs={"credentials_fn": get_podaac_s3_credentials},
        ingestor=GSFCIngestor,
        processor=GSFCDailyFile,
    ),
    "S6": SourcePipeline(
        enumerator=S6Enumerator,
        downloader=S3Downloader,
        downloader_kwargs={"credentials_fn": get_podaac_s3_credentials},
        ingestor=S6Ingestor,
        processor=S6DailyFile,
    ),
    "S6B": SourcePipeline(
        enumerator=S6Enumerator,
        downloader=S3Downloader,
        downloader_kwargs={"credentials_fn": get_podaac_s3_credentials},
        ingestor=S6Ingestor,
        processor=S6DailyFile,
    ),
}


@dataclass
class AcquiredData:
    """Result of the data acquisition phase."""

    ingested_data: IngestedData
    collection_ids: list[str]
    granule_titles: list[str]


class SourceNotSupported(Exception):
    pass


class DailyFileJob:
    def __init__(self, date: str, source: str):
        logging.info(f"Starting {source} job for {date}")
        self.date: datetime = datetime.strptime(date, "%Y-%m-%d")
        self.source: str = source
        self.source_config: SourceConfig = get_source_config(source)
        self.satellite: str = self.source_config.satellite

        if source not in SOURCE_REGISTRY:
            raise SourceNotSupported(f"{source} is not currently supported. Available: {list(SOURCE_REGISTRY.keys())}")

        pipeline = SOURCE_REGISTRY[source]
        self.enumerator_cls = pipeline.enumerator
        self.downloader_cls = pipeline.downloader
        self.downloader_kwargs = pipeline.downloader_kwargs
        self.ingestor_cls = pipeline.ingestor
        self.processor_cls = pipeline.processor

    def acquire(self, bucket: str) -> AcquiredData | None:
        """Phase 1: Enumerate granules, download files, and ingest into normalized form."""
        logging.info("Enumerating granules...")
        enumerator = self.enumerator_cls(self.date, self.source_config)
        file_refs = enumerator.enumerate()

        if not file_refs:
            return None

        downloader = self.downloader_cls(**self.downloader_kwargs)
        file_objs = downloader.download_all(file_refs)

        ingestor = self.ingestor_cls()
        ingested_data = ingestor.ingest(file_objs, bucket=bucket)

        return AcquiredData(
            ingested_data=ingested_data,
            collection_ids=[f.collection_id for f in file_refs],
            granule_titles=[f.title for f in file_refs],
        )

    def process(self, acquired: AcquiredData) -> xr.Dataset:
        """Phase 2: Process ingested data into a daily file dataset."""
        return self.processor_cls(
            acquired.ingested_data,
            self.date,
            self.source_config,
            acquired.collection_ids,
            source_files=", ".join(acquired.granule_titles),
        ).ds


def save_ds(ds: xr.Dataset, output_path: str):
    assert_valid_dataset(ds)
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


def make_empty(job: DailyFileJob) -> xr.Dataset:
    """
    In the event no data is found we still want an empty daily file with the expected metadata.
    """
    logging.info(f"No {job.source} data found for {job.date}. Using template file.")
    daily_ds = xr.open_dataset(os.path.join(REF_FILES_DIR, "empty_templates", job.source_config.empty_template))
    base_attrs = get_base_global_attrs()
    daily_ds.attrs.update(base_attrs)
    daily_ds.attrs["time_coverage_start"] = job.date.strftime("%Y-%m-%dT00:00:00Z")
    daily_ds.attrs["time_coverage_end"] = job.date.strftime("%Y-%m-%dT23:59:59Z")
    daily_ds.attrs["comment"] = "No data available from source"
    return daily_ds


def _get_output_filename(job: DailyFileJob) -> str:
    return job.source_config.filename_template.format(
        satellite=job.satellite,
        date=job.date.strftime("%Y%m%d"),
    )


def upload_ds(daily_ds: xr.Dataset, job: DailyFileJob, bucket: str):
    filename = _get_output_filename(job)
    out_path = f"/tmp/{filename}"
    save_ds(daily_ds, out_path)

    s3_output_path = os.path.join(
        f"s3://{bucket}/{job.source_config.s3_prefix}",
        job.satellite,
        str(job.date.year),
        filename,
    )
    aws_manager.upload_obj(out_path, s3_output_path)
    logging.info("Job complete.")
    daily_ds.close()


def start_job(date: str, source: str, bucket: str):
    job = DailyFileJob(date, source)

    # Phase 1: Acquire data
    acquired = job.acquire(bucket)

    # Phase 2: Process into daily file
    if acquired:
        daily_ds = job.process(acquired)
    else:
        daily_ds = make_empty(job)

    upload_ds(daily_ds, job, bucket)
