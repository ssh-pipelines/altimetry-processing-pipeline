from dataclasses import dataclass
from datetime import datetime
import logging
from typing import Type
import numpy as np
import xarray as xr

from utilities.aws_utils import aws_manager
from utilities.pipeline_layout import daily_file_key, daily_file_filename, s3_uri

from daily_files.config.source_config import SourceConfig, get_source_config
from daily_files.config.dataset_schema import assert_valid_dataset
from utilities.aviso_auth import build_aviso_session
from daily_files.fetching.downloader import (
    Downloader,
    HttpDownloader,
    S3Downloader,
    get_podaac_s3_credentials,
)

from daily_files.ingestion.aviso_l2p_ingest import AvisoL2PIngestor
from daily_files.ingestion.ingest import IngestedData, Ingestor
from daily_files.ingestion.gsfc_ingest import GSFCIngestor
from daily_files.ingestion.s6_ingest import S6Ingestor

from daily_files.processing.aviso_l2p_daily_file import AvisoL2PDailyFile
from daily_files.processing.daily_file import DailyFile
from daily_files.processing.empty_template import build_empty_dataset
from daily_files.processing.gsfc_daily_file import GSFCDailyFile
from daily_files.processing.s6_daily_file import S6DailyFile


@dataclass(frozen=True)
class SourcePipeline:
    downloader: Type[Downloader]
    downloader_kwargs: dict
    ingestor: Type[Ingestor]
    processor: Type[DailyFile]


SOURCE_REGISTRY: dict[str, SourcePipeline] = {
    "GSFC": SourcePipeline(
        downloader=S3Downloader,
        downloader_kwargs={"credentials_fn": get_podaac_s3_credentials},
        ingestor=GSFCIngestor,
        processor=GSFCDailyFile,
    ),
    "S6": SourcePipeline(
        downloader=S3Downloader,
        downloader_kwargs={"credentials_fn": get_podaac_s3_credentials},
        ingestor=S6Ingestor,
        processor=S6DailyFile,
    ),
    "S6B": SourcePipeline(
        downloader=S3Downloader,
        downloader_kwargs={"credentials_fn": get_podaac_s3_credentials},
        ingestor=S6Ingestor,
        processor=S6DailyFile,
    ),
    "GSFC_6.1": SourcePipeline(
        downloader=S3Downloader,
        downloader_kwargs={"credentials_fn": None},
        ingestor=GSFCIngestor,
        processor=GSFCDailyFile,
    ),
    "EXAMPLE_S3": SourcePipeline(
        downloader=S3Downloader,
        downloader_kwargs={"credentials_fn": None},
        ingestor=GSFCIngestor,
        processor=GSFCDailyFile,
    ),
    "S3B": SourcePipeline(
        downloader=HttpDownloader,
        downloader_kwargs={"session_fn": build_aviso_session},
        ingestor=AvisoL2PIngestor,
        processor=AvisoL2PDailyFile,
    ),
}


@dataclass
class AcquiredData:
    """Result of the data acquisition phase."""

    ingested_data: IngestedData
    granule_filenames: list[str]


class SourceNotSupported(Exception):
    pass


def _filename_from_uri(uri: str) -> str:
    return uri.rsplit("/", 1)[-1]


class DailyFileJob:
    def __init__(self, date: str, source: str):
        logging.info(f"Starting {source} job for {date}")
        self.date: datetime = datetime.strptime(date, "%Y-%m-%d")
        self.source: str = source
        self.source_config: SourceConfig = get_source_config(source)

        if source not in SOURCE_REGISTRY:
            raise SourceNotSupported(f"{source} is not currently supported. Available: {list(SOURCE_REGISTRY.keys())}")

        pipeline = SOURCE_REGISTRY[source]
        self.downloader_cls = pipeline.downloader
        self.downloader_kwargs = pipeline.downloader_kwargs
        self.ingestor_cls = pipeline.ingestor
        self.processor_cls = pipeline.processor

    def acquire(self, granules: list[str], bucket: str) -> AcquiredData | None:
        """Phase 1: Download granule URIs and ingest into normalized form."""
        if not granules:
            return None

        downloader = self.downloader_cls(**self.downloader_kwargs)
        file_objs = downloader.download_all(granules)

        filenames = [_filename_from_uri(uri) for uri in granules]
        ingestor = self.ingestor_cls()
        ingested_data = ingestor.ingest(file_objs, filenames=filenames, bucket=bucket)

        return AcquiredData(
            ingested_data=ingested_data,
            granule_filenames=filenames,
        )

    def process(self, acquired: AcquiredData) -> xr.Dataset:
        """Phase 2: Process ingested data into a daily file dataset."""
        return self.processor_cls(
            acquired.ingested_data,
            self.date,
            self.source_config,
            source_files=", ".join(acquired.granule_filenames),
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
        if any(x in var for x in ["ssha", "dac", "inv_bar_cor"]):
            encoding[var]["dtype"] = "float64"
            encoding[var]["_FillValue"] = np.finfo(np.float64).max

    logging.info(f"Saving netCDF to {output_path}")
    ds.to_netcdf(output_path, encoding=encoding)


def make_empty(job: DailyFileJob) -> xr.Dataset:
    """
    In the event no data is found we still want an empty daily file with the expected metadata.
    """
    logging.info(f"No {job.source} data found for {job.date}. Building empty dataset.")
    processor_cls = SOURCE_REGISTRY[job.source].processor
    daily_ds = build_empty_dataset(job.source_config, processor_cls)
    daily_ds.attrs["time_coverage_start"] = job.date.strftime("%Y-%m-%dT00:00:00Z")
    daily_ds.attrs["time_coverage_end"] = job.date.strftime("%Y-%m-%dT23:59:59Z")
    daily_ds.attrs["comment"] = "No data available from source"
    return daily_ds


def upload_ds(daily_ds: xr.Dataset, job: DailyFileJob, bucket: str):
    filename = daily_file_filename(job.source_config, job.date)
    out_path = f"/tmp/{filename}"
    save_ds(daily_ds, out_path)

    s3_output_path = s3_uri(bucket, daily_file_key(job.source_config, job.date, "p1"))
    aws_manager.upload_obj(out_path, s3_output_path)
    logging.info("Job complete.")
    daily_ds.close()


def start_job(date: str, source: str, bucket: str, granules: list[str]):
    job = DailyFileJob(date, source)

    acquired = job.acquire(granules, bucket)

    if acquired:
        daily_ds = job.process(acquired)
    else:
        daily_ds = make_empty(job)

    upload_ds(daily_ds, job, bucket)
