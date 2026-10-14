import logging
import os
from datetime import datetime, timedelta
from io import TextIOWrapper
from typing import Optional, Tuple

import numpy as np
import xarray as xr

from simple_gridder.gridding import Gridder
from utilities.aws_utils import aws_manager
from utilities.pipeline_layout import (
    DEVELOPMENT_GRID_PRODUCT,
    development_simple_grid_key,
    published_along_track_key,
    s3_uri,
    simple_grid_key,
)
from utilities.products import published_simple_grid_product
from utilities.source_profile import get_product


class SimpleGridderJob:
    """One date's grid: read whatever the contributors have, grid it.

    Two independent inputs, where there used to be one `source` doing both jobs:

    - ``contributors`` — the **published products** to read. Always published,
      never internal staging, so gridding is decoupled from Source entirely.
    - ``publish_as`` — the published product this grid *becomes*, or ``None`` for
      development output.

    Whether the result is an official product is decided by ``publish_as`` alone,
    never inferred from the contributor set: set-equality against a derived
    expectation fails silently in both directions, and a diverged set would route
    a real product into the development directory on a successful run.

    A ``None`` ``publish_as`` resolves to the development Product regardless of
    contributors. Deriving its identity from the contributors instead would write
    the real reference DOI into a throwaway grid over ``["NASA-SSH"]``.
    """

    def __init__(
        self,
        date: str,
        bucket: str,
        contributors: list[str],
        publish_as: Optional[str],
        resolution: Optional[str],
    ):
        if not contributors:
            raise ValueError("A grid needs at least one contributing published product.")

        logging.info(
            f"Starting grid job for {date}: contributors={contributors}, "
            f"publish_as={publish_as}"
        )
        self.center_date: datetime = datetime.strptime(date, "%Y-%m-%d")
        self.start_date: datetime = self.center_date - timedelta(5)
        self.end_date: datetime = self.start_date + timedelta(9)

        self.bucket: str = bucket
        self.contributors: list[str] = list(contributors)
        self.publish_as: Optional[str] = publish_as

        if publish_as is None:
            self.product = get_product(DEVELOPMENT_GRID_PRODUCT)
            self.key = development_simple_grid_key(
                "_".join(self.contributors), self.center_date
            )
        else:
            self.product = published_simple_grid_product(publish_as)
            self.key = simple_grid_key(publish_as, self.center_date)

        if resolution == "quart":
            # Known one-off: the quarter-degree grids sit outside the layout's
            # key families and are not a published product.
            self.key = self.key.replace("simple_grids/", "simple_grids/quart_deg/")
            self.key = self.key.replace("simple_grid_v1_1", "simple_grid_quart_v1_1")

        self.filename = self.key.rsplit("/", 1)[-1]
        self.dst = s3_uri(self.bucket, self.key)

    def fetch_daily_files(self) -> Tuple[list[TextIOWrapper], list[str]]:
        """
        Stream daily files from s3
        """

        streamed_objects = []
        streamed_filenames = []
        window_keys = self.generate_keys()
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

    def generate_keys(self):
        """Every contributor's published along-track key across the 10-day window.

        Contributors with disjoint operating spans contribute nothing on most
        dates; `fetch_daily_files` logs and skips the keys that do not exist.
        """
        dates_in_window = np.arange(
            self.start_date.strftime("%Y-%m-%d"),
            (self.end_date + timedelta(1)).strftime("%Y-%m-%d"),
            dtype="datetime64[D]",
        )
        keys = []
        for d in dates_in_window:
            date_dt = datetime.strptime(str(d), "%Y-%m-%d")
            for contributor in self.contributors:
                keys.append(
                    s3_uri(self.bucket, published_along_track_key(contributor, date_dt))
                )
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

    def upload_grid(self):
        filepath = os.path.join("/tmp", self.filename)
        logging.info(f"Uploading {filepath} to {self.dst}")
        aws_manager.upload_obj(filepath, self.dst)


def start_job(
    date: str,
    contributors: list[str],
    publish_as: Optional[str],
    resolution: Optional[str],
    bucket: str,
) -> Optional[str]:
    """
    - date: str (%Y-%m-%d) The center of the ten day window
    - contributors: the published products whose along-track data the grid reads
    - publish_as: the published product this grid becomes, or None for development

    Returns the bucket-relative key of the grid written, or ``None`` when the date is
    skipped because no daily files were available in the window.
    """

    simple_gridder_job = SimpleGridderJob(date, bucket, contributors, publish_as, resolution)
    df_objs, filenames = simple_gridder_job.fetch_daily_files()

    if not filenames:
        logging.info(f"No daily files found or opened for {contributors} on {date}")
        return None

    gridder = Gridder(
        simple_gridder_job.center_date,
        simple_gridder_job.start_date,
        simple_gridder_job.end_date,
        filenames,
        df_objs,
        resolution,
        simple_gridder_job.product,
    )

    ds = gridder.make_grid(simple_gridder_job.filename)

    simple_gridder_job.save_grid(ds)
    simple_gridder_job.upload_grid()

    return simple_gridder_job.key
