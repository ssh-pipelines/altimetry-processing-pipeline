import logging
import os
from datetime import datetime

import xarray as xr

from enso_jobs.ensogridder import ENSOGridder
from enso_jobs.ensomapper import ENSOMapper
from utilities.aws_utils import AWSManager
from utilities.pipeline_layout import (
    enso_filename,
    enso_grid_key,
    enso_map_key,
    s3_uri,
    simple_grid_key,
)

aws_manager = AWSManager()


def start_job(date: datetime, bucket: str, published_product: str) -> str:
    """Process the ENSO grid (and maps) for a date. Returns the bucket-relative key of
    the ENSO grid written.

    Keyed on a **published product**, not a Source: ENSO derives from the
    reference-mission grid only, and that grid is composed from several missions.
    It reads the grid's internal staging copy — the ENSO grids and maps are
    website assets, so they stay in the internal layout and are not promoted.
    """
    logging.info(f"Processing {published_product} grid for {date.date()}")

    # Stream the staged simple grid for this published product and date
    key = s3_uri(bucket, simple_grid_key(published_product, date))
    try:
        streamed_data = aws_manager.stream_obj(key)
        ds = xr.open_dataset(streamed_data, engine="h5netcdf")
    except Exception as e:
        logging.exception(f"Error attempting to stream {key}: {e}")
        raise RuntimeError(e)

    try:
        grid_processer = ENSOGridder()
        mapper = ENSOMapper()
    except Exception as e:
        logging.exception(e)
        raise RuntimeError(e)

    try:
        # Make grids
        grid_ds = grid_processer.process_grid(ds, date)
        logging.info("Grid making complete")

        filename = enso_filename(date)
        src = f"/tmp/{filename}"
        grid_key = enso_grid_key(published_product, date)
        aws_manager.upload_obj(src, s3_uri(bucket, grid_key))
        os.remove(src)

        # Make maps
        mapper.make_maps(grid_ds)
        logging.info("Map making complete")

        for kind in ("ortho", "plate"):
            map_key = enso_map_key(published_product, date, kind)
            filename = map_key.rsplit("/", 1)[-1]
            src = f"/tmp/{filename}"
            aws_manager.upload_obj(src, s3_uri(bucket, map_key))
            os.remove(src)

        return grid_key

    except Exception as e:
        logging.exception(f"Error processing {date}: {e}")
        raise
