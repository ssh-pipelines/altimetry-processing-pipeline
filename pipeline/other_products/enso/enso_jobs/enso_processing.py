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
from utilities.source_profile import get_source_profile

aws_manager = AWSManager()


def start_job(date: datetime, bucket: str, source: str) -> str:
    """Process the ENSO grid (and maps) for a date. Returns the bucket-relative key of
    the ENSO grid written."""
    logging.info(f"Processing {source} grid for {date.date()}")

    profile = get_source_profile(source)

    # Stream simple grid from bucket based on date and source
    key = s3_uri(bucket, simple_grid_key(profile, date))
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
        grid_key = enso_grid_key(source, date)
        aws_manager.upload_obj(src, s3_uri(bucket, grid_key))
        os.remove(src)

        # Make maps
        mapper.make_maps(grid_ds)
        logging.info("Map making complete")

        for kind in ("ortho", "plate"):
            map_key = enso_map_key(source, date, kind)
            filename = map_key.rsplit("/", 1)[-1]
            src = f"/tmp/{filename}"
            aws_manager.upload_obj(src, s3_uri(bucket, map_key))
            os.remove(src)

        return grid_key

    except Exception as e:
        logging.exception(f"Error processing {date}: {e}")
        raise
