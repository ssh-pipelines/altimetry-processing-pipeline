import logging
import os
import xarray as xr
from datetime import datetime

from enso_jobs.ensogridder import ENSOGridder
from enso_jobs.ensomapper import ENSOMapper
from utilities.aws_utils import AWSManager

aws_manager = AWSManager()


def start_job(date: datetime, bucket: str):
    logging.info(f"Processing grid for {date.date()}")

    # Stream simple grid from bucket based on date
    filename = f'NASA-SSH_alt_ref_simple_grid_v1_{date.strftime("%Y%m%d")}.nc'
    key = os.path.join(f"s3://{bucket}/simple_grids/p3", str(date.year), filename)
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
        
        date_str = date.strftime("%Y%m%d")

        filename = f'ENSO_{date_str}.nc'
        src = f"/tmp/{filename}"
        dst = f"s3://{bucket}/enso_grids/{filename}"
        aws_manager.upload_obj(src, dst)

        # Make maps
        mapper.make_maps(grid_ds)
        logging.info("Map making complete")
        
        filename = f'ENSO_ortho_{date_str}.png'
        src = f"/tmp/{filename}"
        dst = f"s3://{bucket}/maps/enso_maps/ortho/{filename}"
        aws_manager.upload_obj(src, dst)

        filename = f'ENSO_plate_{date_str}.png'
        src = f"/tmp/{filename}"
        dst = f"s3://{bucket}/maps/enso_maps/plate/{filename}"
        aws_manager.upload_obj(src, dst)

    except Exception as e:
        logging.exception(f"Error processing {date}: {e}")
