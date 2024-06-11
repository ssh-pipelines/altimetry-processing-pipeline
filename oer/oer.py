from datetime import datetime, timedelta
import logging
import xarray as xr
import os
from dateutil.rrule import rrule, DAILY

from oer.compute_polygon_correction import create_polygon, evaluate_correction, apply_correction
from oer.utils.s3_utils import S3Utils

s3_utils = S3Utils()

def save_ds(ds: xr.Dataset, local_filename: str) -> str:
    out_path = os.path.join('/tmp', local_filename)    
    ds.to_netcdf(out_path)
    return out_path

def fetch_xovers(window_start: datetime, window_end: datetime, satellite: str) -> xr.Dataset:
    date_range = list(rrule(DAILY, dtstart=window_start, until=window_end))
    streams = []
    for d in date_range:
        key = os.path.join(f's3://example-bucket/crossovers/p1/{satellite}', str(d.year), f'xovers_{satellite}-{d.strftime("%Y-%m-%d")}.nc')
        if s3_utils.key_exists(key):
            stream = s3_utils.stream_s3(key)
            streams.append(stream)
        else:
            logging.warning(f'Unable to stream {key} as it does not exist')
    return xr.open_mfdataset(streams, decode_times=False)

def fetch_daily_file(date: datetime, satellite: str, df_filename: str) -> xr.Dataset:
    prefix = os.path.join('s3://example-bucket/daily_files/p1', satellite, str(date.year), df_filename)
    if s3_utils.key_exists(prefix):
        stream = s3_utils.stream_s3(prefix)
    else:
        raise ValueError(f'Key {prefix} does not exist!')
    return xr.open_dataset(stream)
    
def make_polygon(date: datetime, satellite: str) -> xr.Dataset:
    window_len = 10  # set window, since xover files "look forward" in time
    window_pad = 1  # padding to avoid edge effects at window end
    window_start = max(date - timedelta(window_len) - timedelta(window_pad), datetime(1992, 9, 25))
    window_end = date + timedelta(window_pad)
    
    xover_ds = fetch_xovers(window_start, window_end, satellite)
    
    polygon_ds = create_polygon(xover_ds, date, satellite)
    
    # Save the polygon as netCDF and upload to S3
    polygon_filename = f'oerpoly_{satellite}_{date.strftime("%Y-%m-%d")}.nc'
    out_path = save_ds(polygon_ds, polygon_filename)
    target_path = os.path.join('s3://example-bucket/oer', satellite, date.year, polygon_filename)
    s3_utils.upload_s3(out_path, target_path)
    return polygon_ds

def make_correction(polygon_ds: xr.Dataset, daily_file_ds: xr.Dataset, date: datetime, satellite: str) -> xr.Dataset:
    correction_ds = evaluate_correction(polygon_ds, daily_file_ds, date, satellite)

    # Save the correction and upload to S3
    correction_filename = f'oer_correction_{satellite}_{date.strftime("%Y-%m-%d")}.nc'
    out_path = save_ds(correction_ds, correction_filename)
    target_path = os.path.join('s3://example-bucket/oer', satellite, date.year, correction_filename)
    s3_utils.upload_s3(out_path, target_path)
    return correction_ds
    
def apply_oer(daily_file_ds: xr.Dataset, correction_ds: xr.Dataset, satellite: str, date: datetime, daily_file_filename: str) -> xr.Dataset:
    ds = apply_correction(daily_file_ds, correction_ds)
    if 'time' in ds['basin_names_table'].dims:
        ds['basin_names_table'] = ds['basin_names_table'].isel(time=0)
    # Save the correction and upload to S3
    out_path = save_ds(ds, daily_file_filename)
    target_path = os.path.join('s3://example-bucket/daily_files/p2', satellite, str(date.year), daily_file_filename)
    s3_utils.upload_s3(out_path, target_path)    
    return ds

def oer(date: datetime, satellite: str, log_level='INFO'):
    '''
    create_polygon()
    evaluate_correction()
    '''
    logging.root.handlers = []
    logging.basicConfig(
        level=log_level,
        format='[%(levelname)s] %(asctime)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    
    # Create polygon
    polygon_ds = make_polygon(date, satellite)
    
    # Get corresponding daily file
    daily_file_filename = f'{satellite}-alt_ssh{date.strftime("%Y%m%d")}.nc'
    daily_file_ds = fetch_daily_file(date, satellite, daily_file_filename)

    # Evaluate correction
    correction_ds = make_correction(polygon_ds, daily_file_ds, date, satellite)
    
    # Apply correction to daily file
    apply_oer(daily_file_ds, correction_ds, satellite, date, daily_file_filename)
    logging.info(f'OER complete for {satellite} {date}')