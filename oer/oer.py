from datetime import datetime, timedelta
from glob import glob
import logging
import xarray as xr
import os
from oer.compute_oer_polygons import create_polygon
from oer.compute_oer_correction import evaluate_correction

def stream_window(window_start: datetime, window_end: datetime, satellite: str) -> xr.Dataset:
    print(f'Streaming files between {window_start}, {window_end}...')
    files = sorted(glob('/Users/username/Developer/Measures-Cloud/repos/OER_CLOUD/tests/test_xovers/*.nc'))
    files = filter(lambda x: os.path.basename(x) >= f'xovers_{satellite}-{window_start.strftime("%Y-%m-%d")}.nc', files)
    files = filter(lambda x: os.path.basename(x) <= f'xovers_{satellite}-{window_end.strftime("%Y-%m-%d")}.nc', files)
    ds = xr.open_mfdataset(sorted(list(files)), decode_times=False)
    return ds

def save_ds(ds: xr.Dataset, local_filename: str) -> str:
    out_path = os.path.join('/tmp', local_filename)    
    ds.to_netcdf(out_path)
    return out_path

def upload_ds(out_path: str):
    
    pass

def stream_daily_file(date: datetime, satellite: str):
    daily_file_filename = f'{satellite}-alt_ssh{date.strftime("%Y%m%d")}.nc'
    prefix = os.path.join('s3://example-bucket/daily_files/p1', satellite, date.year, daily_file_filename)

    
def make_polygon(date: datetime, satellite: str) -> xr.Dataset:
    window_len = 10  # set window, since xover files "look forward" in time
    window_pad = 1  # padding to avoid edge effects at window end
    window_start = max(date - timedelta(window_len) - timedelta(window_pad), datetime(1992, 9, 25))
    window_end = date + timedelta(window_pad)
    xover_ds = stream_window(window_start, window_end, satellite)
    
    polygon_ds = create_polygon(xover_ds, date, satellite)
    
    # Save the polygon and upload to S3
    polygon_filename = f'oerpoly_{satellite}_{date.strftime("%Y-%m-%d")}.nc'
    out_path = save_ds(polygon_ds, polygon_filename)
    upload_ds(out_path)
    
    return polygon_ds

def make_correction(polygon_ds: xr.Dataset, daily_file_ds: xr.Dataset, date: datetime, satellite: str):
    correction_ds = evaluate_correction(polygon_ds, daily_file_ds, date, satellite)

    # Save the correction and upload to S3
    correction_filename = f'oer_correction_{satellite}_{date.strftime("%Y-%m-%d")}.nc'
    out_path = save_ds(correction_ds, correction_filename)
    upload_ds(out_path)
    
def apply_correction():
    pass

def oer(date: datetime, satellite: str):
    '''
    create_polygon()
    evaluate_correction()
    '''
    # Create polygon
    polygon_ds = make_polygon(date, satellite)
    
    # Get corresponding daily file
    daily_file_ds = stream_daily_file(date, satellite)
    
    # Evaluate correction
    correction_ds = make_correction(polygon_ds, daily_file_ds, date, satellite)
    
    # Apply correction to daily file
    apply_correction(daily_file_ds, correction_ds)