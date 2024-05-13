'''
Create polygon
Use polygon to get corrections for daily file
Apply corrections to daily file
'''

from datetime import datetime, timedelta
from glob import glob
import xarray as xr
import os
from compute_oer_polygons import create_polygon
from compute_oer_correction import evaluate_correction

def stream_window(window_start: datetime, window_end: datetime, satellite: str) -> xr.Dataset:
    files = glob('/Users/username/Developer/Measures-Cloud/repos/xover_cloud/OER_Files/crossover_files/GSFC/1992/*.nc')
    files = filter(lambda x: os.path.basename(x) >= f'xovers_{satellite}-{window_start.strftime("%Y-%m-%d")}.nc', files)
    files = filter(lambda x: os.path.basename(x) <= f'xovers_{satellite}-{window_end.strftime("%Y-%m-%d")}.nc', files)
    ds = xr.open_mfdataset(sorted(list(files)), decode_times=False)
    return ds

def save_ds(ds: xr.Dataset, date: datetime, satellite: str) -> str:
    out_path = os.path.join('/tmp', f'oerpoly_{satellite}_{date.strftime("%Y-%m-%d")}.nc')
    ds.to_netcdf(out_path)
    return out_path

def upload_ds(out_path: str):
    
    pass

def oer(date: datetime, satellite: str):
    '''
    create_polygon()
    evaluate_correction()
    '''
    window_len = 10  # set window, since xover files "look forward" in time
    window_pad = 1  # padding to avoid edge effects at window end

    window_start = max(date - timedelta(window_len) - timedelta(window_pad), datetime(1992, 9, 25))
    window_end = date + timedelta(window_pad)
    
    xover_ds = stream_window(window_start, window_end, satellite)
    
    polygon_ds = create_polygon(xover_ds, date, satellite)
    out_path = save_ds(polygon_ds, date)
    upload_ds(out_path)
    
    evaluate_correction(polygon_ds)
    