from datetime import datetime
from glob import glob
from typing import Callable, List

import xarray as xr

from processing import gsfc_pass, rads_pass, s6_pass

'''
Temporary file used to mock triggering of processing.py. Something like this will exist external to the repo:
- trigger lambdas for each satellite/date combo

This will be given Satellite, date, source
ex: JASON_3, 2021-10-21, rads

1. Look on S3 for pass files for Jason_3 that contain data from 2021-10-21
2. Open those pass files from S3
3. Loop through the files and process them
4. Concatenate them (or merge) into a daily netCDF file
5. Write that file to S3
'''

def mock_s3_get() -> List[str]:
    paths = glob(
        '/Users/username/Downloads/ssha-dev-data-minimal-backup-2022-07-21/rads/J3/j3p*c210.nc')
    paths.sort()
    return paths

def get_processor(source: str) -> Callable:
    if source == 'rads':
        return rads_pass
    elif source == 's6':
        return s6_pass
    elif source == 'gsfc':
        return gsfc_pass
    else:
        raise Exception(f'{source} not supported source.')

def merge_passes(inputs: List[xr.Dataset]) -> xr.Dataset:
    ds = xr.concat(inputs, 'time')
    ds = ds.sortby('time')
    return ds

def work(satellite: str, date: datetime, source: str):
    '''
    
    '''
    # Mock S3 - need to account for better date grabbing 
    paths = mock_s3_get()
    processor = get_processor(source)

    processed_passes = []
    for path in paths:
        ds = xr.open_dataset(path)
        processed_ds = processor(ds, date, satellite)
        if processed_ds.time.size:
            processed_passes.append(processed_ds)

    daily_ds = merge_passes(processed_passes)
    output = f'{satellite}-alt_ssh{str(date)[:10].replace("-","")}.nc'
    daily_ds.to_netcdf(output)


if __name__ == '__main__':
    date = datetime(2021, 10, 21)
    satellite = 'JASON-3'
    source = 'rads'
    work(satellite, date, source)
