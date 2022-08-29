from datetime import datetime
import xarray as xr
import numpy as np

from glob import glob

from processing import rads_pass, s6_pass

'''
Temporary file used to mock triggering of processing.py. Something like this will exist external to the repo:
- trigger lambdas for each satellite/date combo

This will be given Satellite, date
ex: JASON_3, 2021-10-30

1. Look on S3 for pass files for Jason_3 that contain data from 2021-10-30
2. Open those pass files from S3
3. Loop through the files and process them
4. Concatenate them (or merge) into a daily netCDF file
5. Write that file to S3
'''

paths = glob(
    '/Users/username/Downloads/ssha-dev-data-minimal-backup-2022-07-21/rads/J3/j3p*c210.nc')
paths.sort()
passes = [xr.open_dataset(path) for path in paths]
jason_3_ds = passes[0]

updated_pass = rads_pass(jason_3_ds, datetime(2021, 10, 30), 'JASON-3')
