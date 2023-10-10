import xarray as xr
import numpy as np
from datetime import datetime, timedelta
import logging
from daily_files import smoothing
from daily_files.daily_file import GSFC_DailyFile, S6_DailyFile

'''

'''

def s6_processing(ds: xr.Dataset, date: datetime) -> xr.Dataset:

    # s6_dailyfile = S6_DailyFile(ssh, lat, lon, time)

    ds = ds.rename_vars({'ssha': 'ssh'})
    pass


def gsfc_processing(ds: xr.Dataset, date: datetime) -> xr.Dataset:
    '''
    
    '''
    gsfc_daily_file = GSFC_DailyFile(ds, date)
    return gsfc_daily_file.ds