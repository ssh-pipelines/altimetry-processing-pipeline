# -*- coding: utf-8 -*-
"""
compute_oer_correction.py - Script to read cubic spline coefficients for each
daily file and compute the orbit error reduction term and save it to a new
file.

03 Apr 2024.
"""

import numpy as np
import os
import pytz
from datetime import datetime
import xarray as xr
from scipy.interpolate import PPoly

   
def evaluate_correction(polygon_ds: xr.Dataset, daily_file_ds: xr.Dataset, date: datetime, satellite: str): 
       
    # load coefs and tbreaks from polynomial file
    coef = polygon_ds.coef.values
    tbrk = polygon_ds.tbrk.values
    pp = PPoly(coef,tbrk)
    
    
    # get timestamp for hour 0 of file's current date
    ref_timestamp = datetime.timestamp(datetime(1990,1,1).replace(tzinfo=pytz.UTC))
    currenttstamp=datetime.timestamp(date.replace(tzinfo=pytz.UTC))
    
    # compute hours since start of this day
    ssh_time = daily_file_ds.time.values
    thours = (ssh_time + ref_timestamp - currenttstamp)/3600

    # compute orbit error reduction, as additive correction to ssh
    oer = -1 * pp(thours)
        
    # create xarray data set of variable to save in netcdf file    
    ds= xr.Dataset(
        data_vars = {
            "ssh_time": (["time"], ssh_time, {
                "units": "seconds since 1990-01-01",
                "long_name": "time",
                "standard_name": "time",
                "REFTime": "1990-01-01 00:00 00",
                "REFTime_comment":  "This string contains a time in the "+\
                    "format yyyy-mm-dd HH:MM:SS to which all times in the "+\
                        "time variable are referenced.",
                        "calendar": "proleptic_gregorian"
            }),
            "oer": (["time"], oer, {
                "units": "meters",
                "long_name": "Orbit error reduction",
                "comment": "add this variable to ssh and ssh_smoothed to" +\
                    "reduce orbit error"
            })
        },
        attrs = {
            "title": f'{satellite} Orbit Error Reduction, interpolated onto ssh',
            "subtitle": f'created for {daily_file_filename}'
        }
    )   
    return ds

