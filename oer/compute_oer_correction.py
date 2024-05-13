# -*- coding: utf-8 -*-
"""
compute_oer_correction.py - Script to read cubic spline coefficients for each
daily file and compute the orbit error reduction term and save it to a new
file.

03 Apr 2024.
"""

import numpy as np
import os, glob
import netCDF4 as nc
import pytz
from datetime import datetime
import xarray as xr
import time
from scipy.interpolate import PPoly

# set directory name for crossovers
sat_string='GSFC'
#sat_string='S6'

# input data directories
pdname='./oer_polygon_files/'+sat_string+'/'
ddname='../'+sat_string+'\\'

# output directories
outpath='./oer_correction_files/'+sat_string+'/'
if not os.path.exists:
    os.makedirs(outpath)
odname='./oer_correction_files/' + sat_string + '/'


# get filenames in polygon directory
pfiles = sorted(glob.glob(os.path.join(pdname, '*/oerpoly*.nc')))
# get filenames in data directory
dfiles = sorted(glob.glob(os.path.join(ddname, '*/*.nc')))

# loop through files and make crossovers for each day
starttime=time.time()
for i in range(len(pfiles)):
    
    # first check to see if there is a corresponding file in dfiles
    dt_str=pfiles[i].split('\\')[-1].split('_')[-1].split('.')[0]
    year_dir=pfiles[i].split('\\')[1]
    dfname=ddname+year_dir + '\\' + sat_string + \
        '-alt_ssh'+dt_str.replace('-','')+'.nc'
    dfind = [j for j, elem in enumerate(dfiles) if dfname in elem]
    if len(dfind)==0:
        print("Cannot Find "+dfname+" in SSH data directory!")
    else:
        
        k=dfind[0]
    
        # open polynomial file
        pnf=nc.Dataset(pfiles[i],'r')
        
        # load coefs and tbreaks from polynomial file
        coef=np.array(pnf['coef'])
        tbrk=np.array(pnf['tbrk'])
        pp=PPoly(coef,tbrk)
        pnf.close()
        
        # open data file
        dnf=nc.Dataset(dfiles[k],'r')
        
        # get reference timestamp for all time values in this file
        if hasattr(dnf['time'],'REFTime'):
            rtime=dnf['time'].REFTime
        else:
            rtime='1990-01-01 00:00:00'
        ref_date=datetime.strptime(rtime,'%Y-%m-%d %H:%M:%S')
        ref_date=ref_date.replace(tzinfo=pytz.UTC)
        rtstamp=datetime.timestamp(ref_date)
        # get timestamp for hour 0 of file's current date
        currentdate=datetime.strptime(dt_str,'%Y-%m-%d')
        currentdate=currentdate.replace(tzinfo=pytz.UTC)
        currenttstamp=datetime.timestamp(currentdate)
        
        # compute hours since start of this day
        ssh_time=np.array(dnf['time'])
        thours=(ssh_time+rtstamp-currenttstamp)/3600
        dnf.close()
    
        # compute orbit error reduction, as additive correction to ssh
        oer=-1*pp(thours)
    
        # save oer into new netcdf file with similar name as input file
        # create file name
        ofpath=outpath + dfiles[k].split('\\')[1] 
        if not os.path.exists(ofpath):
            os.makedirs(ofpath)
        ofname=ofpath + '/oer_correction_' + dt_str +'.nc'
            
        # create xarray data set of variable to save in netcdf file    
        ds= xr.Dataset(
            {
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
            }
        )
    
        # Add global attributes
        ds.attrs["title"] = sat_string + " Orbit Error Reduction, interpolated onto ssh"
        ds.attrs["subtitle"] = "created for " + dfiles[k]
         
        # Save to netCDF file
        ds.to_netcdf(ofname)

    # print progress
    if (i % 100) == 0:
        print(ofname,i,'of',len(dfiles),time.time()-starttime)

