# -*- coding: utf-8 -*-
"""
compute_oer_polygons.py - Script to read daily crossover files and compute oribt error 
reduction polygon as a cubic spline fit. This version loops through
25 Feb 2024.
"""

import numpy as np
import os
from datetime import datetime, timedelta
import pytz
import xarray as xr
from oer.oerfit import oerfit



def create_polygon(xover_ds: xr.Dataset, date: datetime, satellite: str):
    '''
    Function to create single polygon
    Needs to pull xovers from bucket first
    '''    
    pgon_def_duration = 86400  # define polygon over 1 day
    pgon_t_margin = 7200  # keep 2 hours of extra data to avoid jumps between days
    ssh_max_error = 0.3  # ignore absolute xover differences larger than this in meters
    
    ref_timestamp = datetime.timestamp(datetime(1990,1,1).replace(tzinfo=pytz.UTC))

    cycle1 = xover_ds.cycle1.values
    cycle2 = xover_ds.cycle2.values
    pass1 = xover_ds.pass1.values
    pass2 = xover_ds.pass2.values
    psec1 = np.float64(xover_ds.time1.values + ref_timestamp)
    psec2 = np.float64(xover_ds.time2.values + ref_timestamp)
    ssh1 = xover_ds.ssh1.values
    ssh2 = xover_ds.ssh2.values
    xcds = np.array([xover_ds['lon'].values, xover_ds['lat'].values]).T

    # keep date of "current" file
    cur_timestamp = datetime.timestamp(date.replace(tzinfo=pytz.UTC))

    # compute trackid from passnum & cyc
    trackid1 = cycle1*10000+pass1
    trackid2 = cycle2*10000+pass2

    # since these are self crossovers, stack pass1 & pass2 data
    dssh0 = ssh1-ssh2
    dssh = np.concatenate((dssh0, -dssh0))
    psec = np.concatenate((psec1, psec2))
    trackid = np.concatenate((trackid1, trackid2))

    # need to make a time variable in units of hours, refernced
    # to current date at time 00:00:00.  Polygon will be expressed
    # in terms of this variable
    phours = (psec-cur_timestamp)/3600

    # pick times for spline fit: first find all passes with data
    # within 2 hour window before & after the current day.
    iilimit = np.where((psec >= cur_timestamp-pgon_t_margin) &
                       (psec < cur_timestamp+pgon_def_duration+pgon_t_margin))[0]

    # case of no data in this day, make a polynomial that's all zeros
    if len(iilimit) == 0:
        tbrk = np.array(range(-3, 28))
        coef = np.zeros((4, len(tbrk)-1))
        rms_sig = np.zeros(len(tbrk)-1)
        rms_res = np.zeros(len(tbrk)-1)
        nint = np.zeros(len(tbrk)-1)
        print(f'no data in {date}')
    else:

        # make list of passes in this time window & find min/max times for them
        cplist, iitrack = np.unique(trackid[iilimit], return_index=True)
        mintime = min(phours[np.where(trackid == np.min(cplist))])
        maxtime = max(phours[np.where(trackid == np.max(cplist))])

        # find list of data in this time window & abs(dssh) < ssh_max_error
        iitoday = np.where((phours >= mintime) & (phours <= maxtime) &
                           (np.abs(dssh) < ssh_max_error))[0]

        # sort index by time
        ii = np.argsort(phours[iitoday])
        iitoday = iitoday[ii]

        # padd with zeros to make sure polygon doesn't blow up away from data
        hpmin = np.arange(-5, mintime, .3)
        hpmax = np.arange(maxtime, 29, .3)
        phours_pad = np.concatenate((hpmin, phours[iitoday], hpmax))
        dssh_pad = np.concatenate((hpmin*0, dssh[iitoday], hpmax*0))
        trackid_pad = np.concatenate((hpmin*0+trackid[iitoday[0]],
                                      trackid[iitoday],
                                      hpmax*0+trackid[iitoday[-1]]))
        # send to our own spline function for fit
        coef, tbrk, rms_sig, rms_res, nint = oerfit(phours_pad, dssh_pad, trackid_pad)

    # save polygon information into parallel daily file

    # create xarray data set of variable to save in netcdf file
    ds = xr.Dataset(
        {
            "coef": (["N_order", "N_intervals"], coef, {
                "units": "meters/hour^3, meters/hour^2, meters/hour, meters",
                "long_name": "coefficients for cubic spine polynomials"
            }),
            "tbrk": (["N_breaks"], tbrk, {
                "units": "Hours since " + datetime.strftime(date.replace(tzinfo=pytz.UTC), '%Y-%m-%d %H:%M:%S %Z'),
                "long_name": "Breaks in cubic spline"
            }),
            "rms_sig": (["N_intervals"], rms_sig, {
                "units": "m",
                "long_name": "RMS of crossover difference pairs in each interval"
            }),
            "rms_res": (["N_intervals"], rms_res, {
                "units": "m",
                "long_name": "RMS of residuals after cubic spline is removed"
            }),
            "nint": (["N_intervals"], nint, {
                "units": "counts",
                "long_name": "Number of crossovers in each interval"
            })
        }
    )

    # Add global attributes
    ds.attrs["title"] = f"{satellite} Spline coefficents for Orbit Error Reduction"
    ds.attrs["subtitle"] = f"created for {satellite} {date}"

    return ds

