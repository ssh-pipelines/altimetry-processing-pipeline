import xarray as xr
import numpy as np
from datetime import datetime, timedelta

import processing_utils

'''
Questions...
- Which of the rads_pass code is specific to rads files and what is general to all daily file creation
- Kriging3d currently expects h5 files - will need to update to expect .nc

1. rads_dates.py:
Extract from file: TIME, LATS, LONS, SSH, SEA_ICE, FLAG1, FLAG2, Satellite, Phase, TRACK_ID, ColTitle, latencies, bad_tracks
Modifications: TIME+= equator crossing time, SEA_ICE = seaice_conc or zeros np array, TRACK_ID = np array of track_id

2. rads_dates2.py:
Works on sats2 = ['TOPEX','JASON-1','JASON-2','JASON-3'] first, then sats1 = ['ERS-1','ERS-2','ENVISAT1','CRYOSAT2','SARAL','SNTNL-3A','SNTNL-3B']:
    getAllPass() -> grabs all .jsons that were saved in rads_dates.py
    data_sort_dump() -> loops through all time steps:
        - removes large absolute values
        - applies ssh bias
        - creates smoothed ssh
        - subsets data that falls on day
        - builds up complete "thisday" and "nextday" dictionaries - why both today and tomorrow?
        - makes h5 file 'alt_ssh{0:d}{1:02d}{2:02d}.h5'

What we want changed:
- Remove all intermediary save to disks
- End result is netcdf
- Define some attributes (current daily files have none)
- sat_id as attr, not array
'''


def rads_pass(ds: xr.Dataset, date: datetime, sat: str) -> xr.Dataset:
    '''
    Given a single rads pass file, we want to apply the following corrections:
    - set times using equator crossing time
    - build or apply sea ice flag and qual_alt_rain_ice flags
    - wrap lons to 0 - 360 (possibly necessary downstream)
    - define bad tracks
    - define latencies
    - remove large absolute values from SSH
    - apply ssh bias
    - create smoothed ssh var
    - subset data (remove data from different day)

    ds: the opened rads pass file
    date: the date for which we want a daily file

    returns:
    ds: the processed pass
    '''

    if ds.time_rel_eq.values is None:
        raise Exception("No time dimension")

    data = {}
    ds = ds.rename_vars({'sla': 'ssh'})

    ds = processing_utils.filter_outliers(ds)
    ds = processing_utils.apply_bias(ds, sat)
    data['ssh'] = ds.ssh.values

    ssh_smoothed = processing_utils.ssh_smoothing(data['ssh'], ds.time.values)
    data['ssh_smoothed'] = ssh_smoothed

    try:
        eq_time = datetime.strptime(
            ds.attrs["equator_time"], "%Y-%m-%d %H:%M:%S.%f")
        times = [(timedelta(seconds=t) + eq_time).timestamp()
                 for t in ds.time_rel_eq.values]
    except Exception as e:
        print(e)
        exit()

    data['times'] = times

    # Make seaice_conc if it doesn't exist
    if 'seaice_conc' in ds:
        data['seaice_conc'] = ds.seaice_conc.values
    else:
        data['seaice_conc'] = np.zeros(data['ssh'].shape, 'i')

    # Make qual_alt_rain_ice if it doesn't exist
    if 'qual_alt_rain_ice' in ds:
        data['qual_alt_rain_ice'] = ds.qual_alt_rain_ice.values
    else:
        data['qual_alt_rain_ice'] = np.zeros(data['ssh'].shape, 'i')

    # Update seaice_conc based on qual_alt_rain_ice flag
    # qual_alt_rain_ice sets seaice_conc to binary 0% or 100%
    data['seaice_conc'] = np.where(data['qual_alt_rain_ice'] != 0, 100., data['qual_alt_rain_ice'])


    # Wrap lons
    lons = ds.lon.values
    lons[lons < 0.] += 360.
    data['lon'] = lons
    data['lat'] = ds.lat.values

    # Get latencies
    if "latency" in ds:
        data['latency'] = ds.latency.values
    else:
        data['latency'] = np.zeros(data['ssh'].shape, 'i')

    # If this file uses Shailen's orbit, set latencies to 12.
    if "alt" in ds and "JPL orbital altitude" in ds.alt.attrs['long_name']:
        data['latency'] = np.full_like(ds.latency.values, 12)

    # Drop nans
    nan_indeces = np.isnan(ssh_smoothed)

    for k in data.keys():
        data[k] = np.array(data[k])[~nan_indeces]

    ds = processing_utils.make_ds(data)

    #TODO: Julie and Kevin made it here

    ds = processing_utils.date_subset(ds, date)

    ds = ds.sortby('time')

    # Set attrs
    ds.attrs['track_id'] = ds.attrs["cycle_number"] * \
        1000 + ds.attrs["pass_number"]

    ds.attrs['start_time'] = ds.time.values[0]
    ds.attrs['end_time'] = ds.time.values[-1]
    ds.attrs['sat_id'] = ds.attrs["mission_name"]
    # ds.to_netcdf('test.nc')
    return ds


def s6_pass(ds: xr.Dataset, date: datetime) -> xr.Dataset:
    ds = ds.rename_vars({'ssha': 'ssh'})
    pass


def gsfc_pass(ds: xr.Dataset, date: datetime) -> xr.Dataset:
    pass