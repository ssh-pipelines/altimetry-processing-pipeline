import xarray as xr
import numpy as np
from datetime import datetime, timedelta
import logging
from daily_files import smoothing
from daily_files.daily_file import GSFC_DailyFile, S6_DailyFile

'''

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
        logging.exception("Pass is missing time_rel_eq values")
        raise Exception("No time dimension")

    data = {}
    ds = ds.rename_vars({'sla': 'ssh'})

    ds = smoothing.filter_outliers(ds)
    ds = smoothing.apply_bias(ds, sat)
    data['ssh'] = ds.ssh.values

    # Make seaice_conc if it doesn't exist
    if 'seaice_conc' in ds:
        data['seaice_conc'] = ds.seaice_conc.values
    else:
        logging.info(f'Missing seaice_conc flag. Adding zeros array')
        data['seaice_conc'] = np.zeros(data['ssh'].shape, 'i')

    # Make qual_alt_rain_ice if it doesn't exist
    if 'qual_alt_rain_ice' in ds:
        data['qual_alt_rain_ice'] = ds.qual_alt_rain_ice.values
    else:
        logging.info(f'Missing qual_alt_rain_ice flag. Adding zeros array')
        data['qual_alt_rain_ice'] = np.zeros(data['ssh'].shape, 'i')

    # Wrap lons
    lons = ds.lon.values
    lons[lons < 0.] += 360.
    data['lon'] = lons
    data['lat'] = ds.lat.values

    # Get latencies
    if "latency" in ds:
        data['latency'] = ds.latency.values
    else:
        logging.info(f'Missing latency flag. Adding zeros array')
        data['latency'] = np.zeros(data['ssh'].shape, 'i')

    # If this file uses Shailen's orbit, set latencies to 12.
    if "alt" in ds and "JPL orbital altitude" in ds.alt.attrs['long_name']:
        logging.info(f'Modifying latency array to account for Shailens orbit')
        data['latency'] = np.full_like(ds.latency.values, 12)


    try:
        eq_time = ds.attrs["equator_time"]
        eq_time_dt = datetime.strptime(eq_time, "%Y-%m-%d %H:%M:%S.%f")
        times = [timedelta(microseconds=t*1000000) + eq_time_dt for t in ds.time_rel_eq.values]
    except Exception as e:
        logging.error(f'Error while computing time values. {e}')
        print(e)
    times = [np.datetime64(str(t)[:19]) for t in times]

    new_ds = smoothing.make_ds(data, times)
    ssh_smoothed = smoothing.ssh_smoothing(new_ds)
    new_ds['ssh_smoothed'] = (('time'), ssh_smoothed)
        
    new_ds = smoothing.date_subset(new_ds, date)
    new_ds = new_ds.sortby('time')

    # Set attrs
    track = ds.attrs["cycle_number"] * 1000 + ds.attrs["pass_number"]
    ds.attrs['track_id'] = track

    ds.attrs['start_time'] = ds.time.values[0]
    ds.attrs['end_time'] = ds.time.values[-1]
    ds.attrs['sat_id'] = ds.attrs["mission_name"]
    return new_ds


def s6_pass(ds: xr.Dataset, date: datetime, sat: str) -> xr.Dataset:

    # s6_dailyfile = S6_DailyFile(ssh, lat, lon, time)

    ds = ds.rename_vars({'ssha': 'ssh'})
    pass


def gsfc_pass(ds: xr.Dataset, date: datetime, sat: str) -> xr.Dataset:
    '''
    
    '''
    gsfc_daily_file = GSFC_DailyFile(ds, date)
    return gsfc_daily_file.ds