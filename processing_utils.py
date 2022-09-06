from typing import List
import xarray as xr
import numpy as np
import yaml
from datetime import datetime, timedelta

'''
Processing functions that are shared across all pass sources
'''

with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)


def filter_outliers(ds: xr.Dataset, limit: float = 1.5) -> xr.Dataset:
    '''
    Removes values that exceed limit
    '''
    ds = ds.where(np.abs(ds.ssh.values) < limit)
    return ds


def apply_bias(ds: xr.Dataset, sat: str) -> xr.Dataset:
    '''
    
    '''
    ds.ssh.values = ds.ssh + config[sat]['bias']
    return ds


def ssh_smoothing(ssh: np.array, times: np.array) -> np.array:
    '''
    Calculate smoothed ssh values and add to ds
    Can introduce nans to time dimension so we need to drop indices where that is true
    '''
    filter_half = 19//2

    sinc_coeff = np.array([0.222115, 0.196706, 0.134041, 0.0646945, 0.0150775, -0.00675300,
                           -0.00907955, -0.00461792, -0.00110582, -2.06117e-05], 'f')

    TX = [((times[max(0, j-filter_half):j+filter_half+1]-times[j]),
           ssh[max(0, j-filter_half):j+filter_half+1]) for j in range(len(times))]

    ssh_smoothed = []
    for dt, x in TX:
        if len(dt) <= filter_half:
            ssh_smoothed.append(np.nan)
            continue

        good = np.abs(dt) < filter_half + 0.5
        dt = dt[good]  # -9, -8, ..., 0, 1, 2, ... 8, 9
        x = x[good]

        indt = np.rint(dt).astype('i')  # round to nearest integer
        w = sinc_coeff[np.abs(indt)]

        if indt[0] > -filter_half:
            # missing points at left,
            # mirror from right to fill up the missing points
            num = filter_half + indt[0]  # suppose indt[0]=-7, then num=2
            indt_mirror0, = np.where(indt[1:num+1]-indt[0] <= num)
            indt_mirror = indt_mirror0+1  # index to w for the points to be mirrored
            # index to filter window (center index is 0) where the mirrored points to be placed
            wt_mirror_indx = 2*indt[0] - indt[indt_mirror]
            w[indt_mirror] += sinc_coeff[np.abs(wt_mirror_indx)]

        if indt[-1] < filter_half:
            # missing points at left,
            # mirror from right to fill up the missing points
            num = filter_half - indt[-1]  # suppose indt[0]=-7, then num=2
            indt_mirror0, = np.where(indt[-1]-(indt[-num-1:-1]) <= num)
            indt_mirror = indt_mirror0-num-1  # index to w for the points to be mirrored
            # index where the mirrored points to be positioned
            wt_mirror_indx = 2*indt[-1] - indt[-num-1:-1][indt_mirror0]
            w[indt_mirror] += sinc_coeff[np.abs(wt_mirror_indx)]

        ssh_smoothed.append(np.sum(x*w)/np.sum(w))

    return ssh_smoothed

def make_ds(data: dict, times) -> xr.Dataset:
    '''
    Convert dictionary of np arrays into an xarray Dataset object.
    '''
    variables = {k: xr.DataArray(v, dims=['time'])
                for k, v in data.items()}

    ds = xr.Dataset(
        data_vars=variables,
        coords=dict(time=times)
    )
    ds.time.encoding['units'] = 'seconds since 1970-01-01'

    return ds

def date_subset(ds: xr.Dataset, date: datetime) -> xr.Dataset:
    '''
    Drop times outside of date
    '''
    today = str(date)[:10]
    tomorrow = str(date + timedelta(days=1))[:10]
    ds = ds.sel(time=slice(today, tomorrow))
    return ds



