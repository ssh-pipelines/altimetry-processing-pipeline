import xarray as xr
import pandas as pd
import numpy as np
import time
import logging


'''
Smoothing functions that are shared across all data sources
'''

def mirror_nans(ssh_vals: np.ndarray) -> np.ndarray:
    '''
    An array of odd length will mirror nans
    '''
    logging.debug('Mirroring values in window')
    nan_indices = np.where(np.isnan(ssh_vals))[0]
    ssh_vals[-nan_indices - 1] = np.nan
    return ssh_vals

def smooth_point(ssh_vals: np.ndarray, flag: np.ndarray) -> np.float32:
    
    sinc_coeff = np.array([-2.06117e-05, -0.00110582, -0.00461792, -0.00907955, -0.00675300, 
                           0.0150775, 0.0646945, 0.134041, 0.196706, 
                           0.222115, 
                           0.196706, 0.134041, 0.0646945, 0.0150775,
                           -0.00675300, -0.00907955, -0.00461792, -0.00110582, -2.06117e-05775, 
                           ])
    
    ssh_vals = np.where(flag == 0, ssh_vals, np.nan)

    # Set values in slice to nan where basin is not visible
    '''
    To be implemented
    '''
    
    # Determine nans in window and mirror them
    ssh_vals = mirror_nans(ssh_vals)
    
    if all([np.isnan(ssh_vals[i]) for i in range(6,13)]):
        return np.nan

    # Set smoothed point to NaN if all vals are NaN
    if np.sum(np.isnan(ssh_vals)) == 18:
        return np.nan
    
    numerator = np.nansum(ssh_vals * sinc_coeff)
    denominator = np.nansum(sinc_coeff[~np.isnan(ssh_vals)])
    smoothed_val = numerator/denominator
    return smoothed_val


def ssh_smoothing(ds: xr.Dataset) -> xr.Dataset:
    '''
    Calculate smoothed ssh values and add to ds.

    We use xarray's reindex feature to automatically populate missing
    index values with NaNs. This works because we assume "time" has been 
    indexed to each second by this point. 
    '''
    logging.info('Beginning smoothing...')
    start = time.time()
    
    ssh_smoothed = []
    times = np.arange(ds.time.values[0] - np.timedelta64(9, 's'), ds.time.values[-1] + np.timedelta64(10, 's'), dtype='datetime64[s]')
    df = pd.DataFrame({'ssh': ds.ssh.values, 'nasa_flag': ds.nasa_flag.values}, ds.time.values)

    padded_df = df.reindex(times)

    for cur_time in ds.time.values:

        window_start = cur_time - np.timedelta64(9, 's')
        window_end = cur_time + np.timedelta64(9, 's')
        df_slice = padded_df[window_start: window_end]
                
        smoothed_val = smooth_point(df_slice.ssh.values.copy(), df_slice.nasa_flag.values.copy())
        ssh_smoothed.append(smoothed_val)

    ds['ssh_smoothed'] = (('time'), ssh_smoothed)
    logging.debug(f'Smoothing took {time.time() - start} seconds')
    return ds