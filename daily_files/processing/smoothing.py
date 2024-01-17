import xarray as xr
import pandas as pd
import numpy as np
import time
import logging


'''
Smoothing functions that are shared across all data sources
'''

SINC_COEFF = np.array([-2.06117e-05, -0.00110582, -0.00461792, -0.00907955, -0.00675300, 
                        0.0150775, 0.0646945, 0.134041, 0.196706, 
                        0.222115, 
                        0.196706, 0.134041, 0.0646945, 0.0150775,
                        -0.00675300, -0.00907955, -0.00461792, -0.00110582, -2.06117e-05775, 
                        ])

def mirror_nans(arr: np.ndarray) -> np.ndarray:
    nan_indices = np.isnan(arr)
    arr[nan_indices[::-1]] = np.nan
    return arr

def nan_check(ssh_vals: np.ndarray) -> bool:
    '''
    Check if 19 point window is all nans, or
    if the 5 points around the center are all nans
    '''
    nan_arr = np.isnan(ssh_vals)
    if np.all(nan_arr) or np.all(nan_arr[6:13]):
        return True
    return False

def smooth_point(ssh_vals: np.ndarray) -> np.float32:
    numerator = np.nansum(ssh_vals * SINC_COEFF)
    denominator = np.nansum(SINC_COEFF[~np.isnan(ssh_vals)])
    smoothed_val = numerator/denominator
    return smoothed_val


def ssh_smoothing(ds: xr.Dataset) -> xr.Dataset:
    '''
    Calculate smoothed ssh values and add to ds.

    We use the pandas reindex feature to automatically populate missing
    index values with NaNs. This works because we assume "time" has been 
    indexed to each second by this point.
    
    We keep the data to be smoothed in a pandas dataframe to make use of
    fast time-based indexing (necessary because GSFC data does not contain all timesteps)
    '''
    logging.info('Beginning smoothing...')
    start = time.time()
        
    # Convert to pandas and reindex based on +/- 9 second padding time list. Fills with NaNs at new index locations
    df = pd.DataFrame({'ssh': ds.ssh.values, 'flag': ds.nasa_flag.values}, ds.time.values)    
    padded_df = df.reindex(np.arange(ds.time.values[0] - np.timedelta64(9, 's'), ds.time.values[-1] + np.timedelta64(10, 's'), dtype='datetime64[s]'))
    
    # Apply nasa_flag to ssh
    padded_df.ssh = np.where(padded_df.flag.values == 0, padded_df.ssh, np.nan)
   
    # Make 19 point time windows centered on times in original data
    time_windows = [(t - np.timedelta64(9, 's'), t + np.timedelta64(9, 's')) for t in ds.time.values]
    
    # Iterate through original time windows, slicing the reindexed (padded) pandas version of data to get filled in 19 point window
    # Compute smoothed value for each original time step
    ssh_smoothed = []
    for begin, end in time_windows:
        ssh_vals = padded_df[begin:end].ssh.values.copy()
        
        # Set values in window to nan where basin is not visible
        '''
        To be implemented
        '''
            
        # Mirror nans in window
        ssh_vals = mirror_nans(ssh_vals)
    
        # Check if we have correct non-nan values to compute smooth value
        if nan_check(ssh_vals):
            smoothed_val = np.nan
        else:
            smoothed_val = smooth_point(ssh_vals)            
        ssh_smoothed.append(smoothed_val)
        
    # Add smoothed values array to ds object
    ds['ssh_smoothed'] = (('time'), ssh_smoothed)
    logging.debug(f'Smoothing took {time.time() - start} seconds')

    return ds