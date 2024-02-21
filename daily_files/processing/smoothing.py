import xarray as xr
import pandas as pd
import numpy as np
import time
import logging


'''
Smoothing functions that are shared across all data sources.
'''

SINC_COEFF = np.array([-2.06117e-05, -0.00110582, -0.00461792, -0.00907955, -0.00675300, 
                        0.0150775, 0.0646945, 0.134041, 0.196706, 
                        0.222115, 
                        0.196706, 0.134041, 0.0646945, 0.0150775,
                        -0.00675300, -0.00907955, -0.00461792, -0.00110582, -2.06117e-05, 
                        ])

def mirror_nans(arr: np.ndarray) -> np.ndarray:
    '''
    Mirror nans in array, if an index is nan, it's mirrored index should also be nan
    '''
    nan_indices = np.isnan(arr)
    arr[nan_indices[::-1]] = np.nan
    return arr

def nan_check(ssh_vals: np.ndarray) -> bool:
    '''
    Check if 19 point window is all nans, or
    if the 6 points around the center are all nans
    '''
    nan_arr = np.isnan(ssh_vals)
    return np.all(nan_arr) or (np.all(nan_arr[6:9]) & np.all(nan_arr[10:13]))

def smooth_point(ssh_vals: np.ndarray) -> np.float32:
    numerator = np.nansum(ssh_vals * SINC_COEFF)
    denominator = np.nansum(SINC_COEFF[~np.isnan(ssh_vals)])
    return numerator/denominator

def pad_df(ds: xr.Dataset) -> pd.DataFrame:
    '''
    Pads an extra +/- 9 seconds to front and end of data frame to ensure sufficent
    data points to smooth
    '''
    df = pd.DataFrame({'ssh': ds.ssh.values, 'flag': ds.nasa_flag.values}, ds.time.values)    
    padded_df = df.reindex(np.arange(ds.time.values[0] - np.timedelta64(9, 's'), ds.time.values[-1] + np.timedelta64(10, 's'), dtype='datetime64[s]'))
    return padded_df

def make_windows(df: pd.DataFrame):
    '''
    Uses pandas rolling function to create windows
    CAUTION! Since we expect the original nans to be carred through (as we need them as part of our smoothing algorithm)
    we need to ensure that nans have been temporarily filled with a fill value prior to this function's execution.
    '''
    return df.rolling(19, center=True)

def smooth(sdf: np.ndarray):
    '''
    Convert fill value back to nan, mirror nans, compute smoothed value
    '''
    sdf = np.where(sdf==9999, np.nan, sdf)
    ssh_vals = mirror_nans(sdf)
    if nan_check(ssh_vals):
        smoothed_val = np.nan
    else:
        smoothed_val = smooth_point(ssh_vals)
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
        
    # Convert to pandas and reindex based on +/- 9 second padding time list. Fills with NaNs at new index locations (times)
    padded_df = pad_df(ds)

    # Apply nasa_flag to ssh
    padded_df.ssh = np.where(padded_df.flag.values == 0, padded_df.ssh, np.nan)
       
    # Use Pandas rolling to shift through 19 point windows, applying smooth function to each
    # Fill nans in order to get rolling windows to function properly since we handle nans on our own
    windows = make_windows(padded_df.ssh.fillna(9999))
    ssh_smoothed = windows.aggregate(smooth)
    
    # Reindex selecting only points at original data
    ssh_smoothed = ssh_smoothed[pd.DatetimeIndex(ds.time.values)]
    ds['ssh_smoothed'] = (('time'), ssh_smoothed)
    
    return ds