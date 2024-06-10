import logging
import os
import re
from typing import Iterable
import numpy as np
import xarray as xr

from crossover.utils.aws_utils import aws_manager

class SourceWindow():
    
    def __init__(self, df_version: str, source: str, day: np.datetime64, window_start: np.datetime64, window_end: np.datetime64) -> None:
        self.df_version: str = df_version
        self.shortname: str = source
        self.day: np.datetime64 = day
        self.window_start: np.datetime64 = window_start
        self.window_end: np.datetime64 = window_end
        
    def set_filepaths_and_dates(self):
        start_year = str(self.window_start.astype('datetime64[Y]')).split('-')[0]
        end_year = str(self.window_end.astype('datetime64[Y]')).split('-')[0]
        unique_years = list(set([start_year, end_year]))
        all_keys = []
        for year in unique_years:    
            prefix = os.path.join('daily_files', self.df_version, self.shortname, year)
            s3_keys = aws_manager.get_filepaths(prefix)
            all_keys.extend(s3_keys)
            
        filtered_keys = list(filter(lambda x: date_from_fp(x)>= self.window_start and 
                                              date_from_fp(x) <= self.window_end,
                                              all_keys))
        self.filepaths = filtered_keys
        self.file_dates: Iterable[np.datetime64] = [date_from_fp(fp) for fp in self.filepaths]
        
    def stream_files(self):
        self.streams = [aws_manager.stream_s3(key) for key in self.filepaths]       

    def init_and_fill_running_window(self):
        '''
        Initialize and fill a running window with satellite data loaded from disk.
        '''
        # Find indices for first_day_in_window and last_day_in_window
        first_day_in_window_index = find_closest_date_index(self.window_start, self.file_dates)
        last_day_in_window_index  = find_closest_date_index(self.window_end,  self.file_dates)
        
        window_indexes = range(first_day_in_window_index, last_day_in_window_index + 1)

        window_ds = xr.open_mfdataset([self.streams[dayind] for dayind in window_indexes], engine='h5netcdf', 
                                      concat_dim='time', chunks={}, parallel=True, combine='nested')
        window_ds = window_ds.dropna('time', subset=['ssh_smoothed'])

        logging.info('Opening files complete')
        
        self.time: np.ndarray = window_ds['time'].values
        self.lon: np.ndarray = window_ds['longitude'].values.astype('float64')
        self.lat: np.ndarray = window_ds['latitude'].values.astype('float64')
        self.ssh: np.ndarray = window_ds['ssh_smoothed'].values.astype('float64')
        self.trackid: np.ndarray = window_ds['cycle'].values.astype('int32')*10000 + window_ds['pass'].values

        self.input_filenames: str = ', '.join([os.path.basename(self.filepaths[dayind]) for dayind in window_indexes])
        self.input_histories: str = window_ds.attrs['history'].split('Created on ')[1]
        self.input_product_generation_steps: str = window_ds.attrs['product_generation_step']

        #Find unique trackids, cycles and passes in the first satellite's data in this window.
        self.unique_trackid = np.unique(self.trackid)
        
        starts = []
        self.track_masks = {}
        for track_id_i in self.unique_trackid:
            starts.append(np.min(self.time[self.trackid == track_id_i]))
            self.track_masks[track_id_i] = np.where(self.trackid == track_id_i)[0]
        
        self.trackid_start_times = np.array(starts, dtype='datetime64[ns]')

DATE_PATTERN = re.compile(r'\d{8}')

def date_from_fp(fp: str) -> np.datetime64:
    filename = os.path.basename(fp)
    match = DATE_PATTERN.search(filename)
    
    if match is None:
        raise ValueError(f"No date found in filename: {filename}")
    
    date_str = match.group()
    try:
        date_obj = np.datetime64(f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}")
    except:
        raise RuntimeError(f'Unable to extract date from filename: {filename = }')    
    return date_obj

def find_closest_date_index(date: np.datetime64, file_dates: Iterable[np.datetime64]) -> int:
    '''
    Find the index of the closest date to the given date in the file_dates list.
    '''
    # Convert file_dates to a numpy array of datetime64 if it's not already
    file_dates_np = np.array(file_dates)
    
    if file_dates_np.size == 0:
        raise RuntimeError("Error: The file_dates list is empty.")
    
    differences = np.abs(file_dates_np - date)
    closest_index = np.argmin(differences)    
    return int(closest_index)