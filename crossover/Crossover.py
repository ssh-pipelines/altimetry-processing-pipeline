import logging
import os
import re
from typing import Iterable
import numpy as np
import xarray as xr

from crossover.utils.aws_utils import aws_manager

class Options():
    '''
    Class for containing global options used to compute crossovers for a single date
    '''
    def __init__(self, source1: str, source2: str) -> None:
        self.max_crossovers_per_day: int = 100 * 25
        self.self_crossovers: bool = source1 == source2
        self.all_sat_names: str = ', '.join(list(set([source1, source2])))
        self.epoch: np.datetime64 = np.datetime64('1990-01-01T00:00:00.000000')
        self.window_size: int = 10
        self.window_padding: int = 2
        self.cycle_length: float = 9.9156
        self.zero_diff: np.timedelta64 = np.timedelta64(0, 'ns')
        self.max_diff: np.timedelta64 = np.timedelta64(int(self.cycle_length * 86400000000000), 'ns')


class SourceWindow():
    
    def __init__(self, df_version: str, source: str, day: np.datetime64, window_start: np.datetime64, window_end: np.datetime64) -> None:
        self.df_version: str = df_version
        self.shortname: str = source
        self.day: np.datetime64 = day
        self.window_start: np.datetime64 = window_start
        self.window_end: np.datetime64 = window_end
        
    def set_filepaths(self):
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
    
    def set_file_dates(self):
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
        
        self.time = window_ds['time'].values
        self.lon = window_ds['longitude'].values.astype('float64')
        self.lat = window_ds['latitude'].values.astype('float64')
        self.ssh = window_ds['ssh_smoothed'].values.astype('float64')
        self.trackid = window_ds['cycle'].values.astype('int32')*10000 + window_ds['pass'].values

        self.input_filenames = ', '.join([os.path.basename(self.filepaths[dayind]) for dayind in window_indexes])
        self.input_histories = window_ds.attrs['history'].split('Created on ')[1]
        self.input_product_generation_steps = window_ds.attrs['product_generation_step']

        #Find unique trackids, cycles and passes in the first satellite's data in this window.
        self.unique_trackid = np.unique(self.trackid)
        
        starts = []
        self.track_masks = {}
        for this_unique_trackid in self.unique_trackid:
            starts.append(np.min(self.time[self.trackid == this_unique_trackid]))
            self.track_masks[this_unique_trackid] = np.where(self.trackid == this_unique_trackid)[0]
        
        self.trackid_start_times = np.array(starts, dtype='datetime64[ns]')


def date_from_fp(fp: str) -> np.datetime64:
    date_pattern = re.compile(r'\d{8}')
    filename = os.path.basename(fp)
    match = date_pattern.search(filename)
    date_str = match.group()
    try:
        date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        date_obj = np.datetime64(date_str)
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