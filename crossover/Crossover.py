import logging
import os
import re
from typing import Union
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
    
    def __init__(self, source: str, day: np.datetime64, window_start: np.datetime64, window_end: np.datetime64) -> None:
        self.shortname = source
        self.day = day
        self.window_start = window_start
        self.window_end = window_end
        
    def set_filepaths(self):
        # s3://example-bucket/daily_files/GSFC/{year}/filessssss.nc
        start_year = str(self.window_start.astype('datetime64[Y]')).split('-')[0]
        end_year = str(self.window_end.astype('datetime64[Y]')).split('-')[0]
        years_set = list(set([start_year, end_year]))
        all_keys = []
        for year in years_set:    
            prefix = os.path.join('daily_files', self.shortname, year)
            s3_keys = aws_manager.get_filepaths(prefix)
            all_keys.extend(s3_keys)
            
        filtered_keys = list(filter(lambda x: date_from_fp(x['Key'])>= self.window_start and date_from_fp(x['Key']) <= self.window_end, all_keys))
        self.filepaths = filtered_keys
    
    def set_file_dates(self):
        self.file_dates = [date_from_fp(fp) for fp in self.filepaths]
        
    def stream_files(self):
        self.streams = [aws_manager.stream_s3(key) for key in self.filepaths]       
    
    def init_and_fill_running_window(self):
        '''
        Initialize and fill a running window with satellite data loaded from disk.
        '''
        logging.info('Initializing and filling data...')
        # Find indices for first_day_in_window and last_day_in_window
        first_day_in_window_index = find_closest_date_index(self.window_start, self.file_dates)
        last_day_in_window_index  = find_closest_date_index(self.window_end,  self.file_dates)

        #Fill the running window with data from disk:
        sat_these_days = [] #Empty list to store the opened files.
        these_filenames = [] #Empty list to store the filenames.
        these_histories = [] #Empty list to store the history attributes.
        these_product_generation_steps = [] #Empty list to store the product_generation_step attributes.
        found_files = 0
        found_points = 0
        
        for dayind in range(first_day_in_window_index, last_day_in_window_index + 1):
            sat_thisday = xr.open_dataset(self.streams[dayind], engine='h5netcdf', mask_and_scale=False)
            if sat_thisday.dims['time'] == 0: 
                continue #Skip this file if it has no data.
            else: 
                found_points += sat_thisday.dims['time']
            found_files += 1
            
            sat_these_days.append(sat_thisday)
            these_filenames.append(os.path.basename(self.filepaths[dayind]))
            these_histories.append(sat_thisday.attrs['history'].split('Created on ')[1])
            these_product_generation_steps.append(sat_thisday.attrs['product_generation_step'])
        
            sat_thisday.close()
            
        if found_files == 0:
            logging.info(f"No files found for {self.shortname} from {self.window_start} to {self.window_end}.")
            return
        if found_points == 0:
            logging.info(f"No data found for {self.shortname} from {self.window_start} to {self.window_end}.")
            return
        ds = xr.concat(sat_these_days, dim='time')
        logging.info("Loading and merging data...done")
        
        self.input_filenames = ', '.join(these_filenames)
        self.input_histories = ', '.join(these_histories)
        self.input_product_generation_steps = ', '.join(these_product_generation_steps)
        
        #Add this day's data (after filtering, standardizing key names, etc.) to the running window.
        #Filter out default values:
        ssh_fill_value = ds['ssh_smoothed'].attrs['_FillValue']
        flag_filter = np.ones(ds['ssh_smoothed'].shape, dtype=bool)
        
        good_indices = np.where((ds['ssh_smoothed'].values != ssh_fill_value) & (flag_filter))[0]
        self.time = ds['time'][good_indices].values
        self.lon = ds['longitude'][good_indices].values.astype('float64')
        self.lat = ds['latitude'][good_indices].values.astype('float64')
        self.ssh = ds['ssh_smoothed'][good_indices].values.astype('float64')
        self.trackid = ds['cycle'][good_indices].values.astype('int32')*10000 + ds['pass'][good_indices].values

        #Find unique trackids, cycles and passes in the first satellite's data in this window.
        self.unique_trackid = np.unique(self.trackid)
        trackid_start_times_list = [np.min(self.time[self.trackid == this_unique_trackid]) for this_unique_trackid in self.unique_trackid]
        self.trackid_start_times = np.array(trackid_start_times_list, dtype='datetime64[ns]')

        #Save the track_masks (indices) for each unique trackid.
        #Each unique trackid is a key in the track_masks dictionary, and the value is an array of indices.
        self.track_masks = {this_unique_trackid: np.where(self.trackid == this_unique_trackid)[0] for this_unique_trackid in self.unique_trackid}

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

def find_closest_date_index(date: np.datetime64, file_dates: list) -> Union[int, None]:
    '''
    Find the index of the closest date to the given date in the file_dates list.
    '''
    # Convert file_dates to a numpy array of datetime64 if it's not already
    file_dates_np = np.array(file_dates)
    # Attempt to find the index of the exact date
    exact_match_indices = np.where(file_dates_np == date)[0]
    if exact_match_indices.size > 0:
        # Return the first exact match if it exists
        return int(exact_match_indices[0])
    else:
        # If the date is not found, find the closest one
        if file_dates_np.size == 0:
            logging.error("Error: The file_dates list is empty.")
            return None
        else:
            # Calculate the absolute difference between the target date and each date in file_dates
            differences = np.abs(file_dates_np - date)
            # Find the index of the smallest difference
            closest_index = np.argmin(differences)
            # Optionally, print a message about the closest date found
            closest_date_str = np.datetime_as_string(file_dates_np[closest_index], unit='D')
            logging.debug(f"Exact date not found. The closest date to {np.datetime_as_string(date, unit='D')} is {closest_date_str}.")
            return int(closest_index)