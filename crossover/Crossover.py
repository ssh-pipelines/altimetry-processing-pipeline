import logging
import os
import re
from typing import Iterable
import numpy as np
import xarray as xr

from crossover.utils.aws_utils import aws_manager

DATE_PATTERN = re.compile(r'\d{8}')

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
        
        unique_years = list({start_year, end_year})
        all_keys = []
        dates = []
        
        for year in unique_years:
            prefix = os.path.join('daily_files', self.df_version, self.shortname, year)
            s3_keys = aws_manager.get_filepaths(prefix)
            for fp in s3_keys:
                date = self.date_from_fp(fp)
                if self.window_start <= date <= self.window_end:
                    all_keys.append(fp)
                    dates.append(date)
        
        self.filepaths = all_keys
        self.file_dates: Iterable[np.datetime64] = dates
        
    def stream_files(self):
        streams = []
        for key in self.filepaths:
            if aws_manager.check_exists(key):
                streams.append(aws_manager.stream_s3(key))
            else:
                logging.warning(f'Unable to stream {key} as it does not exist')
        self.streams = streams

    def init_and_fill_running_window(self):
        '''
        Initialize and fill a running window with satellite data loaded from disk.
        '''

        window_indexes = self.make_window_indexes()
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

        self.unique_trackid = np.unique(self.trackid)
        
        starts = []
        self.track_masks = {}
        for track_id_i in self.unique_trackid:
            starts.append(np.min(self.time[self.trackid == track_id_i]))
            self.track_masks[track_id_i] = np.where(self.trackid == track_id_i)[0]
        
        self.trackid_start_times = np.array(starts, dtype='datetime64[ns]')

    @staticmethod
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

    def make_window_indexes(self) -> Iterable[int]:
        first_day_in_window_index = self.find_closest_date_index(self.window_start, self.file_dates)
        last_day_in_window_index  = self.find_closest_date_index(self.window_end,  self.file_dates)
        return range(first_day_in_window_index, last_day_in_window_index + 1)

    @staticmethod
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