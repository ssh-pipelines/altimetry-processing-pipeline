from dataclasses import dataclass, fields
from typing import Iterable, Tuple
import numpy as np
import xarray as xr
import os
import logging
from datetime import datetime, UTC

from crossover.xover_ssh import xover_ssh
from crossover.Crossover import SourceWindow
from crossover.utils.aws_utils import aws_manager


EPOCH: np.datetime64 = np.datetime64('1990-01-01T00:00:00.000000')
WINDOW_SIZE: int = 10
WINDOW_PADDING: int = 2
CYCLE_LENGTH: float = 9.9156
ZERO_DIFF: np.timedelta64 = np.timedelta64(0, 'ns')
MAX_DIFF: np.timedelta64 = np.timedelta64(int(CYCLE_LENGTH * 86400000000000), 'ns')

@dataclass
class CrossoverData:
    time1: Iterable[np.datetime64]
    time2: Iterable[np.datetime64]
    lon: Iterable[float]
    lat: Iterable[float]
    ssh1: Iterable[float]
    ssh2: Iterable[float]
    cycle1: Iterable[int]
    pass1: Iterable[int]
    cycle2: Iterable[int]
    pass2: Iterable[int]

    @classmethod
    def create_empty(cls) -> "CrossoverData":
        return cls(
            time1=[],
            time2=[],
            lon=[],
            lat=[],
            ssh1=[],
            ssh2=[],
            cycle1=[],
            pass1=[],
            cycle2=[],
            pass2=[]
        )

    def to_numpy(self):
        for field in fields(self):
            value = getattr(self, field.name)
            setattr(self, field.name, np.array(value))

    def filter_and_sort(self, next_day: np.datetime64):
        self.to_numpy()  # Convert lists to numpy arrays
        mask = self.time1 < next_day
        for field in fields(self):
            value = getattr(self, field.name)
            setattr(self, field.name, value[mask])
        
        sorted_indices = np.argsort(self.time1)
        for field in fields(self):
            value = getattr(self, field.name)
            setattr(self, field.name, value[sorted_indices])

class CrossoverProcessor:
    def __init__(self, day: np.datetime64, source: str, df_version: str):
        self.day = day
        self.source = source
        self.df_version = df_version
        self.source_window: SourceWindow = self.window_init()

    def window_init(self) -> SourceWindow:
        logging.info(f"Looking for {self.source} self-crossovers...")
        
        window_start = self.day
        window_end = self.day + np.timedelta64(WINDOW_SIZE + WINDOW_PADDING, 'D')
        return SourceWindow(self.df_version, self.source, self.day, window_start, window_end)
    
    def initialize_and_fill_data(self) -> bool:
        logging.info(f'Initializing and filling data for window...')
        self.source_window.set_filepaths_and_dates()
        self.source_window.stream_files()
        if len(self.source_window.streams) > 0:
            self.source_window.init_and_fill_running_window()
            return True
        else:
            logging.info(f'No valid data found in {self.source_window.shortname} window {self.source_window.window_start} to {self.source_window.window_end}.')
            return False
    
    def create_or_search_xovers(self) -> xr.Dataset:
        if not self.initialize_and_fill_data():
            ds = self.create_dataset(CrossoverData.create_empty())
        try:
            ds = self.search_day_for_crossovers()
        except Exception as e:
            logging.error(f"Error searching for crossovers. Making empty xover instead. {e}")
            ds = self.create_dataset(CrossoverData.create_empty())
        return ds
    
    def search_day_for_crossovers(self) -> xr.Dataset:
        crossover_data = CrossoverData.create_empty()
        logging.info(f"Processing {np.datetime_as_string(self.day, unit='D')}")
        next_day = self.day + np.timedelta64(1, 'D')

        for i, trackid1 in enumerate(self.source_window.unique_trackid[self.source_window.trackid_start_times < next_day]):
            time1, lonlat1, ssh1 = self.get_data_at_track(trackid1)
            trackid2_possible_crossovers = self.find_possible_trackids(trackid1, i)
            
            for trackid2 in trackid2_possible_crossovers:
                time2, lonlat2, ssh2 = self.get_data_at_track(trackid2)

                if len(time1) > 1 and len(time2) > 1:
                    xcoords, xssh, xtime = xover_ssh(lonlat1, lonlat2, ssh1, ssh2, time1, time2)

                    if np.size(xcoords) == 0:
                        continue

                    crossover_data.ssh1.append(xssh[0])
                    crossover_data.ssh2.append(xssh[1])
                    crossover_data.time1.append(EPOCH + np.timedelta64(int(xtime[0]), 'ns'))
                    crossover_data.time2.append(EPOCH + np.timedelta64(int(xtime[1]), 'ns'))
                    crossover_data.lon.append(xcoords[0])
                    crossover_data.lat.append(xcoords[1])
                    crossover_data.cycle1.append(trackid1 // 10000)
                    crossover_data.cycle2.append(trackid2 // 10000)
                    crossover_data.pass1.append(trackid1 % 10000)
                    crossover_data.pass2.append(trackid2 % 10000)

        crossover_data.filter_and_sort(next_day)
        return self.create_dataset(crossover_data)
    
    def get_data_at_track(self, trackid: int) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        track_mask = self.source_window.track_masks[trackid]
        time = (self.source_window.time[track_mask] - EPOCH).astype('timedelta64[ns]').astype('float64')
        lonlat = np.column_stack((self.source_window.lon[track_mask], self.source_window.lat[track_mask]))
        ssh = self.source_window.ssh[track_mask]
        return time, lonlat, ssh

    def find_possible_trackids(self, trackid1: int, i: int) -> np.ndarray:
        different_cycles = np.abs(trackid1 - self.source_window.unique_trackid) > 1
        opposite_passes = (trackid1 % 2) != (self.source_window.unique_trackid % 2)
        within_window = (
            (self.source_window.trackid_start_times - self.source_window.trackid_start_times[i] <= MAX_DIFF) &
            (self.source_window.trackid_start_times - self.source_window.trackid_start_times[i] > ZERO_DIFF)
        )
        
        return self.source_window.unique_trackid[different_cycles & opposite_passes & within_window]
    
    def create_dataset(self, crossover_data: CrossoverData) -> xr.Dataset:
        ds = xr.Dataset(
            data_vars={
                "time2": ('time1', crossover_data.time2,
                    {"long_name": "Time of crossover in later pass"},
                ),
                "lon": ('time1', crossover_data.lon,
                    {"units": "degrees", "long_name": "Crossover longitude"},
                ),
                "lat": ('time1', crossover_data.lat,
                    {"units": "degrees", "long_name": "Crossover latitude"},
                ),
                "ssh1": ('time1', crossover_data.ssh1,
                    {"units": "m", "long_name": "SSH at crossover in earlier pass"},
                ),
                "ssh2": ('time1', crossover_data.ssh2,
                    {"units": "m", "long_name": "SSH at crossover in later pass"},
                ),
                "cycle1": ('time1', crossover_data.cycle1,
                    {"units": "N/A", "long_name": "Cycle number of earlier pass"},
                ),
                "cycle2": ('time1', crossover_data.cycle2,
                    {"units": "N/A", "long_name": "Cycle number of later pass"},
                ),
                "pass1": ('time1', crossover_data.pass1,
                    {"units": "N/A", "long_name": "Pass number of earlier pass"},
                ),
                "pass2": ('time1', crossover_data.pass2,
                    {"units": "N/A", "long_name": "Pass number of later pass"},
                ),
            },
            coords={
                'time1': ('time1', crossover_data.time1,
                    {"long_name": "Time of crossover in earlier pass"},
                )
            },
            attrs={
                "title": f'{self.source_window.shortname} self-crossovers',
                "subtitle": f"within {WINDOW_SIZE} days",
                "window_length": f"{(self.source_window.window_end - self.source_window.window_start).astype('int32')} days (nominal: {WINDOW_SIZE} days + {WINDOW_PADDING} days padding)",
                "created_on": datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%S'),
                "input_filenames": self.source_window.input_filenames,
                "input_histories": self.source_window.input_histories,
                "input_product_generation_steps": self.source_window.input_product_generation_steps,
                "satellite_names": self.source_window.shortname
            }
        )

        ds['time1'].encoding['units'] = f"seconds since {EPOCH}"
        ds['time2'].encoding['units'] = f"seconds since {EPOCH}"
        logging.info(f"Processing {np.datetime_as_string(self.day, unit='D')} complete")
        return ds
        
    def save_to_netcdf(self, ds: xr.Dataset) -> str:
        filename = f'xovers_{self.source}-{np.datetime_as_string(self.day)}.nc'
        local_output_path = os.path.join('/tmp', filename)
        logging.info(f'Saving netcdf to {local_output_path}')
        ds.to_netcdf(local_output_path)
        return local_output_path
    
    def upload_xover(self, local_path):
        filename = os.path.basename(local_path)
        s3_output_path = os.path.join('crossovers', self.df_version, self.source, np.datetime_as_string(self.day, unit='Y'), filename)
        aws_manager.upload_s3(local_path, aws_manager.DAILY_FILE_BUCKET, s3_output_path)
        
    def run(self):
        ds = self.create_or_search_xovers()
        local_path = self.save_to_netcdf(ds)
        self.upload_xover(local_path)