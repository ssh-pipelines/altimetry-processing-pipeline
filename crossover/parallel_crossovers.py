from typing import Tuple
import numpy as np
import xarray as xr
import os
import logging
from datetime import datetime, UTC

from crossover.xover_ssh import xover_ssh
from crossover.Crossover import SourceWindow
from crossover.utils.aws_utils import aws_manager

MAX_CROSSOVERS: int = 100 * 25
EPOCH: np.datetime64 = np.datetime64('1990-01-01T00:00:00.000000')
WINDOW_SIZE: int = 10
WINDOW_PADDING: int = 2
CYCLE_LENGTH: float = 9.9156
ZERO_DIFF: np.timedelta64 = np.timedelta64(0, 'ns')
MAX_DIFF: np.timedelta64 = np.timedelta64(int(CYCLE_LENGTH * 86400000000000), 'ns')

def get_data_at_track(source_window:SourceWindow, trackid: int) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Get data corresponding to a specific track ID.

    Parameters:
    - source_window: SourceWindow object containing data.
    - trackid: Track ID for which to retrieve data.

    Returns:
    - Tuple containing time, lonlat, and ssh data arrays.
    """
    track_mask = source_window.track_masks[trackid]
    time = (source_window.time[track_mask] - EPOCH).astype('timedelta64[ns]').astype('float64')
    lonlat = np.column_stack((source_window.lon[track_mask], source_window.lat[track_mask]))
    ssh = source_window.ssh[track_mask]
    return time, lonlat, ssh

def find_possible_trackids(source_window_1: SourceWindow, source_window_2: SourceWindow, trackid1: int, i: int) -> np.ndarray:
    """
    Find possible track IDs for crossovers based on given criteria.

    Parameters:
    - source_window_1: SourceWindow object for the first satellite.
    - source_window_2: SourceWindow object for the second satellite.
    - trackid1: Track ID from the first satellite.
    - i: Index of the current track ID.

    Returns:
    - Array of possible track IDs for crossovers.
    """
    
    # 1. Don't look for crossovers in the same or adjacent cycle/pass number.
    different_cycles = np.abs(trackid1 - source_window_2.unique_trackid) > 1
    # 2. Only match ascending with descending passes and vice versa.
    opposite_passes = (trackid1 % 2) != (source_window_2.unique_trackid % 2)
    # 3. Only match passes within window length, but make sure the time difference is positive to avoid double-counting.
    within_window = (
        (source_window_2.trackid_start_times - source_window_1.trackid_start_times[i] <= MAX_DIFF) &
        (source_window_2.trackid_start_times - source_window_1.trackid_start_times[i] > ZERO_DIFF)
    )
    
    return source_window_2.unique_trackid[different_cycles & opposite_passes & within_window]

def search_day_for_crossovers(current_day: np.datetime64, source_window_1: SourceWindow, source_window_2: SourceWindow) -> xr.Dataset:
    """
    Search for crossovers on a given day.

    Parameters:
    - current_day: The current day for which to find crossovers.
    - source_window_1: SourceWindow object containing satellite 1 configuration and data.
    - source_window_2: SourceWindow object containing satellite 2 configuration and data.
    
    Returns:
    - ds: xr.Dataset containing the crossover data for a single day
    """


    
    # Initialize a dictionary that will store one day's worth of crossover data.
    crossover_data = {
        'time1'   : np.zeros(MAX_CROSSOVERS, dtype='datetime64[ns]'),
        'time2'   : np.zeros(MAX_CROSSOVERS, dtype='datetime64[ns]'),
        'lon'     : np.zeros(MAX_CROSSOVERS, dtype='float64'),
        'lat'     : np.zeros(MAX_CROSSOVERS, dtype='float64'),
        'ssh1'    : np.zeros(MAX_CROSSOVERS, dtype='float64'),
        'ssh2'    : np.zeros(MAX_CROSSOVERS, dtype='float64'),
        'cycle1'  : np.zeros(MAX_CROSSOVERS, dtype='int32'),
        'pass1'   : np.zeros(MAX_CROSSOVERS, dtype='int32'),
        'cycle2'  : np.zeros(MAX_CROSSOVERS, dtype='int32'),
        'pass2'   : np.zeros(MAX_CROSSOVERS, dtype='int32'),
    }

    logging.info(f"Processing {np.datetime_as_string(current_day, unit='D')}")
    next_day = current_day + np.timedelta64(1, 'D')
    crossover_count = 0 # Reset counter used to keep track of how many crossovers we've found today.

    # Loop through unique trackids in the first day of the running window for the first satellite,
    # filtering to only include those where the start time is before the last time of the first day.   
    for i, trackid1 in enumerate(source_window_1.unique_trackid[source_window_1.trackid_start_times < next_day]):
        time1, lonlat1, ssh1 = get_data_at_track(source_window_1, trackid1)
        trackid2_possible_crossovers = find_possible_trackids(source_window_1, source_window_2, trackid1, i)
        
        # Now loop through the trackids for the second satellite that might have crossovers with the current trackid1.
        for trackid2 in trackid2_possible_crossovers:
            time2, lonlat2, ssh2 = get_data_at_track(source_window_2, trackid2)

            # Use the xover_ssh function to compute crossovers.
            if len(time1) > 1 and len(time2) > 1:
                xcoords, xssh, xtime = xover_ssh(lonlat1, lonlat2, ssh1, ssh2, time1, time2)

                if np.size(xcoords) == 0:
                    continue

                crossover_data['ssh1'][crossover_count] = xssh[0]
                crossover_data['ssh2'][crossover_count] = xssh[1]
                crossover_data['time1'][crossover_count] = EPOCH + np.timedelta64(int(xtime[0]), 'ns')                    
                crossover_data['time2'][crossover_count] = EPOCH + np.timedelta64(int(xtime[1]), 'ns')
                crossover_data['lon'][crossover_count] = xcoords[0]
                crossover_data['lat'][crossover_count] = xcoords[1]
                crossover_data['cycle1'][crossover_count] = trackid1 // 10000
                crossover_data['cycle2'][crossover_count] = trackid2 // 10000
                crossover_data['pass1'][crossover_count] = trackid1 % 10000
                crossover_data['pass2'][crossover_count] = trackid2 % 10000
                crossover_count += 1

    # Trim crossovers to eliminate empty elements.
    crossover_data = {key: value[:crossover_count] for key, value in crossover_data.items()}

    # Trim crossovers so we only retain crossovers on this_day.
    today_indices = np.where(crossover_data['time1'] < next_day)[0]
    crossover_data = {key: value[today_indices] for key, value in crossover_data.items()}

    # After filtering the crossovers for the current day
    # Get the indices that would sort the 'time1' array
    sorted_indices = np.argsort(crossover_data['time1'])
    # Use these indices to sort all arrays in the dictionary
    crossover_data = {key: value[sorted_indices] for key, value in crossover_data.items()}

    ds = xr.Dataset(
        data_vars={
            "time2": ('time1', crossover_data["time2"],
                {"long_name": "Time of crossover in later pass"},
            ),
            "lon": ('time1', crossover_data["lon"],
                {"units": "degrees", "long_name": "Crossover longitude"},
            ),
            "lat": ('time1', crossover_data["lat"],
                {"units": "degrees", "long_name": "Crossover latitude"},
            ),
            "ssh1": ('time1', crossover_data["ssh1"],
                {"units": "m", "long_name": "SSH at crossover in earlier pass"},
            ),
            "ssh2": ('time1', crossover_data["ssh2"],
                {"units": "m", "long_name": "SSH at crossover in later pass"},
            ),
            "cycle1": ('time1', crossover_data["cycle1"],
                {"units": "N/A", "long_name": "Cycle number of earlier pass"},
            ),
            "cycle2": ('time1', crossover_data["cycle2"],
                {"units": "N/A", "long_name": "Cycle number of later pass"},
            ),
            "pass1": ('time1', crossover_data["pass1"],
                {"units": "N/A", "long_name": "Pass number of earlier pass"},
            ),
            "pass2": ('time1', crossover_data["pass2"],
                {"units": "N/A", "long_name": "Pass number of later pass"},
            ),
        },
        coords={
            'time1': ('time1', crossover_data["time1"],
                {"long_name": "Time of crossover in earlier pass"},
            )
        },
        attrs={
            "title": source_window_1.shortname + ' self-crossovers',
            "subtitle": f"within {WINDOW_SIZE} days",
            "window_length": f"{(source_window_1.window_end - source_window_1.window_start).astype('int32')} days (nominal: {WINDOW_SIZE} days + {WINDOW_PADDING} days padding)",
            "created_on": datetime.now(UTC).strftime('%Y-%m-%dT%H%M%S'),
            "input_filenames": source_window_1.input_filenames,
            "input_histories": source_window_1.input_histories,
            "input_product_generation_steps": source_window_1.input_product_generation_steps,
            "satellite_names": ', '.join(list(set([source_window_1.shortname, source_window_2.shortname]))),
        }
    )

    ds['time1'].encoding['units'] = f"seconds since {EPOCH}"
    ds['time2'].encoding['units'] = f"seconds since {EPOCH}"
    logging.info(f"Processing {np.datetime_as_string(current_day, unit='D')} complete")
    return ds

def window_init(day: np.datetime64, source1: str, source2: str, df_version: str):
    logging.info(f"Looking for {source1} self-crossovers...")
    
    # Window starts and ends
    window_1 = (day, day + np.timedelta64(WINDOW_SIZE + WINDOW_PADDING, 'D'))
    window_2 = (day, day + np.timedelta64(WINDOW_SIZE + WINDOW_PADDING, 'D'))
        
    source_window_1 = SourceWindow(df_version, source1, day, *(window_1))
    source_window_2 = SourceWindow(df_version, source2, day, *(window_2))
    return source_window_1, source_window_2

def compute_crossovers(day: np.datetime64, source1: str, source2: str, df_version: str):
    logging.root.handlers = []
    logging.basicConfig(
        level='INFO',
        format='[%(levelname)s] %(asctime)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    
    source_window_1, source_window_2 = window_init(day, source1, source2, df_version)
    
    # Initialize satellite windows with filepaths and data
    for i, source_window in enumerate([source_window_1, source_window_2], 1):
        logging.info(f'Initializing and filling data for window {i}...')
        source_window.set_filepaths_and_dates()
        source_window.stream_files()
        source_window.init_and_fill_running_window()

    # Search for crossovers between both windows
    ds = search_day_for_crossovers(day, source_window_1, source_window_2)
    
    # Save to netCDF file to tmp
    sats_as_string = ds.attrs['satellite_names'].replace(', ','_')
    filename = f'xovers_{sats_as_string}-{np.datetime_as_string(day)}.nc'
    local_output_path = os.path.join('/tmp', filename)
    logging.info(f'Saving netcdf to {local_output_path}')
    ds.to_netcdf(os.path.join(local_output_path))
    
    # Upload to s3
    s3_output_path = os.path.join('crossovers', df_version, sats_as_string, np.datetime_as_string(day, unit='Y'), filename)
    aws_manager.upload_s3(local_output_path, aws_manager.DAILY_FILE_BUCKET, s3_output_path)
