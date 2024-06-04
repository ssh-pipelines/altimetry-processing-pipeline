import numpy as np
import xarray as xr
import os
import logging
from datetime import datetime, UTC

from crossover.xover_ssh import xover_ssh
from crossover.Crossover import Options, SourceWindow
from crossover.utils.aws_utils import aws_manager


def search_day_for_crossovers(this_day: np.datetime64, source_window_1: SourceWindow, source_window_2: SourceWindow) -> xr.Dataset:
    """
    Search for crossovers on a given day.

    Parameters:
    - this_day: The current day for which to find crossovers.
    - source_window_1: SourceWindow object containing satellite 1 configuration and data.
    - source_window_2: SourceWindow object containing satellite 2 configuration and data.
    Returns:
    - None
    """

    # Initialize a dictionary that will store one day's worth of crossover data.
    crossover_data_for_today = {
        'time1'   : np.zeros(OPTIONS.max_crossovers_per_day, dtype='datetime64[ns]'),
        'time2'   : np.zeros(OPTIONS.max_crossovers_per_day, dtype='datetime64[ns]'),
        'lon'     : np.zeros(OPTIONS.max_crossovers_per_day, dtype='float64'),
        'lat'     : np.zeros(OPTIONS.max_crossovers_per_day, dtype='float64'),
        'ssh1'    : np.zeros(OPTIONS.max_crossovers_per_day, dtype='float64'),
        'ssh2'    : np.zeros(OPTIONS.max_crossovers_per_day, dtype='float64'),
        'cycle1'  : np.zeros(OPTIONS.max_crossovers_per_day, dtype='int32'),
        'pass1'   : np.zeros(OPTIONS.max_crossovers_per_day, dtype='int32'),
        'cycle2'  : np.zeros(OPTIONS.max_crossovers_per_day, dtype='int32'),
        'pass2'   : np.zeros(OPTIONS.max_crossovers_per_day, dtype='int32'),
    }

    logging.info(f"Processing {np.datetime_as_string(this_day, unit='D')}")
    next_day = this_day + np.timedelta64(1, 'D')
    num_crossovers_today = 0 #Reset counter used to keep track of how many crossovers we've found today.

    # Loop through unique trackids in the first day of the running window for the first satellite,
    # filtering to only include those where the start time is before the last time of the first day.
    trackid1_starting_today_indices = np.where(source_window_1.trackid_start_times < next_day)[0]
    trackid1_starting_today = source_window_1.unique_trackid[trackid1_starting_today_indices]

    for ind1,trackid1 in enumerate(trackid1_starting_today):
        track_mask1 = source_window_1.track_masks[trackid1]
        time1 = (source_window_1.time[track_mask1] - OPTIONS.epoch).astype('timedelta64[ns]').astype('float64')
        lonlat1 = np.column_stack((source_window_1.lon[track_mask1], source_window_1.lat[track_mask1]))
        ssh1 = source_window_1.ssh[track_mask1]

        # 1. Don't look for crossovers in the same or adjacent cycle/pass number.
        boolean1 = np.abs((trackid1 - source_window_2.unique_trackid)) > 1
        # 2. Only match ascending with descending passes and vice versa.
        boolean2 = (trackid1 % 2) != (source_window_2.unique_trackid % 2)
        # 3. Only match passes within window length, but make sure the time difference is positive to avoid double-counting.
        boolean3 = (((source_window_2.trackid_start_times - source_window_1.trackid_start_times[ind1]) <= OPTIONS.max_diff) & \
                    ((source_window_2.trackid_start_times - source_window_1.trackid_start_times[ind1]) > OPTIONS.zero_diff))

        trackid2_possible_crossovers = source_window_2.unique_trackid[np.where(boolean1 & boolean2 & boolean3)[0]]

        # Now loop through the trackids for the second satellite that might have crossovers with the current trackid1.
        for trackid2 in trackid2_possible_crossovers:
            track_mask2 = source_window_2.track_masks[trackid2]
            time2 = (source_window_2.time[track_mask2] - OPTIONS.epoch).astype('timedelta64[ns]').astype('float64')
            lonlat2 = np.column_stack((source_window_2.lon[track_mask2], source_window_2.lat[track_mask2]))
            ssh2 = source_window_2.ssh[track_mask2]

            # Use the xover_ssh function to compute crossovers.
            if len(time1) > 1 and len(time2) > 1:
                xcoords, xssh, xtime = xover_ssh(lonlat1, lonlat2, ssh1, ssh2, time1, time2)

                # Keep valid crossover points:
                if np.size(xcoords)!=0:
                    if np.size(xcoords) > 2: 
                        logging.error(f"!!!!WARNING!!! {np.size(xcoords) = }")

                    crossover_data_for_today['ssh1'][num_crossovers_today] = xssh[0]
                    crossover_data_for_today['ssh2'][num_crossovers_today] = xssh[1]
                    crossover_data_for_today['time1'][num_crossovers_today] = OPTIONS.epoch + np.timedelta64(int(xtime[0]), 'ns')                    
                    crossover_data_for_today['time2'][num_crossovers_today] = OPTIONS.epoch + np.timedelta64(int(xtime[1]), 'ns')
                    crossover_data_for_today['lon'][num_crossovers_today] = xcoords[0]
                    crossover_data_for_today['lat'][num_crossovers_today] = xcoords[1]
                    crossover_data_for_today['cycle1'][num_crossovers_today] = trackid1 // 10000
                    crossover_data_for_today['cycle2'][num_crossovers_today] = trackid2 // 10000
                    crossover_data_for_today['pass1'][num_crossovers_today] = trackid1 % 10000
                    crossover_data_for_today['pass2'][num_crossovers_today] = trackid2 % 10000
                    num_crossovers_today += 1

    # Trim crossovers to eliminate empty elements.
    crossover_data_for_today = {key: value[:num_crossovers_today] for key, value in crossover_data_for_today.items()}

    # Trim crossovers so we only retain crossovers on this_day.
    today_indices = np.where(crossover_data_for_today['time1'] < next_day)[0]
    crossover_data_for_today = {key: value[today_indices] for key, value in crossover_data_for_today.items()}

    # After filtering the crossovers for the current day
    # Get the indices that would sort the 'time1' array
    sorted_indices = np.argsort(crossover_data_for_today['time1'])
    # Use these indices to sort all arrays in the dictionary
    crossover_data_for_today = {key: value[sorted_indices] for key, value in crossover_data_for_today.items()}

    ds = xr.Dataset(
        data_vars={
            "time2": (
                'time1',
                crossover_data_for_today["time2"],
                {"long_name": "Time of crossover in later pass"},
            ),
            "lon": (
                'time1',
                crossover_data_for_today["lon"],
                {"units": "degrees", "long_name": "Crossover longitude"},
            ),
            "lat": (
                'time1',
                crossover_data_for_today["lat"],
                {"units": "degrees", "long_name": "Crossover latitude"},
            ),
            "ssh1": (
                'time1',
                crossover_data_for_today["ssh1"],
                {"units": "m", "long_name": "SSH at crossover in earlier pass"},
            ),
            "ssh2": (
                'time1',
                crossover_data_for_today["ssh2"],
                {"units": "m", "long_name": "SSH at crossover in later pass"},
            ),
            "cycle1": (
                'time1',
                crossover_data_for_today["cycle1"],
                {"units": "N/A", "long_name": "Cycle number of earlier pass"},
            ),
            "cycle2": (
                'time1',
                crossover_data_for_today["cycle2"],
                {"units": "N/A", "long_name": "Cycle number of later pass"},
            ),
            "pass1": (
                'time1',
                crossover_data_for_today["pass1"],
                {"units": "N/A", "long_name": "Pass number of earlier pass"},
            ),
            "pass2": (
                'time1',
                crossover_data_for_today["pass2"],
                {"units": "N/A", "long_name": "Pass number of later pass"},
            ),
        },
        coords={
            'time1': (
                'time1',
                crossover_data_for_today["time1"],
                {"long_name": "Time of crossover in earlier pass"},
            )
        },
        attrs={
            "title": source_window_1.shortname + ' self-crossovers',
            "subtitle": f"within {OPTIONS.window_size} days",
            "window_length": f"{(source_window_1.window_end - source_window_1.window_start).astype('int32')} days (nominal: {OPTIONS.window_size} days + {OPTIONS.window_padding} days padding)",
            "created_on": datetime.now(UTC).strftime('%Y-%m-%dT%H%M%S'),
            "input_filenames": source_window_1.input_filenames,
            "input_histories": source_window_1.input_histories,
            "input_product_generation_steps": source_window_1.input_product_generation_steps,
            "satellite_names": OPTIONS.all_sat_names,
        },
    )

    ds['time1'].encoding['units'] = f"seconds since {OPTIONS.epoch}"
    ds['time2'].encoding['units'] = f"seconds since {OPTIONS.epoch}"
    logging.info(f"Processing {np.datetime_as_string(this_day, unit='D')} complete")
    return ds

def window_init(day: np.datetime64, source1: str, source2: str, df_version: str):
    logging.info('Setting up global options and two source windows.')

    global OPTIONS
    OPTIONS = Options(source1, source2)

    #If sat1 and sat2 are the same, we're looking for "self-crossovers" between the same satellite.
    #Are we looking for self crossovers or crossovers between multiple satellites?
    logging.info(f"Looking for {source1} self-crossovers...")
    
    window_start_1 = day
    window_end_1 = day + np.timedelta64(OPTIONS.window_size + OPTIONS.window_padding, 'D')
    window_start_2 = day
    window_end_2 = day + np.timedelta64(OPTIONS.window_size + OPTIONS.window_padding, 'D')
        
    source_window_1 = SourceWindow(df_version, source1, day, window_start_1, window_end_1)
    source_window_2 = SourceWindow(df_version, source2, day, window_start_2, window_end_2)
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
        source_window.set_filepaths()
        source_window.set_file_dates()
        source_window.stream_files()
        source_window.init_and_fill_running_window()

    # Search for crossovers between both windows
    ds = search_day_for_crossovers(day, source_window_1, source_window_2)
    
    # Save to netCDF file to tmp
    year_as_string = str(day.astype('datetime64[Y]')).split('-')[0]
    sats_as_string = ds.attrs['satellite_names'].replace(', ','_')
    filename = f'xovers_{sats_as_string}-{np.datetime_as_string(day)}.nc'
    local_output_path = os.path.join('/tmp', filename)
    logging.info(f'Saving netcdf to {local_output_path}')
    ds.to_netcdf(os.path.join(local_output_path))
    
    # Upload to s3
    s3_output_path = os.path.join('crossovers', df_version, sats_as_string, year_as_string, filename)
    aws_manager.upload_s3(local_output_path, aws_manager.DAILY_FILE_BUCKET, s3_output_path)
