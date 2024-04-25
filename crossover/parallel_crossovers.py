import numpy as np
import xarray as xr
import pandas as pd
import os
import logging

from crossover.xover_ssh import xover_ssh
from crossover.Crossover import Options, SourceWindow
from crossover.utils.aws_utils import aws_manager


def search_day_for_crossovers(this_day: np.datetime64, source_window_1: SourceWindow, source_window_2: SourceWindow) -> None:
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
        #print(f"{trackid1 = }, start time: {np.datetime_as_string(window1['trackid_start_times'][ind1], unit='s')}")
        track_mask1 = source_window_1.track_masks[trackid1]
        time1 = (source_window_1.time[track_mask1] - OPTIONS.epoch).astype('timedelta64[ns]').astype('float64')
        lonlat1 = np.column_stack((source_window_1.lon[track_mask1], source_window_1.lat[track_mask1]))
        ssh1 = source_window_1.ssh[track_mask1]
        #Look for trackids for the second satellite that might have crossovers with the current trackid1.
        #The criteria for this search are different for self-crossovers and crossovers between different satellites.
        if OPTIONS.self_crossovers:
            # 1. Don't look for crossovers in the same or adjacent cycle/pass number.
            boolean1 = np.abs((trackid1 - source_window_2.unique_trackid)) > 1
            # 2. Only match ascending with descending passes and vice versa.
            boolean2 = (trackid1 % 2) != (source_window_2.unique_trackid % 2)
            # 3. Only match passes within window length, but make sure the time difference is positive to avoid double-counting.
            boolean3 = (((source_window_2.trackid_start_times - source_window_1.trackid_start_times[ind1]) <= OPTIONS.max_diff) & \
                ((source_window_2.trackid_start_times - source_window_1.trackid_start_times[ind1]) > OPTIONS.zero_diff))
        else:
            # 1. Look for crossovers in any cycle/pass numbers because they're different between satellites 1 and 2.
            boolean1 = True
            # 2. Match ascending passes with descending AND ascending passes
            boolean2 = True
            # 3. Only match passes within a half cycle. Don't worry if the time difference is negative, but it can't be more negative than a half cycle.
            boolean3 = (((source_window_2.trackid_start_times - source_window_1.trackid_start_times[ind1]) <= OPTIONS.max_diff) & \
                ((source_window_2.trackid_start_times - source_window_1.trackid_start_times[ind1]) > -OPTIONS.max_diff))
            
        trackid2_possible_crossovers = source_window_2.unique_trackid[np.where(boolean1 & boolean2 & boolean3)[0]]

        #Now loop through the trackids for the second satellite that might have crossovers with the current trackid1.
        for trackid2 in trackid2_possible_crossovers:
            #print(f"  {trackid2 = }, start time: {np.datetime_as_string(window2['trackid_start_times'][trackid2], unit='s')}")
            track_mask2 = source_window_2.track_masks[trackid2]
            time2 = (source_window_2.time[track_mask2] - OPTIONS.epoch).astype('timedelta64[ns]').astype('float64')
            lonlat2 = np.column_stack((source_window_2.lon[track_mask2], source_window_2.lat[track_mask2]))
            ssh2 = source_window_2.ssh[track_mask2]
            #Use the xover_ssh function to compute crossovers.
            if len(time1) > 1 and len(time2) > 1:

                xcoords, xssh, xtime = xover_ssh(lonlat1, lonlat2, ssh1, ssh2, time1, time2)
                #Keep valid crossover points:
                if np.size(xcoords)!=0:
                    if np.size(xcoords) > 2: 
                        logging.error(f"!!!!WARNING!!! {np.size(xcoords) = }")
                    crossover_data_for_today['ssh1']  [num_crossovers_today] = xssh[0]
                    crossover_data_for_today['ssh2']  [num_crossovers_today] = xssh[1]
                    crossover_data_for_today['time1'] [num_crossovers_today] = OPTIONS.epoch + np.timedelta64(int(xtime[0]), 'ns')                    
                    crossover_data_for_today['time2'] [num_crossovers_today] = OPTIONS.epoch + np.timedelta64(int(xtime[1]), 'ns')
                    crossover_data_for_today['lon']   [num_crossovers_today] = xcoords[0]
                    crossover_data_for_today['lat']   [num_crossovers_today] = xcoords[1]
                    crossover_data_for_today['cycle1'][num_crossovers_today] = trackid1 // 10000
                    crossover_data_for_today['cycle2'][num_crossovers_today] = trackid2 // 10000
                    crossover_data_for_today['pass1'] [num_crossovers_today] = trackid1 % 10000
                    crossover_data_for_today['pass2'] [num_crossovers_today] = trackid2 % 10000
                    num_crossovers_today += 1

    #Trim crossovers to eliminate empty elements.
    crossover_data_for_today = {key: value[:num_crossovers_today] for key, value in crossover_data_for_today.items()}

    #Trim crossovers so we only retain crossovers on this_day.
    today_indices = np.where(crossover_data_for_today['time1'] < next_day)[0]
    crossover_data_for_today = {key: value[today_indices] for key, value in crossover_data_for_today.items()}
    
    # After filtering the crossovers for the current day
    # Get the indices that would sort the 'time1' array
    sorted_indices = np.argsort(crossover_data_for_today['time1'])
    # Use these indices to sort all arrays in the dictionary
    crossover_data_for_today = {key: value[sorted_indices] for key, value in crossover_data_for_today.items()}

    # Create a dimension name based on 'time1'
    time_dim = 'time1'

    ds = xr.Dataset(
        {
            "time2": (time_dim, crossover_data_for_today['time2'], {
                "long_name": "Time of crossover in later pass"
            }),
            "lon": (time_dim, crossover_data_for_today['lon'], {
                "units": "degrees", 
                "long_name": "Crossover longitude"
            }),
            "lat": (time_dim, crossover_data_for_today['lat'], {
                "units": "degrees", 
                "long_name": "Crossover latitude"
            }),
            "ssh1": (time_dim, crossover_data_for_today['ssh1'], {
                "units": "m", 
                "long_name": "SSH at crossover in earlier pass"
            }),
            "ssh2": (time_dim, crossover_data_for_today['ssh2'], {
                "units": "m", 
                "long_name": "SSH at crossover in later pass"
            }),
            "cycle1": (time_dim, crossover_data_for_today['cycle1'], {
                "units": "N/A", 
                "long_name": "Cycle number of earlier pass"
            }),
            "cycle2": (time_dim, crossover_data_for_today['cycle2'], {
                "units": "N/A", 
                "long_name": "Cycle number of later pass"
            }),
            "pass1": (time_dim, crossover_data_for_today['pass1'], {
                "units": "N/A", 
                "long_name": "Pass number of earlier pass"
            }),
            "pass2": (time_dim, crossover_data_for_today['pass2'], {
                "units": "N/A", 
                "long_name": "Pass number of later pass"
            })
        },
        coords={
            time_dim: (time_dim, crossover_data_for_today['time1'], {
                "long_name": "Time of crossover in earlier pass"
            })
        }
    )

    # Add global attributes
    if OPTIONS.self_crossovers:
        ds.attrs['title'] = source_window_1.shortname + ' crossovers with ' + source_window_2.shortname
    else:
        ds.attrs['title'] = source_window_1.shortname + ' self-crossovers'
    ds.attrs['subtitle'] = f"within {OPTIONS.window_size} days"

    # convert "last_day_in_window - first_day_in_window" from numpy.timedelta64 to int:
    ds.attrs['window_length'] = f"{(source_window_1.window_end - source_window_1.window_start).astype('int32')} days (nominal: {OPTIONS.window_size} days + {OPTIONS.window_padding} days padding)"

    # Save the current local date and time as the "created_on" attribute.
    current_utc_datetime = np.datetime64('now', 's')
    # Convert numpy datetime64 to pandas Timestamp, which supports timezone operations
    current_utc_timestamp = pd.to_datetime(str(current_utc_datetime))
    # Localize the timestamp to UTC, then convert to Los Angeles time (PST or PDT depending on the date)
    la_timestamp = current_utc_timestamp.tz_localize('UTC').tz_convert('America/Los_Angeles')
    # Get timezone name (PST or PDT)
    timezone_name = la_timestamp.tzname()
    # Format the timestamp as a string and append the timezone name
    la_datetime_str_with_tz = la_timestamp.strftime('%Y-%m-%d %H:%M:%S') + f" ({timezone_name})"
    # Assign the local time string with timezone to your dataset's attribute
    ds.attrs['created_on'] = la_datetime_str_with_tz

    # Add the input filenames, histories, product_generation_steps and satellite names to the global attributes
    if OPTIONS.self_crossovers:
        ds.attrs['input_filenames'] = source_window_1.input_filenames
        ds.attrs['input_histories'] = source_window_1.input_histories
        ds.attrs['input_product_generation_steps'] = source_window_1.input_product_generation_steps
        ds.attrs['satellite_names'] = OPTIONS.all_sat_names
    else:
        ds.attrs['input_filenames'] = f'{source_window_1.input_filenames}, {source_window_2.input_filenames}'
        ds.attrs['input_histories'] = f'{source_window_1.input_histories}, {source_window_2.input_histories}'
        ds.attrs['input_product_generation_steps'] = f'{source_window_1.input_product_generation_steps}, {source_window_2.input_product_generation_steps}'
        ds.attrs['satellite_names'] = OPTIONS.all_sat_names

    # Set the units attribute on both time variables so it references the epoch correctly.
    # Check if 'units' attribute exists and remove it
    if 'units' in ds['time1'].attrs:
        del ds['time1'].attrs['units']
    if 'units' in ds['time2'].attrs:
        del ds['time2'].attrs['units']
    ds['time1'].encoding['units'] = f"seconds since {OPTIONS.epoch}"
    ds['time2'].encoding['units'] = f"seconds since {OPTIONS.epoch}"
    return ds
    


def crossover_setup(day: np.datetime64, source1: str, source2: str):
    logging.info('Setting up global options and two source windows.')
    global OPTIONS
    OPTIONS = Options(source1, source2)

    #If sat1 and sat2 are the same, we're looking for "self-crossovers" between the same satellite.
    #Are we looking for self crossovers or crossovers between multiple satellites?
    if OPTIONS.self_crossovers:
        logging.info(f"Looking for {source1} self-crossovers...")
    else:
        logging.info(f"Looking for crossovers between {source1} and {source2}")
        logging.error("Non self crossover window length attr needs to be addressed. Quitting for now.")
        raise RuntimeError("Non self crossover window length attr needs to be addressed. Quitting for now.")
    
    if OPTIONS.self_crossovers:
        window_start_1 = day
        window_end_1 = day + np.timedelta64(OPTIONS.window_size + OPTIONS.window_padding, 'D')
        window_start_2 = day
        window_end_2 = day + np.timedelta64(OPTIONS.window_size + OPTIONS.window_padding, 'D')
    else:
        window_start_1 = day
        window_end_1 = day + np.timedelta64(OPTIONS.window_padding + 1, 'D')        
        window_start_2 = day - np.timedelta64(OPTIONS.window_size + OPTIONS.window_padding, 'D')
        window_end_2 = day + np.timedelta64(OPTIONS.window_size + OPTIONS.window_padding, 'D')
        
    source_window_1 = SourceWindow(source1, day, window_start_1, window_end_1)
    source_window_2 = SourceWindow(source2, day, window_start_2, window_end_2)
    return source_window_1, source_window_2

def initialize_data(source_window: SourceWindow):
    source_window.set_filepaths()
    source_window.set_file_dates()
    source_window.stream_files()
    source_window.init_and_fill_running_window()


def compute_crossovers(day: np.datetime64, source1: str, source2: str):
    source_window_1, source_window_2 = crossover_setup(day, source1, source2)
    
    # Initialize satellite windows with filepaths and data
    for source_window in [source_window_1, source_window_2]:
        initialize_data(source_window)

    ds = search_day_for_crossovers(day, source_window_1, source_window_2)
    
    # Save to netCDF file to tmp
    year_as_string = str(day.astype('datetime64[Y]')).split('-')[0]
    sats_as_string = ds.attrs['satellite_names'].replace(', ','_')
    filename = f'xovers_{sats_as_string}-{np.datetime_as_string(day)}.nc'
    local_output_path = os.path.join('/tmp', filename)
    ds.to_netcdf(os.path.join(local_output_path))
    
    # Upload to s3
    s3_output_path = os.path.join('s3://', aws_manager.DAILY_FILE_BUCKET, 'crossovers', sats_as_string, year_as_string, filename)
    aws_manager.upload_s3(local_output_path, aws_manager.DAILY_FILE_BUCKET, s3_output_path)