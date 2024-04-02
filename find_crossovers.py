import numpy as np
import xarray as xr
import pandas as pd
import os, glob, copy, time, re
from typing import Union
import logging

from xover_ssh import xover_ssh

start_time = time.time()

print("IN NEXT PLOTTING SCRIPT: CALC THE RMS FOR EACH DAILY CROSSOVER FILE, THEN DO PASS BY PASS ANALYSIS: FIND ALL THE CROSSOVERS FOR EACH PASS AND NOTE THSI REQUIRES LOADING TEN PREVIOUS CROSSOVER DAILY FILES: *COMBINE* TRACKID1 AND TRACKID2 INTO ONE PLOT- SWITCH SIGNS OF SSH1-SSH2 AND TIME1-TIME2 BASED ON IF WE'RE USING TRACKID1 OR TRACKID2")

first_day = np.datetime64('1993-01-01', 'D')
last_day  = np.datetime64('1993-01-13', 'D')

sat_data_path = os.path.join('..','sat_data_for_crossovers') # This is the parent directory where the input satellite data folders are located.
#sat_data_path = '/home/username/cloud_daily_files' # This is the parent directory where the input satellite data folders are located.

crossover_files_path = os.path.join('..', 'local_crossover_files') # This is where the crossover files will be saved.
#crossover_files_path = os.path.join('..', 'crossover_files') # This is where the crossover files will be saved.

all_sats = {} #An empty dictionary to store multiple satellite data sources, each in its own nested dictionary.
all_sats['GSFC']  = {'path': 'GSFC', 'pattern': '*.nc', 'source': 'new_podaac', 'shortname': 'GSFC',
                     'lon_key': 'longitude', 'lat_key': 'latitude', 'ssh_key': 'ssh_smoothed', 'cycle_key': 'cycle', 'pass_key': 'pass', 'flag_key': 'nasa_flag'}
all_sats['CMEMS'] = {'path': 'CMEMS', 'pattern': '*.nc', 'source': 'CMEMS', 'shortname': 'CMEMS',
                     'lon_key': 'lon', 'lat_key': 'lat', 'ssh_key': 'sla_filtered', 'cycle_key': 'cycle',
                     'pass_key': 'track', 'flag_key': ''}

sat1 = all_sats['GSFC']
sat2 = all_sats['GSFC']

epoch = np.datetime64('1990-01-01T00:00:00.000000')

window_size = 10 # The number of days in the running window.
window_padding = 2 # The number of days to pad the running window so passes at the end of the first day have a full window_size of data to compare to.

#Only look for crossovers if the passes in question start at datetimes that differ by less than max_diff,
#but greater than 0 to avoid double-counting.
# cycle length in days
cycle_length = 9.9156
# Convert cycle_length to nanoseconds
# 1 day = 86,400,000,000,000 nanoseconds
cycle_ns = int(cycle_length * 86400000000000)
# Defining zero_diff and max_diff using numpy.timedelta64 with nanoseconds for maximum precision
zero_diff = np.timedelta64(0, 'ns')
max_diff  = np.timedelta64(cycle_ns, 'ns')

# max number of crossovers is ~100 per pass, 25 passes per day
max_crossovers_per_day=100*25

def logging_setup(basename):
    # Create a custom logger
    global logger
    logger = logging.getLogger("my_logger")
    logger.setLevel(logging.DEBUG)
    # Create a file handler
    global now
    now = np.datetime64('now', 's')
    # Convert numpy.datetime64 to a string in ISO 8601 format
    now_str = str(now)
    # Format: 'YYYYMMDD-HHMMSS'
    # Since the string is in ISO 8601 format, it looks like 'YYYY-MM-DDTHH:MM:SS'
    # We replace '-' and ':' with '', and 'T' with '-', to get the desired format
    now_str = now_str.replace('-', '').replace(':', '').replace('T', '-')
    # If you want to specifically format it to 'YYYYMMDD-HHMMSS' only
    now_str = now_str[:8] + '-' + now_str[9:15]
    log_base = basename+"-log-"+now_str
    log_info = log_base+".out"
    log_errors = log_base+".err"
    # Create a file handler for debug and info messages
    debug_info_handler = logging.FileHandler(log_info)
    debug_info_handler.setLevel(logging.DEBUG)
    # Create a file handler for warning, error, and critical messages
    warning_error_handler = logging.FileHandler(log_errors)
    warning_error_handler.setLevel(logging.WARNING)
    # Create a stream handler (console)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    # Set a log format
    log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    # Apply the format to all handlers
    debug_info_handler.setFormatter(log_format)
    warning_error_handler.setFormatter(log_format)
    console_handler.setFormatter(log_format)
    # Add the handlers to the logger
    logger.addHandler(debug_info_handler)
    logger.addHandler(warning_error_handler)
    logger.addHandler(console_handler)

logging_setup("log-xcoords")

#If sat1 and sat2 are the same, we're looking for "self-crossovers" between the same satellite.
#Are we looking for self crossovers or crossovers between multiple satellites?
if sat1['shortname'] == sat2['shortname']:
    self_crossovers = True
    logger.info(f"Looking for self-crossovers in {sat1['shortname'] = }")
else:
    self_crossovers = False
    logger.info(f"Looking for crossovers between {sat1['shortname'] = } and {sat2['shortname'] = }")

# Search for files and build a list of file dates for each satellite:
def build_file_list(these_sat_data: dict) -> None:
    '''
    Build a list of files from the satellite data path and pattern, then build a list of file dates.
    Doesn't return a value, just modifies the input dictionary in place.
    '''
    date_pattern = re.compile(r'\d{8}') # Regular expression to match the date in the format yyyymmdd
    these_sat_data['path'] = os.path.join(sat_data_path, these_sat_data['path'])
    glob_pattern = os.path.join(these_sat_data['path'], '**', these_sat_data['pattern']) # The '**' searches recursively through all subdirectories as long as recursive is set to True below.
    these_sat_data['files'] = sorted(glob.glob(glob_pattern, recursive=True))

    # Add a new key to the dictionary to store the dates in the filenames.
    file_dates = []
    for filename in these_sat_data['files']:
        # Extract date from filename
        match = date_pattern.search(filename)
        if match:
            # Convert the date string to a np.datetime64 object
            date_str = match.group()
            date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
            date_obj = np.datetime64(date_str)
            file_dates.append(date_obj)
    # Assign the list of np.datetime64 objects to the new key
    these_sat_data['file_dates'] = file_dates

build_file_list(sat1)
if self_crossovers:
    sat2 = copy.deepcopy(sat1)
else:
    build_file_list(sat2)

# Determine the earliest date when both satellites have data.
first_available_day = max(sat1['file_dates'][0], sat2['file_dates'][0])
last_available_day  = min(sat1['file_dates'][-1], sat2['file_dates'][-1])
if first_day < first_available_day:
    logger.info(f"Warning: the requested first_day is before the first available date in file_dates. Changing first_day from {first_day} to {first_available_day}.")
    first_day = first_available_day
if last_day > last_available_day:
    logger.info(f"Warning: the requested last_day is after the last available date in file_dates. Changing last_day from {last_day} to {last_available_day}.")
    last_day = last_available_day

# Initialize a dictionary that will store one day's worth of crossover data.
empty_crossover_data = {
    'time1'   : np.zeros(max_crossovers_per_day, dtype='datetime64[ns]'),
    'time2'   : np.zeros(max_crossovers_per_day, dtype='datetime64[ns]'),
    'lon'     : np.zeros(max_crossovers_per_day, dtype='float64'),
    'lat'     : np.zeros(max_crossovers_per_day, dtype='float64'),
    'ssh1'    : np.zeros(max_crossovers_per_day, dtype='float64'),
    'ssh2'    : np.zeros(max_crossovers_per_day, dtype='float64'),
    'cycle1'  : np.zeros(max_crossovers_per_day, dtype='int32'),
    'pass1'   : np.zeros(max_crossovers_per_day, dtype='int32'),
    'cycle2'  : np.zeros(max_crossovers_per_day, dtype='int32'),
    'pass2'   : np.zeros(max_crossovers_per_day, dtype='int32'),
}

def find_date_index(date: np.datetime64, file_dates: list) -> Union[int, None]:
    '''
    Find the index of a date in the file_dates list.
    '''
    try:
        return file_dates.index(date)
    except ValueError:
        # Date not found, construct and print an error message
        first_day_string = np.datetime_as_string(file_dates[ 0])
        last_day_string  = np.datetime_as_string(file_dates[-1])
        error_msg = f"Error: Date {np.datetime_as_string(date,unit='ns')} not found in file_dates array. Range is {first_day_string} to {last_day_string}."
        logger.error(error_msg)
        return None


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
            logger.error("Error: The file_dates list is empty.")
            return None
        else:
            # Calculate the absolute difference between the target date and each date in file_dates
            differences = np.abs(file_dates_np - date)
            # Find the index of the smallest difference
            closest_index = np.argmin(differences)
            # Optionally, print a message about the closest date found
            closest_date_str = np.datetime_as_string(file_dates_np[closest_index], unit='D')
            logger.info(f"Exact date not found. The closest date to {np.datetime_as_string(date, unit='D')} is {closest_date_str}.")
            return int(closest_index)

def init_and_fill_running_window(this_sat: dict, first_day_in_window: np.datetime64, last_day_in_window: np.datetime64) -> dict:
    '''
    Initialize and fill a running window with satellite data loaded from disk.
    '''
    this_window = {}
    # Find indices for first_day_in_window and last_day_in_window
    first_day_in_window_index = find_closest_date_index(first_day_in_window, this_sat['file_dates'])
    last_day_in_window_index  = find_closest_date_index(last_day_in_window,  this_sat['file_dates'])

    #Fill the running window with data from disk:
    sat_these_days = [] #Empty list to store the opened files.
    these_filenames = [] #Empty list to store the filenames.
    these_histories = [] #Empty list to store the history attributes.
    these_product_generation_steps = [] #Empty list to store the product_generation_step attributes.
    for dayind in range(first_day_in_window_index, last_day_in_window_index + 1):
        if this_sat['source'] == 'new_podaac':
            sat_thisday = xr.open_dataset(this_sat['files'][dayind], mask_and_scale=False)
        elif this_sat['source'] == 'CMEMS':
            sat_thisday = xr.open_dataset(this_sat['files'][dayind])
        else:
            raise ValueError(f"Invalid data source {this_sat['source'] = }")
        if sat_thisday.dims['time'] == 0: continue #Skip this file if it has no data.
        sat_these_days.append(sat_thisday)
        these_filenames.append(os.path.basename(this_sat['files'][dayind]))
        these_histories.append(sat_thisday.attrs['history'].split('Created on ')[1])
        these_product_generation_steps.append(sat_thisday.attrs['product_generation_step'])
        sat_thisday.close()
    sat_these_days = xr.concat(sat_these_days, dim='time')
    this_window['input_filenames'] = ', '.join(these_filenames)
    this_window['input_histories'] = ', '.join(these_histories)
    this_window['input_product_generation_steps'] = ', '.join(these_product_generation_steps)
    
    #Add this day's data (after filtering, standardizing key names, etc.) to the running window.
    #Filter out default values:
    ssh_fill_value = sat_these_days[this_sat['ssh_key']].attrs['_FillValue']
    if this_sat['flag_key'] != '' and this_sat['ssh_key'] != 'ssh_smoothed': #If set, filter out (UNsmoothed only!) data points with a NASA flag value of 1 (bad data).
        logger.info("!!! USING NASA FLAG TO FILTER DATA POINTS!!!")
        flag_filter = sat_these_days[this_sat['flag_key']].values == 0
    else: #Otherwise, don't filter out any data points based on the (unused here) NASA flag.
        flag_filter = np.ones(sat_these_days[this_sat['ssh_key']].shape, dtype=bool)
    #breakpoint()
    good_indices = np.where((sat_these_days[this_sat['ssh_key']].values != ssh_fill_value) & (flag_filter))[0]
    this_window['time']    = sat_these_days['time'][good_indices].values
    this_window['lon']     = sat_these_days[this_sat['lon_key']][good_indices].values.astype('float64')
    this_window['lat']     = sat_these_days[this_sat['lat_key']][good_indices].values.astype('float64')
    this_window['ssh']     = sat_these_days[this_sat['ssh_key']][good_indices].values.astype('float64')
    this_window['cycle']   = sat_these_days[this_sat['cycle_key']][good_indices].values
    this_window['pass']    = sat_these_days[this_sat['pass_key']][good_indices].values
    this_window['trackid'] = sat_these_days[this_sat['cycle_key']][good_indices].values.astype('int32')*10000 + \
                             sat_these_days[this_sat['pass_key']][good_indices].values

    #Find unique trackids, cycles and passes in the first satellite's data in this window.
    this_window['unique_trackid'] = np.unique(this_window['trackid'])
    trackid_start_times_list = [np.min(this_window['time'][this_window['trackid'] == this_unique_trackid]) for this_unique_trackid in this_window['unique_trackid']]
    this_window['trackid_start_times'] = np.array(trackid_start_times_list, dtype='datetime64[ns]')

    #Save the track_masks (indices) for each unique trackid.
    #Each unique trackid is a key in the track_masks dictionary, and the value is an array of indices.
    this_window['track_masks'] = {
        this_unique_trackid: np.where(this_window['trackid'] == this_unique_trackid)[0]
        for this_unique_trackid in this_window['unique_trackid']
    }

    return this_window

this_day = first_day
while this_day < last_day:
    logger.info(f"Processing {np.datetime_as_string(this_day, unit='D')}")
    next_day = this_day + np.timedelta64(1, 'D')
    #Initialize the dictionary of today's crossover data with empty arrays:
    crossover_data_for_today = copy.deepcopy(empty_crossover_data)
    num_crossovers_today = 0 #Reset counter used to keep track of how many crossovers we've found today.
    #Load data from first satellite. Only look forward in time.
    first_day_in_window = this_day
    last_day_in_window  = this_day + np.timedelta64(window_size + window_padding, 'D')
    if last_day_in_window > last_day:
        last_day_in_window = last_day
    if self_crossovers:
        window1 = init_and_fill_running_window(sat1, first_day_in_window, last_day_in_window)
    else:
        window1 = init_and_fill_running_window(sat1, first_day_in_window, first_day_in_window + np.timedelta64(1+window_padding, 'D'))
    #Load data from second satellite.
    #If we're looking for self crossovers, we can use the same window for "both" satellites.
    if self_crossovers:
        window2 = copy.deepcopy(window1)
    else:
        #Load data from second satellite. Look forward AND backward in time.
        first_day_in_window = this_day - np.timedelta64(window_size + window_padding, 'D')
        last_day_in_window  = this_day + np.timedelta64(window_size + window_padding, 'D')
        window2 = init_and_fill_running_window(sat2, first_day_in_window, last_day_in_window)
    # Loop through unique trackids in the first day of the running window for the first satellite, 
    # filtering to only include those where the start time is before the last time of the first day.
    trackid1_starting_today_indices = np.where(window1['trackid_start_times'] < next_day)[0]
    trackid1_starting_today = window1['unique_trackid'][trackid1_starting_today_indices]
    for ind1,trackid1 in enumerate(trackid1_starting_today):
        #logger.info(f"{trackid1 = }, start time: {np.datetime_as_string(window1['trackid_start_times'][ind1], unit='s')}")
        track_mask1 = window1['track_masks'][trackid1]
        time1 = (window1['time'][track_mask1] - epoch).astype('timedelta64[ns]').astype('float64')
        lonlat1 = np.column_stack((window1['lon'][track_mask1], window1['lat'][track_mask1]))
        ssh1 = window1['ssh'][track_mask1]
        #Look for trackids for the second satellite that might have crossovers with the current trackid1.
        #The criteria for this search are different for self-crossovers and crossovers between different satellites.
        if self_crossovers:
            # 1. Don't look for crossovers in the same or adjacent cycle/pass number.
            boolean1 = np.abs((trackid1 - window2['unique_trackid'])) > 1
            # 2. Only match ascending with descending passes and vice versa.
            boolean2 = (trackid1 % 2) != (window2['unique_trackid'] % 2)
            # 3. Only match passes within window length, but make sure the time difference is positive to avoid double-counting.
            boolean3 = (((window2['trackid_start_times'] - window1['trackid_start_times'][ind1]) <= max_diff) & ((window2['trackid_start_times'] - window1['trackid_start_times'][ind1]) > zero_diff))
        else:
            # 1. Look for crossovers in any cycle/pass numbers because they're different between satellites 1 and 2.
            boolean1 = True
            # 2. Match ascending passes with descending AND ascending passes
            boolean2 = True
            # 3. Only match passes within a half cycle. Don't worry if the time difference is negative, but it can't be more negative than a half cycle.
            boolean3 = (((window2['trackid_start_times'] - window1['trackid_start_times'][ind1]) <= max_diff) & ((window2['trackid_start_times'] - window1['trackid_start_times'][ind1]) > -max_diff))
        trackid2_possible_crossovers = window2['unique_trackid'][np.where(boolean1 & boolean2 & boolean3)[0]]

        #Now loop through the trackids for the second satellite that might have crossovers with the current trackid1.
        for trackid2 in trackid2_possible_crossovers:
            #logger.info(f"  {trackid2 = }, start time: {np.datetime_as_string(window2['trackid_start_times'][trackid2], unit='s')}")
            track_mask2 = window2['track_masks'][trackid2]
            time2 = (window2['time'][track_mask2] - epoch).astype('timedelta64[ns]').astype('float64')
            lonlat2 = np.column_stack((window2['lon'][track_mask2], window2['lat'][track_mask2]))
            ssh2 = window2['ssh'][track_mask2]
            #Use the xover_ssh function to compute crossovers.
            if len(time1) > 1 and len(time2) > 1:

                xcoords, xssh, xtime = xover_ssh(lonlat1, lonlat2, ssh1, ssh2, time1, time2)
                #Keep valid crossover points:
                if np.size(xcoords)!=0:
                    if np.size(xcoords) > 2: logger.error(f"\n\n*********\n!!!!WARNING!!! {np.size(xcoords) = }\n********\n")
                    crossover_data_for_today['ssh1']  [num_crossovers_today] = xssh[0]
                    crossover_data_for_today['ssh2']  [num_crossovers_today] = xssh[1]
                    crossover_data_for_today['time1'] [num_crossovers_today] = epoch + np.timedelta64(int(xtime[0]), 'ns')                    
                    crossover_data_for_today['time2'] [num_crossovers_today] = epoch + np.timedelta64(int(xtime[1]), 'ns')
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
    dimension_name = 'time1'

    ds = xr.Dataset(
        {
            "time2": (dimension_name, crossover_data_for_today['time2'], {
                "long_name": "Time of crossover in later pass"
            }),
            "lon": (dimension_name, crossover_data_for_today['lon'], {
                "units": "degrees", 
                "long_name": "Crossover longitude"
            }),
            "lat": (dimension_name, crossover_data_for_today['lat'], {
                "units": "degrees", 
                "long_name": "Crossover latitude"
            }),
            "ssh1": (dimension_name, crossover_data_for_today['ssh1'], {
                "units": "cm", 
                "long_name": "SSH at crossover in earlier pass"
            }),
            "ssh2": (dimension_name, crossover_data_for_today['ssh2'], {
                "units": "cm", 
                "long_name": "SSH at crossover in later pass"
            }),
            "cycle1": (dimension_name, crossover_data_for_today['cycle1'], {
                "units": "N/A", 
                "long_name": "Cycle number of earlier pass"
            }),
            "cycle2": (dimension_name, crossover_data_for_today['cycle2'], {
                "units": "N/A", 
                "long_name": "Cycle number of later pass"
            }),
            "pass1": (dimension_name, crossover_data_for_today['pass1'], {
                "units": "N/A", 
                "long_name": "Pass number of earlier pass"
            }),
            "pass2": (dimension_name, crossover_data_for_today['pass2'], {
                "units": "N/A", 
                "long_name": "Pass number of later pass"
            }),
        },
        coords={
            dimension_name: (dimension_name, crossover_data_for_today[dimension_name], {
                "long_name": "Time of crossover in earlier pass"
            })
        }
    )

    # Add global attributes
    if self_crossovers:
        ds.attrs['title'] = sat1['shortname'] + ' crossovers with ' + sat2['shortname']
    else:
        ds.attrs['title'] = sat1['shortname'] + ' self-crossovers'
    ds.attrs['subtitle'] = f"within {window_size} days"

    # convert "last_day_in_window - first_day_in_window" from numpy.timedelta64 to int:
    ds.attrs['window_length'] = f"{(last_day_in_window - first_day_in_window).astype('int32')} days (nominal: {window_size} days + {window_padding} days padding)"

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
    if self_crossovers:
        ds.attrs['input_filenames'] = window1['input_filenames']
        ds.attrs['input_histories'] = window1['input_histories']
        ds.attrs['input_product_generation_steps'] = window1['input_product_generation_steps']
        ds.attrs['satellite_names'] = sat1['shortname']
    else:
        ds.attrs['input_filenames'] = window1['input_filenames'] + ', ' + window2['input_filenames']
        ds.attrs['input_histories'] = window1['input_histories'] + ', ' + window2['input_histories']
        ds.attrs['input_product_generation_steps'] = window1['input_product_generation_steps'] + ', ' + window2['input_product_generation_steps']
        ds.attrs['satellite_names'] = sat1['shortname'] + ', ' + sat2['shortname']

    # Set the units attribute on both time variables so it references the epoch correctly.
    # Check if 'units' attribute exists and remove it
    if 'units' in ds['time1'].attrs:
        del ds['time1'].attrs['units']
    if 'units' in ds['time2'].attrs:
        del ds['time2'].attrs['units']
    ds['time1'].encoding['units'] = f'seconds since {epoch}'
    ds['time2'].encoding['units'] = f'seconds since {epoch}'

    # Save to netCDF file
    filename = 'xovers_' + ds.attrs['satellite_names'].replace(', ','_') + '-' + np.datetime_as_string(this_day) + '.nc'
    ds.to_netcdf(os.path.join(crossover_files_path,filename))
    
    #Move to the next day.
    this_day = next_day

end_time = time.time()
elapsed_time = end_time - start_time
logger.info(f"The script took {elapsed_time} seconds to run.")