import pandas as pd
import xarray as xr
import numpy as np
import os, glob
import datetime, time
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import statsmodels.api as sm
import logging
from tqdm import tqdm
import multiprocessing as mp
from typing import Union, Optional, Dict, Any, Tuple

def logging_setup(basename: str) -> None:
    """
    Set up logging to write to a file and the console.
    Args:
        basename (str): The base name for the log files. The current date/time will be appended to this name.
    Returns:
        None
    """
    # Create a custom logger
    global logger
    logger = logging.getLogger("my_logger")
    logger.setLevel(logging.DEBUG)
    # Create a file handler
    global now
    now = datetime.datetime.now()
    log_base = basename+"-log-"+now.strftime('%Y%m%d-%H%M%S')
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

def my_capitalize(string_to_capitalize: str) -> str:
    """
    Capitalize ONLY the first letter of a string and DON'T modify the rest of it.
    """
    return string_to_capitalize[0].upper() + string_to_capitalize[1:]

def insistently_create(the_path):
    """
    Creates a directory at the specified path if it does not already exist.
    If the path exists and is a file, raises an Exception.

    Args:
        the_path (str): The path where the directory will be created.

    Raises:
        Exception: If the path exists and is a file.
    """
    # Remove the trailing '/' if present
    the_path = the_path.rstrip('/')
    
    # Check if the path exists
    if os.path.exists(the_path):
        # If it exists but is a file, raise an exception
        if os.path.isfile(the_path):
            raise Exception(f"'{the_path}' exists but is a file, not a directory.")
    else:
        # If the directory doesn't exist, create it
        print(f"Creating output directory '{the_path}' because it doesn't exist.")
        os.makedirs(the_path)

def prettyprint_timespan(timespan: float) -> None:
    """
    Pretty-prints a timespan in years, weeks, days, hours, and seconds.
    """
    # Constants for time conversions
    SECONDS_PER_MINUTE = 60
    SECONDS_PER_HOUR = 3600
    SECONDS_PER_DAY = 86400
    SECONDS_PER_WEEK = 604800
    SECONDS_PER_YEAR = 31556952

    # Calculating years, weeks, days, hours
    years = int(timespan // SECONDS_PER_YEAR)
    remaining = timespan % SECONDS_PER_YEAR

    weeks = int(remaining // SECONDS_PER_WEEK)
    remaining = remaining % SECONDS_PER_WEEK

    days = int(remaining // SECONDS_PER_DAY)
    remaining = remaining % SECONDS_PER_DAY

    hours = remaining / SECONDS_PER_HOUR
    remaining = remaining % SECONDS_PER_HOUR

    # Creating a list of time components
    components = []
    if years > 0:
        components.append(f"{years} years")
    if weeks > 0:
        components.append(f"{weeks} weeks")
    if days > 0:
        components.append(f"{days} days")
    if hours > 0:
        components.append(f"{hours:.1f} hours")
    else:
        components.append(f"{remaining:.3f} seconds")
    
    # Joining the components with commas, and "and" for the last component
    if len(components) > 1:
        time_str = ", ".join(components[:-1]) + " and " + components[-1]
    elif components:
        time_str = components[0]
    else:
        time_str = "0 seconds"

    print(f"The script took {time_str} to run.")

def find_gaps(x: np.ndarray, gap_threshold: Union[int, float, datetime.timedelta]) -> np.ndarray:
    """
    Find gaps in the data based on a threshold.
    Returns a list of indices where the gaps start.
    """
    gaps = np.diff(x) > gap_threshold
    gap_indices = np.where(gaps)[0]
    return gap_indices

def custom_formatter(val: Union[int, float], pos: int = None) -> str:
    """
    Custom formatter for the x-axis.
    Returns a string formatted as '83-602'.
    """
    return f'{int(val)//10000}-{int(val)%10000:03}'

def plot_groups(df: pd.DataFrame, ds: xr.Dataset, x_key: str, y_key: str, the_function: str, options: Dict[str, Any]) -> None:
    """
    Plot data with breaks in the x-axis for gaps larger than the threshold
    and custom formatting for x-axis values.
    """
    xrot = 90 #Degrees to rotate labels on the x-axis

    make_breaks = False
    if make_breaks:
        diag_length = .015
        plot_sep = 0.05
    else:
        diag_length = 0.0
        plot_sep = 0.0
    
    envelope_width = 5

    use_custom_formatter = False
    if options['key_units'][x_key] == 'days':
        the_groups = df.groupby(df[x_key].dt.date)
        envelope_width = 2
    elif options['key_units'][x_key] == 'track_id1':
        the_groups = df.groupby(df['track_id1'])
        use_custom_formatter = True
    elif options['key_units'][x_key] == 'track_id2':
        the_groups = df.groupby(df['track_id2'])
        use_custom_formatter = True
    else:
        raise ValueError(f"Unsupported {options['key_units'][x_key] = }")

    num_bins = len(the_groups)
    print(f"Number of {options['key_units'][x_key]} bins: {num_bins}")

    if the_function == 'mean':
        my_func = np.mean
        my_format = ".4e"
    elif the_function == 'RMS':
        my_func = lambda x: np.sqrt(np.mean(np.square(x)))
        my_format = ".3f"
    else:
        raise ValueError("Unsupported function type")

    # Calculate the function over all the data.
    thevalue = my_func(df[y_key])
    print(f"{my_capitalize(the_function)} of crossover {ds[y_key].attrs['long_name']}: {thevalue:{my_format}} {ds[y_key].attrs['units']}")

    the_groups = the_groups[y_key].apply(my_func)

    x = the_groups.index.to_numpy()
    y = the_groups.values

    # Calculate mean and standard deviation of 'y'
    mean_y = np.mean(y)
    std_y = np.std(y)

    # Define the envelope
    lower_bound = mean_y - envelope_width * std_y
    upper_bound = mean_y + envelope_width * std_y

    # Find indices of 'y' values outside the envelope
    outlier_indices = np.where((y < lower_bound) | (y > upper_bound))

    # Create new arrays for 'bad_x' and 'bad_y'
    bad_x = x[outlier_indices]
    bad_y = y[outlier_indices]

    # Remove these values from the original 'x' and 'y' (commented out because otherwise the y-axis limits are wrong)
    #x = np.delete(x, outlier_indices)
    #y = np.delete(y, outlier_indices)

    for i in range(len(x)):
        if use_custom_formatter:
            this_x = custom_formatter(x[i],0)
        else:
            this_x = x[i]
        print(f"{this_x} : {the_function} of {options['all_sat_names']} {x_key} = {y[i]:+.4}")
    
    if len(bad_x) > 0:
        print(f"!!!!!!!!!!!!! {len(bad_x)} outliers found:")
        print(f"mean_y = {mean_y:.4f}")
        print(f"std_y = {std_y:.4f}")
        print(f"lower_bound = {lower_bound:.4f}")
        print(f"upper_bound = {upper_bound:.4f}")

    for i in range(len(bad_x)):
        if use_custom_formatter:
            this_x = custom_formatter(bad_x[i],0)
        else:
            this_x = bad_x[i]
        print(f"{this_x} : {the_function} of {options['all_sat_names']} {x_key} = {bad_y[i]:+.4}")
    
    # Define the gap threshold
    if isinstance(x[0], (int, float, np.int32, np.float64)):
        gap_threshold = 1000  # This threshold will depend on your specific data
    elif isinstance(x[0],datetime.date):
        gap_threshold = datetime.timedelta(days=100)
    else:
        raise ValueError(f"Unsupported x-axis type: {type(x[0]).__name__}. If this type is appropriate, you can add it to the isinstance check.")

    start_idx = 0
    num_bins = len(x)
    gap_indices = find_gaps(x, gap_threshold)
    plt.rcParams.update({"font.size": options['fsize']})
    fig, axs = plt.subplots(1, len(gap_indices) + 1, sharey=True, figsize=options['myfigsize'])

    x_min = min(x)
    x_max = max(x)
    y_min = min(y)
    y_max = max(y)
    the_range = y_max - y_min
    new_y_position  = y_max  + 1*options['blurb_offset'] * (the_range)
    new_y_limit_max = y_max  + 2*options['blurb_offset'] * (the_range)
    new_y_limit_min = y_min  - 2*options['blurb_offset'] * (the_range)
    
    # If there's only one plot, axs will not be an array
    if not isinstance(axs, np.ndarray):
        axs = np.array([axs])
    
    # Set the y-axis limits for all subplots
    for ax in axs:
        ax.set_ylim(new_y_limit_min, new_y_limit_max)

    # Adjust subplot parameters to create more space on the right
    plt.subplots_adjust(right=0.8)  # Adjust this value as needed

    for i, end_idx in enumerate(gap_indices):
        ax = axs[i]
        ax.scatter(x[start_idx:end_idx + 1], y[start_idx:end_idx + 1], 
           marker='o', linestyle='', color='b', edgecolors='black', 
           linewidths=0.5, alpha=0.4)
        
        # Define the current x-axis range
        current_x_range = x[start_idx:end_idx + 1]
        # Filter bad_x and bad_y points that fall within the current x-axis range
        current_bad_x = bad_x[np.logical_and(bad_x >= current_x_range.min(), bad_x <= current_x_range.max())]
        current_bad_y = bad_y[np.logical_and(bad_x >= current_x_range.min(), bad_x <= current_x_range.max())]
        # Plot bad points as red squares
        ax.scatter(current_bad_x, current_bad_y, marker='s', linestyle='', color='r', s=50,
                   edgecolors='black', linewidths=0.5, alpha=1.0)
        
        if upper_bound <= y_max: ax.axhline(y=upper_bound, color='grey', linestyle='--')
        ax.axhline(y=mean_y, color='grey', linestyle='dotted')
        if lower_bound >= y_min: ax.axhline(y=lower_bound, color='grey', linestyle='--')
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(i == 0)  # Only show the left spine for the first plot
        ax.tick_params(axis='x', rotation=xrot)
        if use_custom_formatter: ax.xaxis.set_major_formatter(plt.FuncFormatter(custom_formatter))
        ax.tick_params(axis='y', which='both', left=(i == 0))  # Only show y-axis ticks on the first plot

        start_idx = end_idx + 1

    # Plot the last segment
    ax = axs[-1]
    ax.scatter(x[start_idx:], y[start_idx:],
               marker='o', linestyle='', color='b', edgecolors='black', 
               linewidths=0.5, alpha=0.4)
    
    # Define the current x-axis range
    current_x_range = x[start_idx:]
    # Filter bad_x and bad_y points that fall within the current x-axis range
    current_bad_x = bad_x[np.logical_and(bad_x >= current_x_range.min(), bad_x <= current_x_range.max())]
    current_bad_y = bad_y[np.logical_and(bad_x >= current_x_range.min(), bad_x <= current_x_range.max())]
    # Plot bad points as red squares
    ax.scatter(current_bad_x, current_bad_y, marker='s', linestyle='', color='r', s=50,
               edgecolors='black', linewidths=0.5, alpha=1.0)
    #for j in range(len(current_bad_x)):
    #    ax.text(current_bad_x[j] + 0.02, current_bad_y[j], 
    #            f"bad: {current_bad_x[j]}", ha='left', clip_on=True)
    
    if upper_bound <= y_max:
        ax.axhline(y=upper_bound, color='grey', linestyle='--')
        ax.text(1.02, upper_bound, f"{upper_bound:+.1f} (+{envelope_width} sd)", transform=ax.get_yaxis_transform(), 
        verticalalignment='center', horizontalalignment='left')
    ax.axhline(y=mean_y, color='grey', linestyle='dotted')
    ax.text(1.02, mean_y, f"{mean_y:+.1f} (mean)", transform=ax.get_yaxis_transform(), 
        verticalalignment='center', horizontalalignment='left')
    if lower_bound >= y_min:
        ax.axhline(y=lower_bound, color='grey', linestyle='--')
        ax.text(1.02, lower_bound, f"{lower_bound:+.1f} (-{envelope_width} sd)", transform=ax.get_yaxis_transform(), 
                verticalalignment='center', horizontalalignment='left')

    ax.spines['left'].set_visible(False)
    if use_custom_formatter: ax.xaxis.set_major_formatter(plt.FuncFormatter(custom_formatter))
    ax.tick_params(axis='x', rotation=xrot)
    ax.tick_params(axis='y', left=False)  # Hide y-axis ticks
    axs[0].spines['left'].set_visible(True)

    # Add a text annotation near the top left inside the grid axes
    fig.text(0.14, 0.86, f"{my_capitalize(the_function)} over all {num_bins} {options['key_units'][x_key]} = {thevalue:{my_format}} {ds[y_key].attrs['units']}", fontsize=options['fsize'], verticalalignment='top')

    # Adding diagonal lines to indicate breaks
    d = diag_length # Diagonal line size
    
    for ax in axs[:-1]:
        ax.plot((1 - d, 1 + d), (-d, +d), transform=ax.transAxes, color='k', clip_on=False)
        ax.plot((1 - d, 1 + d), (1 - d, 1 + d), transform=ax.transAxes, color='k', clip_on=False)
    for ax in axs[1:]:
        ax.plot((-d, +d), (-d, +d), transform=ax.transAxes, color='k', clip_on=False)
        ax.plot((-d, +d), (1 - d, 1 + d), transform=ax.transAxes, color='k', clip_on=False)

    # Adjust layout
    plt.subplots_adjust(wspace=plot_sep, bottom=0.3)
    #plt.tight_layout(pad=3, h_pad=plot_sep, w_pad=plot_sep) #This puts extra space between the subplots for some reason!
    #X-Axis Title:
    fig.text(0.5, 0.03, f'{x_key}', ha='center', va='center', fontsize=options['fsize'])
    #Y-Axis Title:
    fig.text(0.04, 0.5, f"{my_capitalize(the_function)} of crossover {ds[y_key].attrs['long_name']} ({ds[y_key].attrs['units']})", ha='center', va='center', rotation='vertical', fontsize=options['fsize'])
    plt.suptitle(f"{my_capitalize(options['units_desc'][options['key_units'][x_key]])} {the_function} of {options['all_sat_names']} crossover {ds[y_key].attrs['long_name']}")
    output_filename = f"{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}-{options['all_sat_names'].replace(', ','_')}-{options['units_desc'][options['key_units'][x_key]]}-{the_function}-plot.png"
    plt.savefig(os.path.join(options['outputpath'],output_filename), dpi=options['dpi_choice'], bbox_inches='tight', facecolor='w', transparent=False)
    # Display plot
    #plt.show()

def make_histogram(df: pd.DataFrame, ds: xr.Dataset, y_key: str, bin_size: float, options: Dict[str, Any]) -> None:
    """
    Create a histogram of a variable.
    """
    bin_edges = np.arange(df[y_key].min(), df[y_key].max() + bin_size, bin_size)
    plt.rcParams.update({"font.size": options['fsize']})
    fig, ax = plt.subplots(figsize=options['myfigsize'])
    plt.hist(df[y_key], bins=bin_edges)
    plt.xlabel(f"{ds[y_key].attrs['long_name']} ({ds[y_key].attrs['units']})")
    plt.ylabel("Frequency")
    plt.title(f"Histogram of {options['all_sat_names']} crossover {ds[y_key].attrs['long_name']}")
    output_filename = f"{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}-{options['all_sat_names'].replace(', ','_')}-{y_key}-histogram.png"
    plt.savefig(os.path.join(options['outputpath'],output_filename), dpi=options['dpi_choice'], bbox_inches='tight', facecolor='w', transparent=False)
    #plt.show()

def make_plot(df: pd.DataFrame, ds: xr.Dataset, x_key: str, y_key: str, options: Dict[str, Any]) -> None:
    """
    Create a scatter plot of two variables.
    """
    x_name  = ds[x_key].attrs['long_name']
    y_name  = ds[y_key].attrs['long_name']
    x_units = ds[x_key].attrs['units']
    y_units = ds[y_key].attrs['units']

    # Create the plot
    plt.rcParams.update({"font.size": options['fsize']})
    fig, ax = plt.subplots(figsize=options['myfigsize'])
    #plt.plot(df[x_key], df[y_key], marker='o', linestyle='', color='b')
    plt.plot(df[x_key], df[y_key], marker='o', linestyle='', color='b', markeredgecolor='black', markeredgewidth=0.5, alpha=0.4)
    plt.xlabel(f"{x_name} ({x_units})")
    plt.ylabel(f"{y_name} ({y_units})")
    plt.title(f"{options['all_sat_names']} crossover {y_name} vs. {x_name}")
    output_filename = f"{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}-{options['all_sat_names'].replace(', ','_')}-{x_key}-vs-{y_key}-plot.png"
    plt.savefig(os.path.join(options['outputpath'],output_filename), dpi=options['dpi_choice'], bbox_inches='tight', facecolor='w', transparent=False)
    #plt.show()

def make_map(df: pd.DataFrame, ds: xr.Dataset, options: Dict[str, Any]) -> None:
    """
    Create a map plot of the SSH differences.
    """
    # Extract data from the dataset
    lons = df['lon']
    lats = df['lat']
    ssh_diff = df['ssh_diff']

    # Create the map using the PlateCarree projection
    plt.rcParams.update({"font.size": options['fsize']})
    fig, ax = plt.subplots(figsize=options['myfigsize'], subplot_kw={'projection': ccrs.PlateCarree()})
    ax.coastlines()
    
    # Add ocean and land features with custom colors
    ax.add_feature(cfeature.OCEAN, facecolor='lightgray')  # Ocean feature
    ax.add_feature(cfeature.LAND, facecolor='gray')  # Land feature
    
    # Make the colorscale symmetric by looking for the largest absolute value.
    max_val = 20#max(abs(ssh_diff.min()), abs(ssh_diff.max()))

    # Plot ssh_diff values using scatter plot
    scatter = ax.scatter(lons, lats, c=ssh_diff, cmap='bwr', s=5,
                         transform=ccrs.PlateCarree(), vmin=-max_val, vmax=max_val)
    #                    edgecolors='black', linewidths=0.2, alpha=0.4)

    # Add colorbar on the left side of the plot
    cbar = plt.colorbar(scatter, ax=ax, label=f"{ds['ssh_diff'].attrs['long_name']} ({ds['ssh_diff'].attrs['units']})", orientation='vertical', shrink=0.7)
    
    # Set the title of the map
    plt.title(f"{options['num_crossovers']} {options['all_sat_names']} crossover {ds['ssh_diff'].attrs['long_name']}s")

    output_filename = f"{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}-{options['all_sat_names'].replace(', ','_')}-ssh_diff-map.png"
    plt.savefig(os.path.join(options['outputpath'],output_filename), dpi=options['dpi_choice'], bbox_inches='tight', facecolor='w', transparent=False)

    # Show the map
    #plt.show()

def is_valid_dataset(file_path: str) -> bool:
    """
    Check if the dataset is valid by opening it and checking if any of its dimensions have a size of 0.
    Returns True if the dataset is valid, False otherwise.
    """
    try:
        ds = xr.open_dataset(file_path)
        return not any(size == 0 for size in ds.dims.values())
    except Exception as e:
        print(f"Error checking dataset {file_path}: {e}")
        return False

def analyze_cycle_data(df: pd.DataFrame, cycle_number: int) -> Tuple[float, float, int]:
    """
    Analyze the data for a given cycle number and return the mean and RMS of ssh_diff, and the number of crossover points.
    """
    # Filter the DataFrame for the given cycle number in both cycle1 and cycle2
    cycle_df = df[(df['cycle1'] == cycle_number) & (df['cycle2'] == cycle_number)]
    
    # Calculate the mean and RMS of ssh_diff for the filtered data
    mean_ssh_diff = cycle_df['ssh_diff'].mean()
    rms_ssh_diff = np.sqrt(np.mean(np.square(cycle_df['ssh_diff'])))
    
    # Count the number of crossover points
    num_crossover_points = len(cycle_df)
    
    return mean_ssh_diff, rms_ssh_diff, num_crossover_points

def make_cycle_plots(df: pd.DataFrame, options: Dict[str, Any]) -> None:
    """
    Analyze the data for each cycle and plot the results.

    Parameters:
    - df (pd.DataFrame): The input DataFrame containing the data.
    - options (Dict[str, Any]): A dictionary containing the options for the analysis and plotting.

    Returns:
    None
    """
    # List to hold cycle numbers (assuming cycle numbers are consecutive and start from 1)
    cycle_numbers = df['cycle1'].unique()
    cycle_numbers.sort()  # Ensure cycle numbers are sorted

    # Lists to store the results
    mean_ssh_diffs = []
    rms_ssh_diffs = []
    num_crossover_points_list = []

    # Analyze data for each cycle
    for cycle in tqdm(cycle_numbers):
        mean_ssh_diff, rms_ssh_diff, num_crossover_points = analyze_cycle_data(df, cycle)
        mean_ssh_diffs.append(mean_ssh_diff)
        rms_ssh_diffs.append(rms_ssh_diff)
        num_crossover_points_list.append(num_crossover_points)
        print(f"Cycle {cycle}: Mean {options['all_sat_names']} SSH diff = {mean_ssh_diff:.4f}, RMS SSH diff = {rms_ssh_diff:.4f}, Number of crossover points = {num_crossover_points}")

    # Plotting the results
    fig, axs = plt.subplots(3, 1, figsize=(16, 18))
    cycle_label = 'cycle number'

    # Mean SSH difference plot
    axs[0].plot(cycle_numbers, mean_ssh_diffs, marker='o')
    axs[0].set_title(f"{options['all_sat_names']} mean SSH difference by cycle")
    axs[0].set_xlabel(cycle_label)
    axs[0].set_ylabel("mean SSH difference (m)")

    # RMS SSH difference plot
    axs[1].plot(cycle_numbers, rms_ssh_diffs, marker='o', color='r')
    axs[1].set_title(f"{options['all_sat_names']} RMS SSH difference by cycle")
    axs[1].set_xlabel(cycle_label)
    axs[1].set_ylabel("RMS SSH difference (m)")

    # Number of crossover points plot
    axs[2].bar(cycle_numbers, num_crossover_points_list, color='g')
    axs[2].set_title(f"{options['all_sat_names']} number of crossover points by cycle")
    axs[2].set_xlabel(cycle_label)
    axs[2].set_ylabel("number of crossover points")

    plt.tight_layout()
    output_filename = f"{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}-{options['all_sat_names'].replace(', ','_')}-cycle-analysis.png"
    plt.savefig(os.path.join(options['outputpath'], output_filename), dpi=options['dpi_choice'], bbox_inches='tight', facecolor='w', transparent=False)
    #plt.show()

def execute_plot(df: pd.DataFrame, ds: xr.Dataset, plot_to_make: str, options: Dict[str, Any]) -> None:
    """
    Execute the specified plotting function.
    """
    if plot_to_make == 'plot_groups':
        options['blurb_offset'] = 0.1
        options['key_units'] = {}
        options['key_units']['time1'] = 'days'
        options['key_units']['track_id1'] = 'track_id1'
        options['key_units']['track_id2'] = 'track_id2'
        options['key_units']['ssh_diff'] = 'm'
        options['units_desc'] = {}
        options['units_desc']['days'] = 'daily'
        options['units_desc']['track_id1'] = 'track ID'
        options['units_desc']['track_id2'] = 'track ID'
        plot_groups(df,ds,'time1',    'ssh_diff','mean',options)
        plot_groups(df,ds,'time1',    'ssh_diff','RMS', options)
        plot_groups(df,ds,'track_id1','ssh_diff','mean',options)
        plot_groups(df,ds,'track_id2','ssh_diff','mean',options)
        plot_groups(df,ds,'track_id1','ssh_diff','RMS', options)
        plot_groups(df,ds,'track_id2','ssh_diff','RMS', options)
    elif plot_to_make == 'plot_histogram':
        hist_keys = ['time_diff','ssh_diff']
        options['bin_size'] = 1.0
        for hist_key in hist_keys:
            make_histogram(df,ds,hist_key,options)
    elif plot_to_make == 'plot_pairs':
        key_pairs = []
        key_pairs.append(['time_diff','ssh_diff'])
        key_pairs.append(['lat','ssh_diff'])
        key_pairs.append(['lon','ssh_diff'])
        for key_pair in key_pairs:
            make_plot(df,ds,key_pair[0],key_pair[1],options['all_sat_names'],options['outputpath'])
    elif plot_to_make == 'plot_map':
        make_map(df,ds,len(df),options['all_sat_names'],options['outputpath'])
    elif plot_to_make == 'plot_cycle_analysis':
        make_cycle_plots(df, options['outputpath'], options['all_sat_names'])

def main():
    start_time = time.time()
    logging_setup("log-map-xcoords")

    options = {}
    options['myfigsize'] = (16,9)
    options['fsize'] = 24
    options['dpi_choice'] = 300
    options['running_local'] = 1

    print("IN THIS PLOTTING SCRIPT: CALC THE RMS FOR EACH DAILY CROSSOVER FILE, THEN DO PASS BY PASS ANALYSIS: FIND ALL THE CROSSOVERS FOR EACH PASS AND NOTE THSI REQUIRES LOADING TEN PREVIOUS CROSSOVER DAILY FILES: *COMBINE* TRACKID1 AND TRACKID2 INTO ONE PLOT- SWITCH SIGNS OF SSH1-SSH2 AND TIME1-TIME2 BASED ON IF WE'RE USING TRACKID1 OR TRACKID2")

    options['big_choice'] = 'S6'
    #options['big_choice'] = 'GSFC'
    #options['big_choice'] = 'CMEMS'

    if options['running_local']:
        options['num_processes'] = min(1,mp.cpu_count())
        print(f"Running locally with {options['num_processes'] = }")
        options['inputpath'] = os.path.join('..','crossover_files',options['big_choice'])
    else:
        options['num_processes'] = min(20,mp.cpu_count())
        print(f"Running on PDM with {options['num_processes'] = }")
        options['inputpath'] = os.path.join('..','crossover_files',options['big_choice'])

    options['outputpath'] = os.path.join('..','output_from_crossovers',options['big_choice'])
    insistently_create(options['outputpath'])

    # Get a list of all the filenames in the input directory
    glob_pattern = os.path.join(options['inputpath'], '**', '*.nc') # The '**' searches recursively through all subdirectories as long as recursive is set to True below.
    filenames = sorted(glob.glob(glob_pattern, recursive=True))
    filenames = filenames[:2]
    #print(f"{filenames = }")

    # Filter the list of file paths, keeping only those that lead to valid datasets
    print(f"Looking for non-empty files among {len(filenames)} files from {options['inputpath']}")
    valid_file_paths = [f for f in filenames if is_valid_dataset(f)]

    # Now, open and combine the valid datasets
    print(f"Loading {len(valid_file_paths)} non-empty files from {options['inputpath']}")

    ds = xr.open_mfdataset(valid_file_paths, combine='by_coords')

    print(f"Finished loading {len(valid_file_paths)} files.")

    # Reconstruct the original satellite strings list and the concatenated string.
    if ', ' not in ds.attrs['satellite_names']:
        self_crossovers = True
    else:
        self_crossovers = False
    options['all_sat_names'] = ds.attrs['satellite_names']

    ds['time_diff'] = ds['time2'] - ds['time1']
    ds['time_diff'].attrs['long_name'] = "time difference"

    ds['ssh_diff']  = ds['ssh2']  - ds['ssh1']
    ds['ssh_diff'].attrs['long_name'] = "SSH difference"
    ds['ssh_diff'].attrs['units'] = ds['ssh1'].attrs['units']

    ds['track_id1'] = ds['cycle1'] * 10000 + ds['pass1']
    ds['track_id2'] = ds['cycle2'] * 10000 + ds['pass2']

    # Change units to days.
    attrs = ds['time_diff'].attrs
    ds['time_diff'] = ds['time_diff'].astype('float64') / (24.0*3600.0*1.0e9)
    ds['time_diff'].attrs = attrs
    ds['time_diff'].attrs['units'] = 'days'

    ds['lon'].attrs['long_name'] = ds['lon'].attrs['long_name'].split('Crossover ')[1]
    ds['lat'].attrs['long_name'] = ds['lat'].attrs['long_name'].split('Crossover ')[1]

    # Convert the xarray Dataset to a pandas DataFrame
    df = ds.to_dataframe()
    # Reset the index to turn 'time1' into a column
    df = df.reset_index()

    #Delete data points that fall outside of desired timespan:
    if 0:
        start_date = pd.Timestamp('2023-10-01')
        df = df[df['time1'] >= start_date]
        df.reset_index(drop=True, inplace=True)

    options['num_crossovers'] = len(df['ssh_diff'])
    print(f"Loaded {options['num_crossovers']} {options['all_sat_names']} crossover points.")

    plots_to_make = []
    plots_to_make.append('plot_groups')
    plots_to_make.append('plot_histogram')
    plots_to_make.append('plot_pairs')
    plots_to_make.append('plot_map')
    plots_to_make.append('plot_cycle_analysis')

    # Create a list of argument tuples for each plotting choice, then run execute_plot in parallel.
    args_list = [(df,ds,plot_to_make,options) for plot_to_make in plots_to_make]
    with mp.Pool(processes=options['num_processes']) as pool:
        pool.starmap(execute_plot, args_list)
    
    end_time = time.time()
    timespan = end_time - start_time
    print(f"The script took {timespan:.3f} seconds to run.")

    prettyprint_timespan(timespan)

if __name__ == '__main__':
    main()