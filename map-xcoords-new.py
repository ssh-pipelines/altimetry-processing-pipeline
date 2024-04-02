import pandas as pd
import xarray as xr
import numpy as np
import os
import datetime
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import statsmodels.api as sm
import logging
from tqdm import tqdm

myfigsize=(16,9)
fsize = 24
dpi_choice = 300

def logging_setup(basename: str) -> None:
  # Create a custom logger
  global logger
  logger = logging.getLogger("my_logger")
  logger.setLevel(logging.DEBUG)
  # Create a file handler
  global now
  now = datetime.datetime.now()
  log_base = basename+"-log-"+now.strftime("%Y%m%d-%H%M%S")
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
    return string_to_capitalize[0].upper() + string_to_capitalize[1:]

def find_gaps(x, gap_threshold) -> np.ndarray:
    """
    Find gaps in the data based on a threshold.
    Returns a list of indices where the gaps start.
    """
    gaps = np.diff(x) > gap_threshold
    gap_indices = np.where(gaps)[0]
    return gap_indices

def custom_formatter(val, pos=None) -> str:
    """
    Custom formatter for the x-axis.
    Returns a string formatted as '83-602'.
    """
    return f'{int(val)//10000}-{int(val)%10000:03}'

def plot_groups(df,ds, x_key: str, y_key: str, the_function: str) -> None:
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
    if key_units[x_key] == 'days':
        the_groups = df.groupby(df[x_key].dt.date)
        envelope_width = 2
    elif key_units[x_key] == 'track_id1':
        the_groups = df.groupby(df['track_id1'])
        use_custom_formatter = True
    elif key_units[x_key] == 'track_id2':
        the_groups = df.groupby(df['track_id2'])
        use_custom_formatter = True
    else:
        raise ValueError(f"Unsupported {key_units[x_key] = }")

    num_bins = len(the_groups)
    logger.info(f"Number of {key_units[x_key]} bins: {num_bins}")

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
    logger.info(f"{my_capitalize(the_function)} of crossover {ds[y_key].attrs['long_name']}: {thevalue:{my_format}} {ds[y_key].attrs['units']}")

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
        logger.info(f"{this_x} : {the_function} of {all_sat_names} {x_key} = {y[i]:+.4}")
    
    if len(bad_x) > 0:
        logger.error(f"!!!!!!!!!!!!! {len(bad_x)} outliers found:")
        logger.error(f"mean_y = {mean_y:.4f}")
        logger.error(f"std_y = {std_y:.4f}")
        logger.error(f"lower_bound = {lower_bound:.4f}")
        logger.error(f"upper_bound = {upper_bound:.4f}")

    for i in range(len(bad_x)):
        if use_custom_formatter:
            this_x = custom_formatter(bad_x[i],0)
        else:
            this_x = bad_x[i]
        logger.error(f"{this_x} : {the_function} of {all_sat_names} {x_key} = {bad_y[i]:+.4}")
    
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
    plt.rcParams.update({"font.size": fsize})
    fig, axs = plt.subplots(1, len(gap_indices) + 1, sharey=True, figsize=myfigsize)

    x_min = min(x)
    x_max = max(x)
    y_min = min(y)
    y_max = max(y)
    the_range = y_max - y_min
    new_y_position  = y_max  + 1*blurb_offset * (the_range)
    new_y_limit_max = y_max  + 2*blurb_offset * (the_range)
    new_y_limit_min = y_min  - 2*blurb_offset * (the_range)
    
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
    fig.text(0.14, 0.86, f"{my_capitalize(the_function)} over all {num_bins} {key_units[x_key]} = {thevalue:{my_format}} {ds[y_key].attrs['units']}", fontsize=fsize, verticalalignment='top')

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
    fig.text(0.5, 0.03, f'{x_key}', ha='center', va='center', fontsize=fsize)
    #Y-Axis Title:
    fig.text(0.04, 0.5, f"{my_capitalize(the_function)} of crossover {ds[y_key].attrs['long_name']} ({ds[y_key].attrs['units']})", ha='center', va='center', rotation='vertical', fontsize=fsize)
    plt.suptitle(f"{my_capitalize(units_desc[key_units[x_key]])} {the_function} of {all_sat_names} crossover {ds[y_key].attrs['long_name']}")
    output_filename = f'{datetime.datetime.now().strftime("%Y%m%d-%H%M%S")}-{all_sat_names}-{units_desc[key_units[x_key]]}-{the_function}-plot.png'
    plt.savefig(os.path.join(outputpath,output_filename), dpi=dpi_choice, bbox_inches='tight', facecolor='w', transparent=False)
    # Display plot
    #plt.show()

def make_histogram(df,ds,y_key,bin_size) -> None:
    bin_edges = np.arange(df[y_key].min(), df[y_key].max() + bin_size, bin_size)
    plt.rcParams.update({"font.size": fsize})
    fig, ax = plt.subplots(figsize=myfigsize)
    plt.hist(df[y_key], bins=bin_edges)
    plt.xlabel(f"{ds[y_key].attrs['long_name']} ({ds[y_key].attrs['units']})")
    plt.ylabel('Frequency')
    plt.title(f"Histogram of {all_sat_names} crossover {ds[y_key].attrs['long_name']}")
    output_filename = f'{datetime.datetime.now().strftime("%Y%m%d-%H%M%S")}-{all_sat_names}-{y_key}-histogram.png'
    plt.savefig(os.path.join(outputpath,output_filename), dpi=dpi_choice, bbox_inches='tight', facecolor='w', transparent=False)
    #plt.show()


def make_plot(df,ds,x_key,y_key) -> None:
    x_name  = ds[x_key].attrs['long_name']
    y_name  = ds[y_key].attrs['long_name']
    x_units = ds[x_key].attrs['units']
    y_units = ds[y_key].attrs['units']

    # Create the plot
    plt.rcParams.update({"font.size": fsize})
    fig, ax = plt.subplots(figsize=myfigsize)
    #plt.plot(df[x_key], df[y_key], marker='o', linestyle='', color='b')
    plt.plot(df[x_key], df[y_key], marker='o', linestyle='', color='b', markeredgecolor='black', markeredgewidth=0.5, alpha=0.4)
    plt.xlabel(f'{x_name} ({x_units})')
    plt.ylabel(f'{y_name} ({y_units})')
    plt.title(f'{all_sat_names} crossover {y_name} vs. {x_name}')
    output_filename = f'{datetime.datetime.now().strftime("%Y%m%d-%H%M%S")}-{all_sat_names}-{x_key}-vs-{y_key}-plot.png'
    plt.savefig(os.path.join(outputpath,output_filename), dpi=dpi_choice, bbox_inches='tight', facecolor='w', transparent=False)
    #plt.show()

def make_map(df,ds) -> None:
    # Extract data from the dataset
    lons = df['lon']
    lats = df['lat']
    ssh_diff = df['ssh_diff']

    # Create the map using the PlateCarree projection
    plt.rcParams.update({"font.size": fsize})
    fig, ax = plt.subplots(figsize=myfigsize, subplot_kw={'projection': ccrs.PlateCarree()})
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
    plt.title(f"{num_crossovers} {all_sat_names} crossover {ds['ssh_diff'].attrs['long_name']}s")

    output_filename = f'{datetime.datetime.now().strftime("%Y%m%d-%H%M%S")}-{all_sat_names}-ssh_diff-map.png'
    plt.savefig(os.path.join(outputpath,output_filename), dpi=dpi_choice, bbox_inches='tight', facecolor='w', transparent=False)

    # Show the map
    #plt.show()

def is_valid_dataset(file_path) -> bool:
    '''
    Check if the dataset is valid by opening it and checking if any of its dimensions have a size of 0.
    '''
    try:
        ds = xr.open_dataset(file_path)
        return not any(size == 0 for size in ds.dims.values())
    except Exception as e:
        print(f"Error checking dataset {file_path}: {e}")
        return False

def analyze_cycle_data(df, cycle_number) -> tuple[float, float, int]:
    # Filter the DataFrame for the given cycle number in both cycle1 and cycle2
    cycle_df = df[(df['cycle1'] == cycle_number) & (df['cycle2'] == cycle_number)]
    
    # Calculate the mean and RMS of ssh_diff for the filtered data
    mean_ssh_diff = cycle_df['ssh_diff'].mean()
    rms_ssh_diff = np.sqrt(np.mean(np.square(cycle_df['ssh_diff'])))
    
    # Count the number of crossover points
    num_crossover_points = len(cycle_df)
    
    return mean_ssh_diff, rms_ssh_diff, num_crossover_points

def make_cycle_plots(df) -> None:
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
        logger.info(f"Cycle {cycle}: Mean SSH diff = {mean_ssh_diff:.4f}, RMS SSH diff = {rms_ssh_diff:.4f}, Number of crossover points = {num_crossover_points}")

    # Plotting the results
    fig, axs = plt.subplots(3, 1, figsize=(16, 18))

    # Mean SSH difference plot
    axs[0].plot(cycle_numbers, mean_ssh_diffs, marker='o')
    axs[0].set_title('Mean SSH difference by cycle')
    axs[0].set_xlabel('cycle number')
    axs[0].set_ylabel('mean SSH difference (m)')

    # RMS SSH difference plot
    axs[1].plot(cycle_numbers, rms_ssh_diffs, marker='o', color='r')
    axs[1].set_title('RMS SSH difference by cycle')
    axs[1].set_xlabel('Cycle Number')
    axs[1].set_ylabel('RMS SSH Difference (m)')

    # Number of crossover points plot
    axs[2].bar(cycle_numbers, num_crossover_points_list, color='g')
    axs[2].set_title('Number of crossover points by cycle')
    axs[2].set_xlabel('cycle number')
    axs[2].set_ylabel('number of crossover points')

    plt.tight_layout()
    output_filename = f'{datetime.datetime.now().strftime("%Y%m%d-%H%M%S")}-cycle-analysis.png'
    plt.savefig(os.path.join(outputpath, output_filename), dpi=dpi_choice, bbox_inches='tight', facecolor='w', transparent=False)
    #plt.show()

def main():
    logging_setup("log-map-xcoords")

    global outputpath
    outputpath = os.path.join('..','output_from_crossovers')
    # Remove the trailing '/' if present
    outputpath = outputpath.rstrip('/')
    # Check if the path exists
    if os.path.exists(outputpath):
        # If it exists but is a file, raise an exception
        if os.path.isfile(outputpath):
            raise Exception(f"'{outputpath}' exists but is a file, not a directory.")
    else:
        # If the directory doesn't exist, create it
        logger.info(f"Creating output directory '{outputpath}' because it doesn't exist.")
        os.makedirs(outputpath)

    input_path = os.path.join('..','crossover_files')
    #input_path = os.path.join('..','local_crossover_files')

    # Get a list of all the filenames in the input directory
    filenames = sorted([f for f in os.listdir(input_path) if f.startswith('xovers_') and f.endswith('.nc')])
    filenames = filenames[0:10]

    # Filter the list of file paths, keeping only those that lead to valid datasets
    logger.info(f"Looking for non-empty files among {len(filenames)} files from {input_path}...")
    valid_file_paths = [os.path.join(input_path, f) for f in filenames if is_valid_dataset(os.path.join(input_path, f))]

    # Now, open and combine the valid datasets
    logger.info(f"Loading {len(valid_file_paths)} non-empty files from {input_path}...")

    ds = xr.open_mfdataset(valid_file_paths, combine='by_coords')

    logger.info(f"Finished loading {len(valid_file_paths)} files.")

    # Reconstruct the original satellite strings list and the concatenated string.
    if ', ' not in ds.attrs['satellite_names']:
        self_crossovers = True
    else:
        self_crossovers = False
    global all_sat_names
    all_sat_names = ds.attrs['satellite_names']

    ds['time_diff'] = ds['time2'] - ds['time1']
    ds['time_diff'].attrs['long_name'] = "Time difference"

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

    global num_crossovers
    num_crossovers = len(df['ssh_diff'])
    logger.info(f"Loaded {num_crossovers} {all_sat_names} crossover points.")

    plot_choices = []
    plot_choices.append('plot_groups')
    plot_choices.append('plot_histogram')
    plot_choices.append('plot_pairs')
    plot_choices.append('plot_map')
    plot_choices.append('plot_cycle_analysis')

    if 'plot_groups' in plot_choices:
        global blurb_offset
        blurb_offset = 0.1
        global key_units
        key_units = {}
        key_units['time1'] = 'days'
        key_units['track_id1'] = 'track_id1'
        key_units['track_id2'] = 'track_id2'
        key_units['ssh_diff'] = 'm'
        global units_desc
        units_desc = {}
        units_desc['days'] = 'daily'
        units_desc['track_id1'] = 'track ID'
        units_desc['track_id2'] = 'track ID'
        plot_groups(df,ds,'time1','ssh_diff','mean')
        plot_groups(df,ds,'time1','ssh_diff','RMS')
        plot_groups(df,ds,'track_id1','ssh_diff','mean')
        plot_groups(df,ds,'track_id2','ssh_diff','mean')
        plot_groups(df,ds,'track_id1','ssh_diff','RMS')
        plot_groups(df,ds,'track_id2','ssh_diff','RMS')

    if 'plot_histogram' in plot_choices:
        hist_keys = ['time_diff','ssh_diff']
        bin_size = 1.0
        for hist_key in hist_keys:
            make_histogram(df,ds,hist_key,bin_size)

    if 'plot_pairs' in plot_choices:
        key_pairs = []
        key_pairs.append(['time_diff','ssh_diff'])
        key_pairs.append(['lat','ssh_diff'])
        key_pairs.append(['lon','ssh_diff'])
        for key_pair in key_pairs:
            make_plot(df,ds,key_pair[0],key_pair[1])

    if 'plot_map' in plot_choices:
        make_map(df,ds)

    if 'plot_cycle_analysis' in plot_choices:
        make_cycle_plots(df)

if __name__ == '__main__':
    main()