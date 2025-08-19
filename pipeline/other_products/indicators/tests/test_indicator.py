import logging
from typing import Iterable
import unittest
import pandas as pd
import xarray as xr
import numpy as np
import netCDF4 as nc
from datetime import datetime, timedelta

from indicators.compute_indicators import IndicatorProcessor
from glob import glob


def decimal_year_to_datetime(decimal_years):
    """Convert an array of decimal years to datetime objects."""
    dates = []
    for year_decimal in decimal_years:
        year = int(year_decimal)
        days = (year_decimal - year) * 365.25  # Convert fraction to days
        date = datetime(year, 1, 1) + timedelta(days=days)
        dates.append(date)
    return np.array(dates)


def running_mean(data, time, window=28.1):
    """
    Compute a 60-day smoothed version of the input data using a running mean.
    The window is 28.1 days before and after, and it shrinks near the edges.

    Parameters:
        data (np.ndarray): 1D NumPy array of data points.
        time (np.ndarray): 1D NumPy array of time points (in days).
        window (float): Half-window size in days (default: 28.1 days).

    Returns:
        np.ndarray: Smoothed data array of the same length as input.
    """
    smoothed = np.full_like(data, np.nan)  # Initialize output with NaNs
    for i in range(len(data)):
        # Define dynamic window range
        lower_bound = time[i] - timedelta(days=window)
        upper_bound = time[i] + timedelta(days=window)
        # Find indices within window
        indices = (time >= lower_bound) & (time <= upper_bound)
        # Compute mean over valid indices
        smoothed[i] = np.nanmean(np.array(data)[indices]) if np.any(indices) else np.nan

    return smoothed


def create_lines(ds: xr.Dataset, indicator_name: str) -> Iterable[str]:
    """
    Creates list of formatted strings consisting of
    date, enso, pdo, and iod values per string.
    """
    lines = []
    for time in ds["time"]:
        time_slice = ds.sel(time=time)
        indicator_value = time_slice[indicator_name].values
        if indicator_name == "gmsl":
            smoothed_gmsl = time_slice["smoothed_gmsl"].values
            lines.append(f"{time:<12.7f} {indicator_value:>12f} {smoothed_gmsl:>12f}\n")
        else:
            lines.append(f"{time:<12.7f} {indicator_value:>12f}\n")
    return lines


def generate_txt(ds: xr.Dataset, indicator_name: str):
    lines = create_lines(ds, indicator_name)

    with open(f"ref_files/txt_templates/NASA_SSH_{indicator_name.upper()}_INDICATOR.txt", "r") as template:
        with open(f"{indicator_name}.txt", "w") as f:
            template_header = template.readlines()
            template_header = [
                hdr.replace("PLACEHOLDER_CREATION_DATE", datetime.now().date().isoformat()) for hdr in template_header
            ]
            f.writelines(template.readlines())
            f.write("\n")
            f.writelines(lines)


class EndToEndGSFCProcessingTestCase(unittest.TestCase):
    temp_dir: str
    daily_ds: xr.Dataset

    class Granule:
        def __init__(self, title) -> None:
            self.title = title

    @classmethod
    def setUpClass(cls) -> None:
        logging.root.handlers = []
        logging.basicConfig(
            level="INFO",
            format="[%(levelname)s] %(asctime)s - %(message)s",
            handlers=[logging.StreamHandler()],
        )

        sg_keys = [
            "tests/test_granules/NASA-SSH_alt_ref_simple_grid_v1_20241111.nc",
            "tests/test_granules/NASA-SSH_alt_ref_simple_grid_v1_20250106.nc",
        ]
        sg_keys = sorted(glob("data/from_podaac_bucket/NASA_SSH_REF_SIMPLE_GRID_V1/*.nc"))

        ind_proc = IndicatorProcessor(sg_keys)

        cls.computed_indicators = []

        # Process each grid
        for grid_key in ind_proc.grid_keys:
            date = datetime.strptime(grid_key.split("_")[-1][:8], "%Y%m%d")
            if date < datetime(1993, 1, 1):
                continue

            logging.info(f"Processing {grid_key}")
            try:
                cycle_ds = nc.Dataset(grid_key, "r")
                latitudes = cycle_ds.variables["latitude"][:]
                lat_idx = np.where((latitudes >= -66) & (latitudes <= 66))[0]
                counts = cycle_ds.variables["counts"][lat_idx]

                if not ind_proc.validate_counts(counts):
                    logging.warning(f"Too much data missing from {date.strftime('%Y-%m-%d')} cycle. Skipping.")
                    continue

                indicator_values = ind_proc.process_cycle(date, cycle_ds)
                cls.computed_indicators.append(indicator_values)

            except Exception as e:
                logging.exception(f"Error processing cycle {grid_key}. {e}")

        # Test appending to existing data
        df = pd.DataFrame(cls.computed_indicators)

        # Set GMSL to 1993 zero mean
        mean_1993 = df[(df["time"] >= 1993) & (df["time"] < 1994)]["gmsl"].mean()
        df["gmsl"] = df["gmsl"] - mean_1993

        indicators_ds = xr.Dataset.from_dataframe(df.set_index("time"))
        indicators_ds = indicators_ds.sortby("time")
        indicators_ds["time"].attrs = {"units": "Date in decimal year format"}
        indicators_ds["gmsl"].attrs = {"units": "cm"}

        smoothed_gmsl = running_mean(
            indicators_ds["gmsl"].values,
            decimal_year_to_datetime(indicators_ds["time"].values),
        )
        indicators_ds["smoothed_gmsl"] = (["time"], smoothed_gmsl, {"units": "cm"})

        for indicator_name in ["gmsl", "enso", "iod", "pdo"]:
            generate_txt(indicators_ds, indicator_name)

        indicators_ds.to_netcdf("indicators.nc")

    def test_file_date_coverage(self):
        self.assertGreaterEqual(self.daily_ds["time"].values.min(), np.datetime64("1995-06-07"))
        self.assertLessEqual(self.daily_ds["time"].values.max(), np.datetime64("1995-06-07T23:59:59"))
