from datetime import datetime, timedelta
import logging
from typing import List
import warnings

import numpy as np
import pandas as pd
import xarray as xr
import netCDF4 as nc

from utilities.aws_utils import aws_manager
from indicators.pattern_data import Pattern
from indicators.txt_engine import generate_txt

with warnings.catch_warnings():
    warnings.simplefilter("ignore", UserWarning)
    from pyresample.utils import check_and_wrap


def dt_to_dec(date: datetime) -> float:
    """
    Transforms datetime values to year decimal values.
    """
    year_start = date.replace(month=1, day=1)
    year_end = year_start.replace(year=date.year + 1)
    fraction_of_year = (date - year_start).total_seconds() / (
        year_end - year_start
    ).total_seconds()
    return date.year + fraction_of_year


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


class IndicatorProcessor:
    def __init__(self, sg_keys: List[str]):
        self.grid_keys = sg_keys
        self.patterns = [Pattern("enso"), Pattern("pdo"), Pattern("iod")]
        self.grid_cell_areas = (
            xr.open_dataset("ref_files/half_deg_grid_cell_areas.nc")
            .sel(latitude=slice(-66, 66), drop=True)["area"]
            .values
        )
        self.trend_ds = xr.open_dataset("ref_files/BH_offset_and_trend_v0_new_grid.nc")
        self.annual_cycle_ds = xr.open_dataset("ref_files/ann_pattern.nc")[
            "ann_pattern"
        ]

    @staticmethod
    def validate_counts(counts: np.ndarray, threshold: float = 0.9) -> bool:
        """
        Checks if counts average is above threshold value.
        """
        return np.nanmean(counts) > threshold * 500

    def calc_gmsl(self, masked_ssha: np.ma.masked_array) -> float:
        """
        Compute GMSL in cm
        """       
        masked_ssha = masked_ssha.filled(np.nan)
        weighted_ssha_sum = np.nansum(masked_ssha * self.grid_cell_areas)
        total_area = np.nansum(self.grid_cell_areas[~np.isnan(masked_ssha)])
        gmsl = (weighted_ssha_sum / total_area) * 100
        return gmsl


    def detrend_deseason(self, date: datetime, masked_ssha: np.ndarray) -> np.ndarray:
        # Compute trend
        time_diff = int((date - datetime(1992, 10, 2)).total_seconds())
        trend = (
            time_diff * self.trend_ds["BH_sea_level_trend_meters_per_second"]
            + self.trend_ds["BH_sea_level_offset_meters"]
        )
        masked_ssha = np.ma.masked_invalid(masked_ssha)  # Mask invalid (NaN) values
        trend = np.ma.masked_invalid(trend)  # Mask invalid (NaN) values

        # Remove trend (ensure we don't perform any operations on NaN values)
        detrended = masked_ssha - trend

        # Grab seasonal cycle
        seasonal_cycle = self.annual_cycle_ds.sel(month=date.month).values / 1e3

        # Mask invalid values in seasonal cycle
        seasonal_cycle = np.ma.masked_invalid(seasonal_cycle)

        # Remove seasonal cycle from detrended data
        detrended_deseasoned = detrended - seasonal_cycle

        # Return a valid ndarray
        return detrended_deseasoned.filled(np.nan)

    def process_cycle(self, date: datetime, cycle_ds: nc.Dataset) -> dict:
        """
        1. Compute global mean value and store
        2. Remove trend -> remove seasonal cycle -> select area of interest -> least squares fit to pattern
        """

        latitudes = cycle_ds.variables["latitude"][:]
        lat_idx = np.where((latitudes >= -66) & (latitudes <= 66))[0]

        lons, lats = check_and_wrap(cycle_ds["longitude"][:], cycle_ds["latitude"][:])

        ssha = cycle_ds.variables["ssha"][:]
        basin_flag = cycle_ds.variables["basin_flag"][:]
        masked_ssha = np.ma.masked_where((basin_flag <= 0) & (basin_flag >= 1000), ssha)

        indicator_data = {"time": dt_to_dec(date)}

        # Compute GMSL
        gmsl = self.calc_gmsl(masked_ssha[lat_idx])
        indicator_data["gmsl"] = gmsl

        # Remove trend and seasonal cycle in prep for indicator computation
        detrended_deseasoned = self.detrend_deseason(date, masked_ssha)
        # Compute indicator value for each pattern
        for pattern in self.patterns:
            # Select pattern area of interest
            target_lon_idx = np.where(np.isin(lons, pattern.pattern_lons))[0]
            target_lat_idx = np.where(np.isin(lats, pattern.pattern_lats))[0]
            ssha_da = detrended_deseasoned[target_lat_idx, :][:, target_lon_idx]

            ssha_anom = np.where(pattern.pattern_nns, ssha_da, np.nan)

            nonnans = ~np.isnan(ssha_anom)
            ssha_anom_to_fit = ssha_anom[nonnans]
            pattern_to_fit = pattern.pattern_field[nonnans] / 1e3

            X = np.vstack(np.array(pattern_to_fit))
            B_hat, _, _, _ = np.linalg.lstsq(
                X.T @ X, X.T @ ssha_anom_to_fit.T, rcond=None
            )
            indicator_data[pattern.name] = B_hat[0]
        return indicator_data

    def generate_ds(self, computed_indicators: dict) -> xr.Dataset:
        df = pd.DataFrame(computed_indicators)

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
        return indicators_ds

    def run(self):
        logging.info("Beginning indicators calculations...")

        computed_indicators = []

        # Process each grid
        for grid_key in self.grid_keys:
            date = datetime.strptime(grid_key.split("_")[-1][:8], "%Y%m%d")
            if date < datetime(1993, 1, 1):
                continue

            logging.info(f"Processing {grid_key}")
            try:
                stream = aws_manager.stream_obj(grid_key)
                
                cycle_ds = nc.Dataset("dummy", memory=stream.read())
                latitudes = cycle_ds.variables["latitude"][:]
                lat_idx = np.where((latitudes >= -66) & (latitudes <= 66))[0]
                counts = cycle_ds.variables["counts"][lat_idx]

                if not self.validate_counts(counts):
                    logging.warning(
                        f"Too much data missing from {date.strftime('%Y-%m-%d')} cycle. Skipping."
                    )
                    continue

                indicator_values = self.process_cycle(date, cycle_ds)
                computed_indicators.append(indicator_values)

            except Exception as e:
                logging.exception(f"Error processing cycle {grid_key}. {e}")

        # Convert results to xarray Dataset
        indicators_ds = self.generate_ds(computed_indicators)

        indicators_ds.to_netcdf("/tmp/indicators.nc")
        aws_manager.upload_obj(
            "/tmp/indicators.nc", "s3://example-bucket/indicators/indicators.nc"
        )

        # Convert xarray Dataset to individual indicator txt files
        for indicator_name in ["gmsl", "enso", "iod", "pdo"]:
            filename = f"NASA_SSH_{indicator_name.upper()}_INDICATOR.txt"
            generate_txt(indicators_ds, indicator_name)
            
            # Upload (and replace) latest version
            aws_manager.upload_obj(
                f"/tmp/{filename}.txt", f"s3://example-bucket/indicators/{filename}"
            )
            
            # Upload archival version
            date_str = datetime.now().date().isoformat().replace('-','')
            date_filename = f"NASA_SSH_{indicator_name.upper()}_INDICATOR_{date_str}.txt"
            aws_manager.upload_obj(
                f"/tmp/{filename}.txt", f"s3://example-bucket/indicators/archive/{indicator_name.upper()}/{date_filename}"
            )
