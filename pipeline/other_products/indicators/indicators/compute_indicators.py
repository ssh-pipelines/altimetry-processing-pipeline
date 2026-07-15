import logging
import os
import shutil
import warnings
from datetime import datetime, timedelta
from os.path import basename, join
from typing import List

import netCDF4 as nc
import numpy as np
import pandas as pd
import xarray as xr
from indicators.pattern_data import Pattern
from indicators.utils import dec_to_dt, dt_to_dec, generate_mp, generate_txt

from utilities.aws_utils import aws_manager
from utilities.pipeline_layout import indicators_key, indicators_prefix, s3_uri

with warnings.catch_warnings():
    warnings.simplefilter("ignore", UserWarning)
    from pyresample.utils import check_and_wrap


def running_mean(data: np.ndarray, time: np.ndarray, window=28.1) -> np.ndarray:
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
    smoothed = np.full_like(data, np.nan)
    for i in range(len(data)):
        lower_bound = time[i] - timedelta(days=window)
        upper_bound = time[i] + timedelta(days=window)
        indices = (time >= lower_bound) & (time <= upper_bound)
        smoothed[i] = np.nanmean(np.array(data)[indices]) if np.any(indices) else np.nan
    return smoothed


class IndicatorProcessor:
    def __init__(self, sg_keys: List[str], source: str):
        self.grid_keys = sg_keys
        self.source = source
        self.patterns = [Pattern("enso"), Pattern("pdo"), Pattern("iod")]
        self.grid_cell_areas = self._open_grid_cell_areas()
        self.trend_ds = xr.open_dataset("ref_files/BH_offset_and_trend_v0_new_grid.nc")
        self.annual_ds = xr.open_dataset("ref_files/ann_pattern.nc")["ann_pattern"]

    @staticmethod
    def _open_grid_cell_areas() -> np.ndarray:
        ds = xr.open_dataset("ref_files/half_deg_grid_cell_areas.nc")
        return ds.sel(latitude=slice(-66, 66), drop=True)["area"].values

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
        masked_ssha = np.ma.masked_invalid(masked_ssha)
        trend = np.ma.masked_invalid(trend)

        # Remove trend (ensure we don't perform any operations on NaN values)
        detrended = masked_ssha - trend

        # Grab seasonal cycle
        seasonal_cycle = self.annual_ds.sel(month=date.month).values / 1e3

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

        # Compute GMSL (raw = before 1993 normalization)
        gmsl = self.calc_gmsl(masked_ssha[lat_idx])
        indicator_data["raw_gmsl"] = gmsl

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

    def load_cached_indicators(self, bucket: str) -> List[dict]:
        """
        Load previously computed indicator records from the cached NetCDF on S3.
        Returns a list of dicts with keys: time, raw_gmsl, enso, pdo, iod.
        """
        cache_key = s3_uri(bucket, indicators_key(self.source))
        if not aws_manager.key_exists(cache_key):
            logging.info("No cached indicators found — starting fresh.")
            return []

        logging.info(f"Loading cached indicators from {cache_key}")
        stream = aws_manager.stream_obj(cache_key)
        tmp_path = "/tmp/cached_indicators.nc"
        with open(tmp_path, "wb") as f:
            f.write(stream.read())

        ds = xr.open_dataset(tmp_path)
        os.remove(tmp_path)

        records = []
        for t in ds["time"].values:
            record = {"time": float(t)}
            # Legacy fallback: pre-migration caches have 'gmsl' instead of 'raw_gmsl'
            if "raw_gmsl" in ds:
                record["raw_gmsl"] = float(ds["raw_gmsl"].sel(time=t).values)
            elif "gmsl" in ds:
                record["raw_gmsl"] = float(ds["gmsl"].sel(time=t).values)
            else:
                logging.warning(f"Cache missing gmsl/raw_gmsl for time={t}, skipping")
                continue
            for var in ["enso", "pdo", "iod"]:
                if var in ds:
                    record[var] = float(ds[var].sel(time=t).values)
            records.append(record)

        ds.close()
        return records

    @staticmethod
    def merge_indicators(cached: List[dict], new: List[dict]) -> List[dict]:
        """
        Merge cached and new indicator records. New values overwrite cached
        at the same time point. Result is sorted by time.
        """
        merged = {r["time"]: r for r in cached}
        for r in new:
            merged[r["time"]] = r
        return sorted(merged.values(), key=lambda r: r["time"])

    def generate_ds(self, computed_indicators: list) -> xr.Dataset:
        df = pd.DataFrame(computed_indicators)

        # Compute normalized GMSL: zero-mean over 1993
        records_1993 = df[(df["time"] >= 1993) & (df["time"] < 1994)]["raw_gmsl"]
        if len(records_1993) > 0:
            mean_1993 = records_1993.mean()
        else:
            logging.warning(
                "No 1993 data available for GMSL normalization — using raw values."
            )
            mean_1993 = 0.0

        df["gmsl"] = df["raw_gmsl"] - mean_1993

        indicators_ds = xr.Dataset.from_dataframe(df.set_index("time"))
        indicators_ds = indicators_ds.sortby("time")
        indicators_ds["time"].attrs = {"units": "Date in decimal year format"}
        indicators_ds["gmsl"].attrs = {"units": "cm"}
        indicators_ds["raw_gmsl"].attrs = {"units": "cm"}

        decimal_years_to_datetimes = np.vectorize(dec_to_dt)
        smoothed_gmsl = running_mean(
            indicators_ds["gmsl"].values,
            decimal_years_to_datetimes(indicators_ds["time"].values),
        )
        indicators_ds["smoothed_gmsl"] = (["time"], smoothed_gmsl, {"units": "cm"})
        return indicators_ds

    def format_and_upload(self, computed_indicators: List[dict], bucket: str):
        """
        From list of dictionary values of indicators:
        1. Make netcdf containing all indicators and upload to s3
        For each indicator:
        2. Make and upload text file to s3
        3. Make .mp file and upload to s3
        4. Make archival version of text file and upload to s3
        5. Make .mp file and upload to s3
        """
        # Convert results to xarray Dataset
        indicators_ds = self.generate_ds(computed_indicators)

        prefix_uri = s3_uri(bucket, indicators_prefix(self.source))

        # Make and upload netcdf
        nc_path = "/tmp/indicators.nc"
        indicators_ds.to_netcdf(nc_path)
        aws_manager.upload_obj(nc_path, s3_uri(bucket, indicators_key(self.source)))

        first_time = int(dec_to_dt(indicators_ds["time"].values[0]).timestamp() * 1000)
        last_time = int(dec_to_dt(indicators_ds["time"].values[-1]).timestamp() * 1000)

        # Convert xarray Dataset to individual indicator txt files
        for indicator_name in ["gmsl", "enso", "iod", "pdo"]:
            # Make text file
            filename = generate_txt(indicators_ds, indicator_name)
            shortname = filename.replace(".txt", "")
            local_path = join("/tmp", filename)

            # Upload (and replace) latest version
            aws_manager.upload_obj(local_path, join(prefix_uri,filename))

            # Generate and upload .mp file
            mp_path = generate_mp(first_time, last_time, local_path, shortname)
            aws_manager.upload_obj(mp_path, join(prefix_uri,basename(mp_path)))

            # Generate and upload archival version
            date_str = datetime.now().date().isoformat().replace("-", "")
            date_filename = filename.replace(".txt", f"_{date_str}.txt")
            archive_path = join("/tmp", date_filename)

            shutil.copyfile(local_path, archive_path)

            s3_archive_path = join(
                prefix_uri,"archive", indicator_name.upper(), date_filename
            )
            aws_manager.upload_obj(archive_path, s3_archive_path)

            # Generate and upload archival .mp file
            mp_path = generate_mp(first_time, last_time, archive_path, shortname)
            s3_mp_path = join(
                prefix_uri,"archive", indicator_name.upper(), basename(mp_path)
            )
            aws_manager.upload_obj(mp_path, s3_mp_path)

    def run(self, bucket: str):
        logging.info("Beginning indicators calculations...")

        # Load cached indicators
        cached_indicators = self.load_cached_indicators(bucket)
        logging.info(f"Loaded {len(cached_indicators)} cached indicator records")

        computed_indicators = []

        # Process each grid
        for grid_key in self.grid_keys:
            date = datetime.strptime(grid_key.split("_")[-1][:8], "%Y%m%d")
            if date < datetime(1993, 1, 1):
                continue

            if not aws_manager.key_exists(grid_key):
                logging.warning(f"Simple grid not found on S3: {grid_key}, skipping.")
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

        # Merge new results with cached
        merged = self.merge_indicators(cached_indicators, computed_indicators)
        logging.info(
            f"Merged {len(computed_indicators)} new + {len(cached_indicators)} cached "
            f"= {len(merged)} total records"
        )

        if merged:
            self.format_and_upload(merged, bucket)
        else:
            logging.warning("No indicator records to output.")
