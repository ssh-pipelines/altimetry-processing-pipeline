from dataclasses import dataclass
import logging
import os
from typing import Iterable, TextIO
import pandas as pd
import xarray as xr
import netCDF4 as nc
import numpy as np
from datetime import datetime

from daily_files.processing.daily_file import DailyFile
from daily_files.collection_metadata import AllCollections, CollectionMeta


class S6DailyFile(DailyFile):
    def __init__(self, file_objs: Iterable[TextIO], date: datetime, collection_ids: Iterable[str], bucket: str):
        self.date = date

        logging.info(f"Opening {len(file_objs)} files")
        opened_files = [self.extract_grouped_data(file_obj) for file_obj in file_objs]
        ds = xr.concat(opened_files, dim="time")
        self.original_ds = ds
        self.collection_ids = collection_ids

        ssha: np.ndarray = ds["ssha"].values
        lats: np.ndarray = ds["latitude"].values
        lons: np.ndarray = ds["longitude"].values
        times: np.ndarray = ds["time"].values
        cycles: np.ndarray = ds["cycle"].values
        passes: np.ndarray = ds["passes"].values
        dac: np.ndarray = ds["dac"].values

        self.source_mss = "DTU18"
        self.target_mss = "DTU21"
        self.mss_name = f"{self.source_mss}_minus_{self.target_mss}.nc"

        super().__init__(ssha, lats, lons, times, cycles, passes, dac)
        self.ds["mean_sea_surface_sol1"] = (
            ("time"),
            self.original_ds["mean_sea_surface_sol1"].values,
        )
        self.ds["mean_sea_surface_sol2"] = (
            ("time"),
            self.original_ds["mean_sea_surface_sol2"].values,
        )
        self.make_daily_file_ds()

    def extract_grouped_data(self, file_obj: TextIO) -> xr.Dataset:
        """
        Use the netCDF4 library to efficiently open and extract grouped variables
        """
        ds = nc.Dataset("file_like", "r", memory=file_obj.read())

        s6_offset = None
        if "product_name" in ds.ncattrs() and "G01" in ds.product_name:
            s6_offset = 0.011

        das = []

        for var in [
            "latitude",
            "longitude",
            "surface_classification_flag",
            "rain_flag",
            "rad_water_vapor_qual",
            "dac",
            "mean_sea_surface_sol1",
            "mean_sea_surface_sol2",
        ]:
            nc_var = ds.groups["data_01"].variables[var]
            nc_var_data = nc_var[:]
            nc_var_attrs = {k: v for k, v in nc_var.__dict__.items() if k != "scale_factor"}
            da = xr.DataArray(nc_var_data, dims="time", attrs=nc_var_attrs, name=var)
            das.append(da)

        for var in ["sig0_ocean", "range_ocean_qual", "swh_ocean", "ssha"]:
            nc_var = ds.groups["data_01"].groups["ku"].variables[var]
            nc_var_data = nc_var[:]

            if var == "ssha" and s6_offset is not None:
                nc_var_data = nc_var_data + s6_offset

            nc_var_attrs = {k: v for k, v in nc_var.__dict__.items() if k != "scale_factor"}
            da = xr.DataArray(nc_var_data, dims="time", attrs=nc_var_attrs, name=var)
            das.append(da)

        merged_ds = xr.merge(das)
        merged_ds = merged_ds.set_coords(["latitude", "longitude"])
        merged_ds["time"] = ds.groups["data_01"].variables["time"][:]
        merged_ds["time"].attrs = {
            k: v
            for k, v in ds.groups["data_01"].variables["time"].__dict__.items()
            if k != "scale_factor" and k != "add_offset"
        }
        merged_ds.attrs = {k: v for k, v in ds.__dict__.items() if k != "scale_factor" and k != "add_offset"}
        merged_ds["cycle"] = (
            ("time"),
            np.full(merged_ds["time"].values.shape, ds.cycle_number),
        )
        merged_ds["passes"] = (
            ("time"),
            np.full(merged_ds["time"].values.shape, ds.pass_number),
        )
        return xr.decode_cf(merged_ds)

    def make_daily_file_ds(self):
        """
        Ordering of steps to create daily file from GSFC granule
        """
        self.map_points_to_basin()
        self.make_nasa_flag()
        self.clean_date(self.date)
        self.mss_swap()
        self.apply_basin_to_nasa()
        self.make_ssha_smoothed(self.date)
        self.set_metadata()
        self.set_source_attrs()

    def make_nasa_flag(self):
        """ """
        logging.info("Making nasa_flag...")
        kqual = self.original_ds["range_ocean_qual"].values
        surfc = self.original_ds["surface_classification_flag"].values
        rqual = self.original_ds["rad_water_vapor_qual"].values
        rain = self.original_ds["rain_flag"].values
        s0 = self.original_ds["sig0_ocean"].values
        swh = self.original_ds["swh_ocean"].values
        ssha = self.original_ds["ssha"].values
        basin_flag = self.ds["basin_flag"].values
        lats = self.ds["latitude"].values

        n_median = 15
        n_std = 95
        timestamps = np.array(range(1, len(ssha) + 1))

        @dataclass
        class Point:
            x: int
            y: int

        p1, p2 = Point(11, 10), Point(16, 6)
        p3, p4 = Point(26, 3), Point(32, 0)

        # 1st trend line goes from (x1, y1) to (x2, y2)
        swtrend1 = (s0 - p1.x) * ((p2.y - p1.y) / (p2.x - p1.x)) + p1.y
        # 2nd trend line goes from (x2, y2) to (x3, y3)
        swtrend2 = (s0 - p2.x) * ((p3.y - p2.y) / (p3.x - p2.x)) + p2.y
        # 3rd trend line goes from (x3, y3) to (x4, y4)
        swtrend3 = (s0 - p3.x) * ((p4.y - p3.y) / (p4.x - p3.x)) + p3.y

        sw_flag = (
            (swh > 14)
            | ((s0 > p1.x) & (swh > 10))
            | ((s0 >= p1.x) & (s0 < p2.x) & (swh > swtrend1))
            | ((s0 >= p2.x) & (s0 < p3.x) & (swh > swtrend2))
            | ((s0 >= p2.x) & (swh > swtrend3))
        )

        prelim_flag = (
            ((surfc == 0) | (surfc == 2))
            & (kqual == 0)
            & ((rain == 0) | (rain == 3) | (rain == 5))
            & ((np.abs(ssha) < 5) & (basin_flag > 0) & (basin_flag < 1000))
            & ~((basin_flag > 0) & (basin_flag < 1000) & (abs(lats) > 60) & (abs(ssha) > 1.2))
        )

        swp_flag = prelim_flag & ~sw_flag

        rolling_median = pd.Series(ssha[swp_flag]).rolling(n_median, center=True, min_periods=1).median().values
        dx_median = ssha[swp_flag] - rolling_median

        outlier_index = np.abs(dx_median) < 2
        pd_roll = pd.Series(np.square(dx_median[outlier_index])).rolling(n_std, center=True, min_periods=1)
        rolling_std = np.clip(np.sqrt(pd_roll.median().values), 0.02, None)

        median_interp = np.interp(timestamps, timestamps[swp_flag], rolling_median)
        dx = ssha - median_interp
        std_interp = np.interp(timestamps, timestamps[swp_flag][outlier_index], rolling_std)

        median_flag = abs(dx) > std_interp * 5
        nasa_flag = ~(
            (~np.isnan(ssha))
            & ((surfc == 0) | (surfc == 2))
            & (kqual == 0)
            & ((rain == 0) | (rain == 3) | (rain == 5))
            & (rqual == 0)
            & (~median_flag)
            & ~((basin_flag > 0) & (basin_flag < 1000) & (abs(lats) > 60) & (abs(ssha) > 1.2))
        )

        source_flag = np.array([kqual, surfc, rqual, rain], dtype=np.int8).T

        self.assign_flags(nasa_flag, median_flag, source_flag)

    def assign_flags(self, nasa_flag, median_flag, source_flag):
        self.ds["nasa_flag"] = (
            ("time"),
            nasa_flag,
            {
                "flag_derivation": (
                    "nasa_flag is set to 0 for data that should be retained, and 1 for data that should be removed. nasa_flag is 0 if: "
                    "basin_flag is set to any valid, non-fill value & data passes an along-track median check, saved in the medain_filter_flag variable & the "
                    "following source_flag values are set to 0: surface_classification_flag (0 or 2), rain_flag, range_ocean_qual, rad_water_vapor_qual, and derived standard deviation"
                )
            },
        )

        source_flag_attrs = {
            "standard_name": "quality_flag",
            "long_name": "Source data flag",
            "comment": "S6 flags used to calculate nasa_flag. See documentation for more details.",
        }
        source_flag_attrs["flag_values"] = np.array([0, 1], dtype=np.int8)
        source_flag_attrs["flag_meanings"] = "good bad"

        for i, src_flag in enumerate(
            [
                "range_ocean_qual",
                "surface_classification_flag",
                "rad_water_vapor_qual",
                "rain_flag",
            ],
            1,
        ):
            source_flag_attrs[f"flag_column_{i}"] = src_flag

        self.ds["source_flag"] = (
            ("time", "src_flag_dim"),
            source_flag,
            source_flag_attrs,
        )

        self.ds["median_filter_flag"] = (
            ("time"),
            median_flag,
            {
                "standard_name": "quality_flag",
                "long_name": "median filter flag",
                "comment": "flag set to 0 for good data, 1 for data that fail a 5 standard deviation filter relative to a 15-point along-track median. See documentation for details.",
                "flag_values": np.array([0, 1], dtype=np.int8),
                "flag_meanings": "good bad",
            },
        )

    def mss_swap(self):
        logging.info("Applying mss swap to ssha values...")
        if len(self.ds["time"]) == 0:
            logging.debug("Empty data arrays, skipping mss swapping")
            return
        mss_path = os.path.join("daily_files", "ref_files", "mss_diffs", self.mss_name)
        mss_swapped_values = self.get_mss_values(mss_path)
        self.ds["ssha"].values = (
            self.ds["ssha"].values
            + self.ds["mean_sea_surface_sol1"]
            - self.ds["mean_sea_surface_sol2"]
            + mss_swapped_values
        )
        self.ds = self.ds.drop_vars(["mean_sea_surface_sol1", "mean_sea_surface_sol2"])

    def set_source_attrs(self):
        """
        Sets S6 specific global attributes
        """
        sources = set()
        source_urls = set()
        references = set()

        for collection_id in self.collection_ids:
            collection_meta: CollectionMeta = AllCollections.collections[collection_id]
            sources.add(collection_meta.source)
            source_urls.add(collection_meta.source_url)
            references.add(collection_meta.reference)

        self.ds.attrs["source"] = ", and ".join(sorted(sources))
        self.ds.attrs["source_url"] = ", and ".join(sorted(source_urls))
        self.ds.attrs["references"] = ", and ".join(sorted(references))
        self.ds.attrs["mean_sea_surface"] = self.target_mss
