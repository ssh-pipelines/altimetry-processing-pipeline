import logging
import os
import re
import xarray as xr
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Iterable, TextIO
from daily_files.processing.daily_file import DailyFile
from daily_files.collection_metadata import AllCollections, CollectionMeta
from utilities.aws_utils import aws_manager


class GSFCDailyFile(DailyFile):
    def __init__(self, file_objs: Iterable[TextIO], date: datetime, collection_ids: Iterable[str]):
        self.date = date

        opened_files = [xr.open_dataset(file_obj, engine="h5netcdf") for file_obj in file_objs]
        cycles = np.concatenate([np.full_like(ds["ssha"].values, ds.attrs["merged_cycle"]) for ds in opened_files])
        self.og_ds = xr.concat(opened_files, dim="N_Records")
        opened_files = []

        ssh: np.ndarray = self.og_ds["ssha"].values / 1000  # Convert from mm
        lats: np.ndarray = self.og_ds["lat"].values
        lons: np.ndarray = self.og_ds["lon"].values
        times: np.ndarray = self.og_ds["time"].values
        dac: np.ndarray = self.compute_dac(np.unique(cycles), ssh)
        cycles, passes = self.compute_cycles_passes(self.og_ds, cycles)
        self.collection_ids = collection_ids

        self.source_mss = "DTU15"
        self.target_mss = "DTU21"
        self.mss_name = f"{self.source_mss}_minus_{self.target_mss}.nc"

        super().__init__(ssh, lats, lons, times, cycles, passes, dac)

        self.make_daily_file_ds()

    def compute_cycles_passes(self, ds: xr.Dataset, cycles: np.ndarray) -> tuple[np.ndarray]:
        """
        Computes passes using look up table that converts a reference_orbit and index value to pass number.
        GSFC uses slightly different pass/cycle definitions. We need to increment cycle number in the ascending half below the equator
        of a pass where pass==1
        """
        logging.info("Computing pass values")
        df = pd.read_csv("daily_files/ref_files/complete_gsfc_pass_lut.csv", converters={"id": str}).set_index("id")

        # Convert reference_orbit and index from GSFC file to 7 digit long, left-padded string
        ds_ids = [
            str(orbit).zfill(3) + str(index).zfill(4)
            for orbit, index in zip(ds["reference_orbit"].values, ds["index"].values)
        ]
        passes = df.loc[ds_ids]["pass"].values

        # Use index where passes wrap back to 1 to select cycles values that require manual incrementing
        index_of_wrap = np.where(passes[:-1] > passes[1:])[0][0] + 1
        cycles[index_of_wrap:][(cycles[index_of_wrap:] == cycles[0]) & (passes[index_of_wrap:] == 1)] += 1
        return cycles, passes

    def compute_dac(self, unique_cycles: np.ndarray, ssh: np.ndarray) -> np.ndarray:
        """
        Loads corresponding NOIB cycle file(s) and subtracts "ssha_noib" from our ssh values
        """
        all_obj_ds = []
        for cycle_num in unique_cycles:
            logging.info(f"Streaming cycle {cycle_num}")
            noib_filename = f"Merged_TOPEX_Jason_OSTM_Jason-3_Sentinel-6_Cycle_{int(cycle_num):04}.V5_2.nc"

            src = os.path.join("s3://", "example-bucket", "aux_files", "GSFC_NOIB", noib_filename)
            try:
                obj = aws_manager.stream_obj(src)
            except Exception as e:
                raise RuntimeError(f"Unable to stream {src}: {e}")
            obj_ds = xr.open_dataset(obj, engine="h5netcdf")
            all_obj_ds.append(obj_ds)
        noib_ds: xr.Dataset = xr.concat(all_obj_ds, dim="N_Records")
        ssha_noib = noib_ds["ssha_noib"].values / 1000
        return ssha_noib - ssh

    def make_daily_file_ds(self):
        """
        Ordering of steps to create daily file from GSFC granule
        """
        self.map_points_to_basin()
        self.make_nasa_flag()
        self.clean_date(self.date)
        self.mss_swap()
        self.apply_basin_to_nasa()
        self.make_ssh_smoothed(self.date)
        self.set_metadata()
        self.set_source_attrs()

    def gsfc_flag_splitting(self) -> np.ndarray:
        """
        Breaks out individual GSFC flags from comprehensive flag
        """
        flag = self.og_ds["flag"].values
        max_bits = int(np.ceil(np.log2(flag.max())))
        binary_representation = (flag[:, None] & (1 << np.arange(max_bits))).astype(bool)
        return binary_representation

    def make_nasa_flag(self):
        """
        Makes nasa_flag, median_filter_flag, source_flag.

        GSFC flags:
        1: Radiometer_Observation_is_Suspect
        2: Attitude_Out_of_Range
        3: Sigma0_Ku_Band_Out_of_Range
        4: Possible_Rain_Contamination
        5: Sea_Ice_Detected
        9: Any_Applied_SSH_Correction_Out_of_Limits
        """
        logging.info("Converting GSFC flag to NASA flag")

        flag_array = self.gsfc_flag_splitting()

        surf_type = self.og_ds["Surface_Type"].values
        ssh = self.ds["ssh"].values
        basin_flag = self.ds["basin_flag"].values
        lats = self.ds["latitude"].values
        
        # Cycle 583 has incorrect "neighbor" flag values so we won't use it
        if 583 in np.unique(self.ds["cycle"].astype(int)):
            src_flag_indices = [1, 2, 3, 4, 5, 9]
        else:
            src_flag_indices = [0, 1, 2, 3, 4, 5, 9]

        prelim_flag = (
            ((surf_type == 0) | (surf_type == 2))
            & (~flag_array[:, src_flag_indices].any(axis=1))
            & (~np.isnan(ssh))
            & (~((basin_flag > 0) & (basin_flag < 1000) & (abs(lats) > 60) & (abs(ssh) > 1.2)))
        )

        # Calculate rolling median and standard deviation
        n_median = 15
        n_std = 95
        timestamps = np.arange(1, len(ssh) + 1)

        rolling_median = pd.Series(ssh[prelim_flag]).rolling(n_median, center=True, min_periods=1).median().values
        dx = ssh[prelim_flag] - rolling_median

        dx_median = pd.Series(np.square(dx)).rolling(n_std, center=True, min_periods=1).median().values
        rolling_std = np.clip(np.sqrt(dx_median), 0.05, None)

        median_interp = np.interp(timestamps, timestamps[prelim_flag], rolling_median)
        std_interp = np.interp(timestamps, timestamps[prelim_flag], rolling_std)

        median_flag = abs(ssh - median_interp) <= std_interp * 5

        nasa_flag = ~(
            ((surf_type == 0) | (surf_type == 2))
            & (~flag_array[:, [1, 2, 3, 5]].any(axis=1))
            & (~np.isnan(ssh))
            & median_flag
            & ~((basin_flag > 0) & (basin_flag < 1000) & (abs(lats) > 60) & (abs(ssh) > 1.2))
        )

        source_flag = np.array(flag_array).astype("bool")

        all_flag_meanings = re.split(r" (?=[A-Za-z_])", self.og_ds["flag"].attrs["flag_meanings"])

        # Assign nasa_flag to dataset
        self.ds["nasa_flag"] = (
            ("time"),
            nasa_flag.data,
            {
                "flag_derivation": f'nasa_flag is 0 if: basin_flag is set to any valid, non-fill value & data passes an along-track '
                f'median check, saved in the medain_filter_flag variable & the following source_flag values are set '
                f'to 0: {", ".join([all_flag_meanings[i] for i in [1,2,3,5]])}'
            },
        )

        # Assign source_flag to dataset
        source_flag_attrs = {
            "standard_name": "quality_flag",
            "long_name": "Source data flag",
            "comment": "GSFC flags used to calculate nasa_flag. See documentation for more details.",
        }
        for i, src_flag in enumerate(all_flag_meanings, 1):
            source_flag_attrs[f"flag_column_{i}"] = src_flag

        source_flag_attrs["flag_values"] = "0, 1"
        source_flag_attrs["flag_meanings"] = "good bad"
        self.ds["source_flag"] = (
            ("time", "src_flag_dim"),
            source_flag,
            source_flag_attrs,
        )

        # Assign median_filter_flag to dataset
        self.ds["median_filter_flag"] = (
            ("time"),
            ~median_flag,
            {
                "standard_name": "quality_flag",
                "long_name": "median filter flag",
                "comment": "flag set to 0 for good data, 1 for data that fail a 5 standard deviation filter relative "
                "to a 15-point along-track median. See documentation for details.",
                "flag_values": "0, 1",
                "flag_meanings": "good bad",
            },
        )

    def mss_swap(self):
        logging.info("Applying mss swap to ssh values...")
        if len(self.ds["time"]) == 0:
            logging.debug("Empty data arrays, skipping mss swapping")
            return
        mss_path = os.path.join("daily_files", "ref_files", "mss_diffs", self.mss_name)
        mss_swapped_values = self.get_mss_values(mss_path)
        self.ds["ssh"].values = self.ds["ssh"].values + mss_swapped_values

    def set_source_attrs(self):
        """
        Sets GSFC specific global attributes
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
        self.ds.attrs["geospatial_lat_min"] = "-67LL"
        self.ds.attrs["geospatial_lat_max"] = "67LL"
        self.ds.attrs["mean_sea_surface"] = self.target_mss
        self.ds.attrs["absolute_offset_applied"] = 0
