import logging
import re
import numpy as np
import pandas as pd
from datetime import datetime
from daily_files.processing.daily_file import DailyFile
from daily_files.config.source_config import SourceConfig
from daily_files.ingestion.ingest import IngestedData


class GSFCDailyFile(DailyFile):
    def __init__(
        self,
        ingested_data: IngestedData,
        date: datetime,
        source_config: SourceConfig,
        collection_ids: list[str],
        source_files: str = "",
    ):
        self.og_ds = ingested_data.source_specific["og_ds"]
        super().__init__(ingested_data, date, source_config, collection_ids, source_files)

    def gsfc_flag_splitting(self) -> np.ndarray:
        """
        Breaks out individual GSFC flags from comprehensive flag
        """
        flag = self.og_ds["flag"].values
        max_bits = int(np.ceil(np.log2(flag.max())))
        binary_representation = (flag[:, None] & (1 << np.arange(max_bits))).astype(bool)
        return binary_representation

    def manual_outliers(self, ssha: np.ndarray, prelim_flag: np.ndarray, lat: np.ndarray) -> np.ndarray:
        """
        Manual method for catching known bad values
        """
        # 1995-06-07
        if self.date == datetime(1995, 6, 7):
            outliers = prelim_flag & (lat >= 20) & (lat <= 25) & (ssha < -1)

        # 2001-06-26
        elif self.date == datetime(2001, 6, 26):
            outliers = prelim_flag & (lat >= -25) & (lat <= -15) & (ssha < -1)

        else:
            outliers = np.full_like(ssha, False, dtype=bool)

        # Apply config-driven bad points (matched by time)
        bad_points = self.source_config.bad_points
        if bad_points:
            date_key = self.date.date()
            if date_key in bad_points:
                times = self.ds["time"].values.astype("datetime64[s]")
                for entry in bad_points[date_key]:
                    bad_time = np.datetime64(entry["time"], "s")
                    outliers |= times == bad_time

        return outliers

    def make_nasa_flag(self):
        """
        Makes nasa_flag, median_filter_flag, source_flag.

        GSFC flags:
        0: Neighboring cycle
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
        ssha = self.ds["ssha"].values
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
            & (~np.isnan(ssha))
            & (~((basin_flag > 0) & (basin_flag < 1000) & (abs(lats) > 60) & (abs(ssha) > 1.2)))
        )

        # Calculate rolling median and standard deviation
        n_median = 15
        n_std = 95
        timestamps = np.arange(1, len(ssha) + 1)

        rolling_median = pd.Series(ssha[prelim_flag]).rolling(n_median, center=True, min_periods=1).median().values
        dx = ssha[prelim_flag] - rolling_median

        dx_median = pd.Series(np.square(dx)).rolling(n_std, center=True, min_periods=1).median().values
        rolling_std = np.clip(np.sqrt(dx_median), 0.05, None)

        median_interp = np.interp(timestamps, timestamps[prelim_flag], rolling_median)
        std_interp = np.interp(timestamps, timestamps[prelim_flag], rolling_std)

        median_flag = abs(ssha - median_interp) <= std_interp * 5

        nasa_flag = ~(
            ((surf_type == 0) | (surf_type == 2))
            & (~flag_array[:, [1, 2, 3, 5]].any(axis=1))
            & (~np.isnan(ssha))
            & median_flag
            & ~((basin_flag > 0) & (basin_flag < 1000) & (abs(lats) > 60) & (abs(ssha) > 1.2))
        )

        outliers = self.manual_outliers(ssha, prelim_flag, lats)
        nasa_flag[outliers] = 1

        source_flag = np.array(flag_array).astype("bool")

        all_flag_meanings = re.split(r" (?=[A-Za-z_])", self.og_ds["flag"].attrs["flag_meanings"])

        # Assign nasa_flag to dataset
        self.ds["nasa_flag"] = (
            ("time"),
            nasa_flag.data,
            {
                "flag_derivation": f"nasa_flag is 0 if: basin_flag is set to any valid, non-fill value & data passes an along-track "
                f"median check, saved in the medain_filter_flag variable & the following source_flag values are set "
                f"to 0: {', '.join([all_flag_meanings[i] for i in [1, 2, 3, 5]])}"
            },
        )

        # Assign source_flag to dataset
        source_flag_attrs = {
            "standard_name": "quality_flag",
            "long_name": "Source data flag",
            "comment": "GSFC flags used to calculate nasa_flag. See documentation for more details.",
            "coverage_content_type": "auxiliaryInformation",
        }
        for i, src_flag in enumerate(all_flag_meanings, 1):
            source_flag_attrs[f"flag_column_{i}"] = src_flag

        source_flag_attrs["flag_values"] = np.array([0, 1], dtype=np.int8)
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
                "flag_values": np.array([0, 1], dtype=np.int8),
                "flag_meanings": "good bad",
                "coverage_content_type": "auxiliaryInformation",
            },
        )

    def set_source_attrs(self):
        super().set_source_attrs()
        self.ds.attrs["absolute_offset_applied"] = 0
