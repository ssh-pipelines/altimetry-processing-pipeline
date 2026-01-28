from dataclasses import dataclass
import logging
import numpy as np
import pandas as pd
from datetime import datetime

from daily_files.processing.daily_file import DailyFile
from daily_files.config.source_config import SourceConfig
from daily_files.ingestion.ingest import IngestedData


class S6DailyFile(DailyFile):
    def __init__(
        self,
        ingested_data: IngestedData,
        date: datetime,
        source_config: SourceConfig,
        collection_ids: list[str],
        source_files: str = "",
    ):
        self.original_ds = ingested_data.source_specific["original_ds"]
        self._ingested_source_specific = ingested_data.source_specific
        super().__init__(
            ingested_data, date, source_config, collection_ids, source_files
        )

    def _pre_process_setup(self):
        self.ds["mean_sea_surface_sol1"] = (
            ("time"),
            self._ingested_source_specific["mean_sea_surface_sol1"],
        )
        self.ds["mean_sea_surface_sol2"] = (
            ("time"),
            self._ingested_source_specific["mean_sea_surface_sol2"],
        )

    def make_nasa_flag(self):
        """ """
        logging.info("Making nasa_flag...")
        kqual = self.original_ds["range_ocean_nr_qual"].values
        surfc = self.original_ds["surface_classification_flag"].values
        rqual = self.original_ds["rad_water_vapor_qual"].values
        rain = self.original_ds["rain_flag_nr"].values
        s0 = self.original_ds["sig0_ocean_nr"].values
        swh = self.original_ds["swh_ocean_nr"].values
        ssha = self.original_ds["ssha_nr"].values
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
            & ~(
                (basin_flag > 0)
                & (basin_flag < 1000)
                & (abs(lats) > 60)
                & (abs(ssha) > 1.2)
            )
        )

        swp_flag = prelim_flag & ~sw_flag

        rolling_median = (
            pd.Series(ssha[swp_flag])
            .rolling(n_median, center=True, min_periods=1)
            .median()
            .values
        )
        dx_median = ssha[swp_flag] - rolling_median

        outlier_index = np.abs(dx_median) < 2
        pd_roll = pd.Series(np.square(dx_median[outlier_index])).rolling(
            n_std, center=True, min_periods=1
        )
        rolling_std = np.clip(np.sqrt(pd_roll.median().values), 0.02, None)

        median_interp = np.interp(timestamps, timestamps[swp_flag], rolling_median)
        dx = ssha - median_interp
        std_interp = np.interp(
            timestamps, timestamps[swp_flag][outlier_index], rolling_std
        )

        median_flag = abs(dx) > std_interp * 5
        nasa_flag = ~(
            (~np.isnan(ssha))
            & ((surfc == 0) | (surfc == 2))
            & (kqual == 0)
            & ((rain == 0) | (rain == 3) | (rain == 5))
            & (rqual == 0)
            & (~median_flag)
            & ~(
                (basin_flag > 0)
                & (basin_flag < 1000)
                & (abs(lats) > 60)
                & (abs(ssha) > 1.2)
            )
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
                    "following source_flag values are set to 0: surface_classification_flag (0 or 2), rain_flag_nr, range_ocean_nr_qual, rad_water_vapor_qual, and derived standard deviation"
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
                "range_ocean_nr_qual",
                "surface_classification_flag",
                "rad_water_vapor_qual",
                "rain_flag_nr",
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

    def _source_mss_correction(self):
        return (
            self.ds["mean_sea_surface_sol1"].values
            - self.ds["mean_sea_surface_sol2"].values
        )

    def _post_mss_swap(self):
        self.ds = self.ds.drop_vars(["mean_sea_surface_sol1", "mean_sea_surface_sol2"])
