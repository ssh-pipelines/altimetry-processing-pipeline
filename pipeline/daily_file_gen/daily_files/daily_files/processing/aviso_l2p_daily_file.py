import logging
from datetime import datetime

import numpy as np
import pandas as pd

from daily_files.config.source_config import SourceConfig
from daily_files.ingestion.ingest import IngestedData
from daily_files.processing.daily_file import DailyFile
from daily_files.processing.dtu21 import get_dtu21_interpolator


_SSHA_BIAS_COMMENT = (
    "No AVISO inter-mission bias has been applied to ssha; the per-record "
    "`inter_mission_bias` array is retained on the dataset as an auxiliary "
    "variable. Cross-mission calibration is performed downstream against "
    "reference-mission daily files."
)


class AvisoL2PDailyFile(DailyFile):
    """Processor for AVISO L2P high-latitude sources (S3B, planned S3A,
    SARAL/AltiKa, HY-2B). Named for the wire format, not any single source —
    all current and future L2P sources share this implementation.

    MSS handling (see ADR 0002): the L2P file's `mean_sea_surface` is swapped
    to project-canonical DTU21 by bilinear interpolation of a bundled global
    DTU21 grid at each granule's (lat, lon). AVISO's `inter_mission_bias` is
    retained on the output as an auxiliary variable but NOT applied to ssha;
    cross-mission calibration runs downstream against reference-mission files.
    """

    TARGET_MSS = "DTU21"

    def __init__(
        self,
        ingested_data: IngestedData,
        date: datetime,
        source_config: SourceConfig,
        source_files: str = "",
    ):
        self._ingested_source_specific = ingested_data.source_specific
        super().__init__(ingested_data, date, source_config, source_files)

    def _pre_process_setup(self):
        # High-latitude sources don't carry an MSS triple in config; the
        # output is referenced to DTU21 (see ADR 0002).
        self.target_mss = self.TARGET_MSS

        self.ds["mean_sea_surface"] = (
            ("time"),
            self._ingested_source_specific["mean_sea_surface"],
        )
        self.ds["inter_mission_bias"] = (
            ("time"),
            self._ingested_source_specific["inter_mission_bias"],
        )

        interp = get_dtu21_interpolator()
        query_points = np.column_stack(
            [self.ds["latitude"].values, self.ds["longitude"].values]
        )
        self.ds["dtu21_interpolated"] = (("time"), interp(query_points))

    def manual_outliers(self, ssha: np.ndarray) -> np.ndarray:
        """Apply config-driven bad_points (matched by time)."""
        outliers = np.full_like(ssha, False, dtype=bool)
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
        logging.info("Making nasa_flag for AVISO L2P...")
        validation_flag = self._ingested_source_specific["validation_flag"].astype(np.int8)
        ssha = self.ds["ssha"].values
        basin_flag = self.ds["basin_flag"].values

        n_median = 15
        n_std = 95
        timestamps = np.arange(1, len(ssha) + 1)

        prelim_flag = (
            (validation_flag == 0)
            & (basin_flag > 0)
            & (basin_flag < 1000)
            & (np.abs(ssha) < 5)
        )

        median_flag = np.zeros_like(ssha, dtype=bool)
        if prelim_flag.any():
            rolling_median = (
                pd.Series(ssha[prelim_flag])
                .rolling(n_median, center=True, min_periods=1)
                .median()
                .values
            )
            dx_median = ssha[prelim_flag] - rolling_median
            outlier_index = np.abs(dx_median) < 2
            if outlier_index.any():
                pd_roll = pd.Series(np.square(dx_median[outlier_index])).rolling(
                    n_std, center=True, min_periods=1
                )
                rolling_std = np.clip(np.sqrt(pd_roll.median().values), 0.02, None)
                median_interp = np.interp(
                    timestamps, timestamps[prelim_flag], rolling_median
                )
                dx = ssha - median_interp
                std_interp = np.interp(
                    timestamps, timestamps[prelim_flag][outlier_index], rolling_std
                )
                median_flag = np.abs(dx) > std_interp * 5

        nasa_flag = ~(
            (~np.isnan(ssha))
            & (validation_flag == 0)
            & (~median_flag)
        )

        outliers = self.manual_outliers(ssha)
        nasa_flag[outliers] = True

        source_flag = validation_flag.reshape(-1, 1).astype(np.int8)

        self.assign_flags(nasa_flag, median_flag, source_flag)

    def assign_flags(self, nasa_flag, median_flag, source_flag):
        self.ds["nasa_flag"] = (
            ("time"),
            nasa_flag,
            {
                "flag_derivation": (
                    "nasa_flag is set to 0 for data that should be retained, and 1 for "
                    "data that should be removed. nasa_flag is 0 if: basin_flag is set "
                    "to any valid, non-fill value & data passes an along-track median "
                    "check, saved in the median_filter_flag variable & the following "
                    "source_flag values are set to 0: validation_flag"
                )
            },
        )

        self.ds["source_flag"] = (
            ("time", "src_flag_dim"),
            source_flag,
            {
                "standard_name": "quality_flag",
                "long_name": "Source data flag",
                "comment": "AVISO L2P validation flag. See documentation for more details.",
                "coverage_content_type": "auxiliaryInformation",
                "flag_column_1": "validation_flag",
                "flag_values": np.array([0, 1], dtype=np.int8),
                "flag_meanings": "good bad",
            },
        )

        self.ds["median_filter_flag"] = (
            ("time"),
            median_flag,
            {
                "standard_name": "quality_flag",
                "long_name": "median filter flag",
                "comment": (
                    "flag set to 0 for good data, 1 for data that fail a 5 standard "
                    "deviation filter relative to a 15-point along-track median. See "
                    "documentation for details."
                ),
                "flag_values": np.array([0, 1], dtype=np.int8),
                "flag_meanings": "good bad",
            },
        )

    def _source_mss_correction(self) -> np.ndarray:
        return (
            self.ds["mean_sea_surface"].values
            - self.ds["dtu21_interpolated"].values
        )

    def _post_mss_swap(self):
        self.ds = self.ds.drop_vars(["mean_sea_surface", "dtu21_interpolated"])

    def set_source_attrs(self):
        super().set_source_attrs()
        existing = self.ds["ssha"].attrs.get("comment", "")
        self.ds["ssha"].attrs["comment"] = (
            f"{existing} {_SSHA_BIAS_COMMENT}".strip() if existing else _SSHA_BIAS_COMMENT
        )
