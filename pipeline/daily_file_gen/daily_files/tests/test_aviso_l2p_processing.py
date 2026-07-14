import logging
import os
import tempfile
import unittest
from datetime import datetime

import numpy as np
import xarray as xr
from daily_files.config.dataset_schema import validate_dataset
from daily_files.config.source_config import get_source_config
from daily_files.daily_file_job import save_ds
from daily_files.ingestion.ingest import IngestedData
from daily_files.processing import dtu21
from daily_files.processing.aviso_l2p_daily_file import AvisoL2PDailyFile
from scipy.interpolate import RegularGridInterpolator


def _make_aviso_l2p_ingested_data(n=500, date=datetime(2025, 1, 7)):
    """Build a synthetic IngestedData mimicking AvisoL2PIngestor output."""
    rng = np.random.RandomState(31)
    times = np.arange(
        np.datetime64(date),
        np.datetime64(date) + np.timedelta64(1, "D"),
        np.timedelta64(86400 // n, "s"),
    ).astype("datetime64[ns]")[:n]

    ssha = rng.normal(0, 0.1, n)
    lats = np.linspace(-66, 81, n)
    lons = np.linspace(232, 358, n)
    cycles = np.full(n, 101, dtype=np.int32)
    passes = np.full(n, 745, dtype=np.int32)
    dac = rng.normal(0, 0.01, n)
    inv_bar_cor = np.zeros(n, dtype=np.float64)

    mean_sea_surface = rng.normal(20, 5, n)
    inter_mission_bias = np.full(n, -0.047)
    validation_flag = rng.choice([0, 1], n, p=[0.9, 0.1]).astype(np.int8)

    # Carry the same original_ds shape AvisoL2PIngestor would emit, in case
    # downstream callers ever inspect it (the processor doesn't today).
    original_ds = xr.Dataset(
        {
            "validation_flag": (("time",), validation_flag),
            "mean_sea_surface": (("time",), mean_sea_surface),
            "inter_mission_bias": (("time",), inter_mission_bias),
        },
        coords={"time": times},
    )

    return IngestedData(
        ssha=ssha,
        lat=lats,
        lon=lons,
        time=times,
        cycles=cycles,
        passes=passes,
        dac=dac,
        inv_bar_cor=inv_bar_cor,
        source_specific={
            "original_ds": original_ds,
            "mean_sea_surface": mean_sea_surface,
            "inter_mission_bias": inter_mission_bias,
            "validation_flag": validation_flag,
        },
    )


def _synthetic_dtu21() -> RegularGridInterpolator:
    """A 100×100 RegularGridInterpolator over the whole globe — values are a
    smooth analytic function of (lat, lon), at MSS-realistic magnitudes."""
    lat = np.linspace(-90.0, 90.0, 100)
    lon = np.linspace(0.0, 360.0, 100)
    values = (
        20.0
        + 0.1 * lat.reshape(-1, 1)
        + 0.01 * lon.reshape(1, -1)
    ).astype(np.float32)
    return RegularGridInterpolator(
        points=(lat, lon),
        values=values,
        method="linear",
        bounds_error=False,
        fill_value=np.nan,
    )


class TestAvisoL2PProcessing(unittest.TestCase):
    daily_ds: xr.Dataset

    @classmethod
    def setUpClass(cls):
        logging.root.handlers = []
        logging.basicConfig(
            level="WARNING",
            format="[%(levelname)s] %(asctime)s - %(message)s",
            handlers=[logging.StreamHandler()],
        )

        dtu21.set_interpolator_for_test(_synthetic_dtu21())

        cls.date = datetime(2025, 1, 7)
        source_config = get_source_config("S3B")
        ingested = _make_aviso_l2p_ingested_data(date=cls.date)
        cls.daily_ds = AvisoL2PDailyFile(
            ingested,
            cls.date,
            source_config,
            source_files="s3b_test_granule.nc.gz",
        ).ds

    @classmethod
    def tearDownClass(cls):
        dtu21.set_interpolator_for_test(None)

    def test_has_required_vars(self):
        for var in [
            "ssha",
            "ssha_smoothed",
            "nasa_flag",
            "source_flag",
            "median_filter_flag",
            "dac",
            "inv_bar_cor",
            "basin_flag",
            "basin_names_table",
            "latitude",
            "longitude",
            "cycle",
            "pass",
        ]:
            self.assertIn(var, self.daily_ds, f"Missing variable: {var}")

    def test_inter_mission_bias_retained(self):
        """The plan calls for retaining `inter_mission_bias` on the dataset
        through P1 (and P2). It must NOT be dropped during processing."""
        self.assertIn("inter_mission_bias", self.daily_ds)

    def test_mss_and_dtu21_dropped_post_swap(self):
        self.assertNotIn("mean_sea_surface", self.daily_ds)
        self.assertNotIn("dtu21_interpolated", self.daily_ds)

    def test_source_flag_is_single_column(self):
        self.assertEqual(self.daily_ds["source_flag"].shape[1], 1)
        self.assertEqual(
            self.daily_ds["source_flag"].attrs.get("flag_column_1"),
            "validation_flag",
        )

    def test_target_mss_is_dtu21(self):
        self.assertEqual(self.daily_ds.attrs["mean_sea_surface"], "DTU21")
        self.assertEqual(
            self.daily_ds["ssha"].attrs["mean_sea_surface"], "DTU21"
        )

    def test_ssha_comment_explains_inter_mission_bias(self):
        comment = self.daily_ds["ssha"].attrs.get("comment", "")
        self.assertIn("inter_mission_bias", comment)
        self.assertIn("No AVISO inter-mission bias has been applied", comment)

    def test_date_subsetting(self):
        if len(self.daily_ds["time"]) == 0:
            self.skipTest("No data after subsetting")
        self.assertGreaterEqual(
            self.daily_ds["time"].values.min(),
            np.datetime64("2025-01-07"),
        )
        self.assertLessEqual(
            self.daily_ds["time"].values.max(),
            np.datetime64("2025-01-07T23:59:59"),
        )

    def test_nasa_flag_values(self):
        flag_vals = np.unique(self.daily_ds["nasa_flag"].values)
        for v in flag_vals:
            self.assertIn(v, [0, 1, True, False])

    def test_schema_validation(self):
        errors = validate_dataset(self.daily_ds)
        self.assertEqual(errors, [], f"Schema validation errors: {errors}")

    def test_save_ds_roundtrip(self):
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "test_s3b.nc")
            save_ds(self.daily_ds, path)
            self.assertTrue(os.path.exists(path))
            with xr.open_dataset(path) as ds:
                self.assertIn("ssha", ds)
                self.assertIn("inter_mission_bias", ds)


if __name__ == "__main__":
    unittest.main()
