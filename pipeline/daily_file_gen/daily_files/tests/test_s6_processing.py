import logging
import unittest
from datetime import datetime
from unittest import mock

import numpy as np
import xarray as xr
from daily_files.config.dataset_schema import validate_dataset
from daily_files.config.source_config import get_source_config
from daily_files.daily_file_job import save_ds
from daily_files.ingestion.ingest import IngestedData
from daily_files.processing.daily_file import DailyFile
from daily_files.processing.s6_daily_file import S6DailyFile


def _make_s6_ingested_data(n=500, date=datetime(2023, 12, 17)):
    """Build a synthetic IngestedData mimicking S6 ingestor output."""
    rng = np.random.RandomState(99)
    times = np.arange(
        np.datetime64(date),
        np.datetime64(date) + np.timedelta64(1, "D"),
        np.timedelta64(86400 // n, "s"),
    ).astype("datetime64[ns]")[:n]

    ssha = rng.normal(0, 0.1, n)
    lats = np.linspace(-66, 66, n)
    lons = np.linspace(0, 360, n, endpoint=False)
    cycles = np.full(n, 200, dtype=np.int32)
    passes = np.tile(np.arange(1, 11), n // 10 + 1)[:n].astype(np.int32)
    dac = rng.normal(0, 0.01, n)

    # Build a minimal original_ds with the fields S6DailyFile.make_nasa_flag expects
    original_ds = xr.Dataset(
        {
            "range_ocean_nr_qual": (
                ("time",),
                rng.choice([0, 1], n, p=[0.95, 0.05]).astype(np.int8),
            ),
            "surface_classification_flag": (
                ("time",),
                rng.choice([0, 2, 1], n, p=[0.8, 0.15, 0.05]).astype(np.int8),
            ),
            "rad_water_vapor_qual": (
                ("time",),
                rng.choice([0, 1], n, p=[0.95, 0.05]).astype(np.int8),
            ),
            "rain_flag_nr": (
                ("time",),
                rng.choice([0, 3, 5, 1], n, p=[0.8, 0.1, 0.05, 0.05]).astype(np.int8),
            ),
            "sig0_ocean_nr": (("time",), rng.uniform(8, 20, n)),
            "swh_ocean_nr": (("time",), rng.uniform(0, 5, n)),
            "ssha_nr": (("time",), ssha),
        },
        coords={"time": times},
    )

    mean_sea_surface_sol1 = rng.normal(30, 5, n)
    mean_sea_surface_sol2 = mean_sea_surface_sol1 + rng.normal(0, 0.01, n)

    inv_bar_cor = rng.normal(0, 0.01, n)

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
            "mean_sea_surface_sol1": mean_sea_surface_sol1,
            "mean_sea_surface_sol2": mean_sea_surface_sol2,
        },
    )


class TestS6Processing(unittest.TestCase):
    daily_ds: xr.Dataset

    @classmethod
    def setUpClass(cls):
        logging.root.handlers = []
        logging.basicConfig(
            level="INFO",
            format="[%(levelname)s] %(asctime)s - %(message)s",
            handlers=[logging.StreamHandler()],
        )

        mss_patcher = mock.patch.object(DailyFile, "get_mss_values", return_value=0.0)
        mss_patcher.start()
        cls.addClassCleanup(mss_patcher.stop)

        cls.date = datetime(2023, 12, 17)
        source_config = get_source_config("S6")
        ingested = _make_s6_ingested_data(date=cls.date)
        cls.daily_ds = S6DailyFile(
            ingested,
            cls.date,
            source_config,
            source_files="test_granule.nc",
        ).ds

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

    def test_no_mss_sol_vars(self):
        """mean_sea_surface_sol1/sol2 should be dropped after mss_swap."""
        self.assertNotIn("mean_sea_surface_sol1", self.daily_ds)
        self.assertNotIn("mean_sea_surface_sol2", self.daily_ds)

    def test_file_date_coverage(self):
        if len(self.daily_ds["time"]) == 0:
            self.skipTest("No data after subsetting")
        self.assertGreaterEqual(
            self.daily_ds["time"].values.min(), np.datetime64("2023-12-17")
        )
        self.assertLessEqual(
            self.daily_ds["time"].values.max(), np.datetime64("2023-12-17T23:59:59")
        )

    def test_nasa_flag_values(self):
        flag_vals = np.unique(self.daily_ds["nasa_flag"].values)
        for v in flag_vals:
            self.assertIn(v, [0, 1, True, False])

    def test_no_absolute_offset_attr(self):
        self.assertNotIn("absolute_offset_applied", self.daily_ds.attrs)

    def test_mean_sea_surface_attr(self):
        self.assertEqual(self.daily_ds.attrs["mean_sea_surface"], "DTU21")

    def test_source_attrs_populated(self):
        for attr in ["source", "source_url", "references"]:
            self.assertNotEqual(self.daily_ds.attrs[attr], "")

    def test_source_flag_dimensions(self):
        """source_flag should have 4 columns matching real S6 data (src_flag_dim=4)."""
        self.assertEqual(self.daily_ds["source_flag"].shape[1], 4)

    def test_schema_validation(self):
        errors = validate_dataset(self.daily_ds)
        self.assertEqual(errors, [], f"Schema validation errors: {errors}")

    def test_save_ds(self):
        """Verify the dataset can be serialized to NetCDF without error."""
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "test_s6.nc")
            save_ds(self.daily_ds, path)
            self.assertTrue(os.path.exists(path))
            with xr.open_dataset(path) as ds:
                self.assertIn("ssha", ds)
