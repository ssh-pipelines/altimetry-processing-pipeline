import logging
import os
import tempfile
import unittest
from datetime import datetime

import numpy as np
import xarray as xr
from scipy.interpolate import RegularGridInterpolator

from daily_files.config.dataset_schema import validate_dataset
from daily_files.config.source_config import get_source_config
from daily_files.daily_file_job import save_ds
from daily_files.ingestion.aviso_l2p_ingest import AvisoL2PIngestor
from daily_files.processing import dtu21
from daily_files.processing.aviso_l2p_daily_file import AvisoL2PDailyFile


_FIXTURE = os.path.join(
    os.path.dirname(__file__),
    "fixtures",
    "global_sla_l2p_ntc_s3b_C0101_P0745_20250106T234659_20250107T003225_20250213T162727.nc",
)


def _build_synthetic_dtu21_for_sample(margin_deg: float = 5.0, n: int = 100):
    """Return a 100×100 RegularGridInterpolator over the lat/lon bbox of the
    S3B sample (plus margin). Values are an analytic function of (lat, lon) so
    the interpolated MSS is deterministic without loading the real grid."""
    with xr.open_dataset(_FIXTURE) as ds:
        lat_lo = float(np.nanmin(ds["latitude"].values)) - margin_deg
        lat_hi = float(np.nanmax(ds["latitude"].values)) + margin_deg
        lon_lo = float(np.nanmin(ds["longitude"].values)) - margin_deg
        lon_hi = float(np.nanmax(ds["longitude"].values)) + margin_deg

    lat_lo = max(lat_lo, -90.0)
    lat_hi = min(lat_hi, 90.0)
    lon_lo = max(lon_lo, 0.0)
    lon_hi = min(lon_hi, 360.0)

    lat = np.linspace(lat_lo, lat_hi, n)
    lon = np.linspace(lon_lo, lon_hi, n)
    # Smooth surface ~ tens of meters, like real MSS magnitudes
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

        # Inject a synthetic DTU21 so CI doesn't have to load the bundled grid.
        dtu21.set_interpolator_for_test(_build_synthetic_dtu21_for_sample())

        ingestor = AvisoL2PIngestor()
        with open(_FIXTURE, "rb") as f:
            ingested = ingestor.ingest([f])

        # Sample granule spans 2025-01-06T23:46 → 2025-01-07T00:32, so date
        # selection retains the records on either side depending on the date.
        cls.date = datetime(2025, 1, 7)
        source_config = get_source_config("S3B")
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
