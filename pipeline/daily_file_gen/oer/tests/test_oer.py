"""
Tests for OER processing pipeline.

ConsistencyTestCase ensures output remains identical throughout refactoring
by comparing against known-good reference output.

EmptyInputTestCase verifies correct behavior when no crossover data falls
within the target day window.

ApplyCorrectionTestCase validates edge cases in the correction application step.
"""
import gzip
import logging
import os
import shutil
import tempfile
import unittest
from datetime import datetime
from glob import glob

import numpy as np
import xarray as xr
from oer.compute_polygon_correction import (
    apply_correction,
    create_polygon,
    evaluate_correction,
)

SAMPLE_DATA_DIR = os.path.join(os.path.dirname(__file__), "sample_data")


def _decompress_gz_files(src_dir, dest_dir):
    """Decompress all .nc.gz files from src_dir into dest_dir as .nc files."""
    os.makedirs(dest_dir, exist_ok=True)
    for filename in os.listdir(src_dir):
        if filename.endswith(".nc.gz"):
            src_path = os.path.join(src_dir, filename)
            dest_path = os.path.join(dest_dir, filename[:-3])  # strip .gz
            with gzip.open(src_path, "rb") as f_in, open(dest_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)


logging.root.handlers = []
logging.basicConfig(
    level="INFO",
    format="[%(levelname)s] %(asctime)s - %(message)s",
    handlers=[logging.StreamHandler()],
)


class ConsistencyTestCase(unittest.TestCase):
    """Test OER pipeline output against reference data."""

    FLOAT_TOLERANCE = 1e-10
    OER_TOLERANCE = 1e-10
    # Polynomial coefficients from np.linalg.solve are sensitive to
    # machine-level floating point differences across environments.
    COEF_TOLERANCE = 1e-9

    @classmethod
    def setUpClass(cls) -> None:
        """Decompress sample data, run pipeline, and load reference output."""
        cls.tmpdir = tempfile.mkdtemp()
        tmp_inputs = os.path.join(cls.tmpdir, "inputs")
        tmp_output = os.path.join(cls.tmpdir, "output")
        _decompress_gz_files(
            os.path.join(SAMPLE_DATA_DIR, "sample_inputs"), tmp_inputs
        )
        _decompress_gz_files(
            os.path.join(SAMPLE_DATA_DIR, "sample_output"), tmp_output
        )

        cls.date = datetime(2025, 1, 1)
        cls.source = "S6"

        # Load crossover inputs
        xover_files = sorted(glob(os.path.join(tmp_inputs, "xovers_*.nc")))
        xover_ds = xr.open_mfdataset(
            xover_files, concat_dim="time1", combine="nested", decode_times=False
        )

        # Step 1: create polygon
        cls.computed_polygon = create_polygon(xover_ds, cls.date, cls.source)

        # Load daily file
        daily_file_ds = xr.open_dataset(
            os.path.join(tmp_inputs, "S6_alt_ref_at_v1_1_20250101.nc")
        )

        # Step 2: evaluate correction
        cls.computed_correction = evaluate_correction(
            cls.computed_polygon, daily_file_ds, cls.date, cls.source
        )

        # Step 3: apply correction (mutates daily_file_ds in-place)
        cls.computed_p2 = apply_correction(daily_file_ds, cls.computed_correction)

        # Load reference outputs
        # decode_times=False for polygon to keep tbrk as raw float hours
        cls.ref_polygon = xr.open_dataset(
            os.path.join(tmp_output, "oerpoly_S6_2025-01-01.nc"),
            decode_times=False,
        )
        cls.ref_correction = xr.open_dataset(
            os.path.join(tmp_output, "oer_correction_S6_2025-01-01.nc"),
        )
        cls.ref_p2 = xr.open_dataset(
            os.path.join(tmp_output, "S6_alt_ref_at_v1_1_20250101.nc"),
        )

    @classmethod
    def tearDownClass(cls) -> None:
        for name in ("ref_polygon", "ref_correction", "ref_p2"):
            ds = getattr(cls, name, None)
            if ds is not None:
                ds.close()
        if hasattr(cls, "tmpdir"):
            shutil.rmtree(cls.tmpdir, ignore_errors=True)

    # ── Polygon tests ──────────────────────────────────────────────

    def test_polygon_dimensions(self):
        self.assertIn("N_order", self.computed_polygon.dims)
        self.assertIn("N_intervals", self.computed_polygon.dims)
        self.assertIn("N_breaks", self.computed_polygon.dims)

    def test_polygon_coef_shape(self):
        computed = self.computed_polygon["coef"].values
        reference = self.ref_polygon["coef"].values
        self.assertEqual(
            computed.shape,
            reference.shape,
            f"coef shape mismatch: computed={computed.shape}, reference={reference.shape}",
        )

    def test_polygon_coef_match(self):
        computed = self.computed_polygon["coef"].values
        reference = self.ref_polygon["coef"].values
        np.testing.assert_allclose(
            computed,
            reference,
            rtol=0,
            atol=self.COEF_TOLERANCE,
            err_msg="polygon coef values exceed tolerance from reference",
        )

    def test_polygon_tbrk_match(self):
        computed = self.computed_polygon["tbrk"].values
        reference = self.ref_polygon["tbrk"].values
        np.testing.assert_allclose(
            computed,
            reference,
            rtol=0,
            atol=self.FLOAT_TOLERANCE,
            err_msg="polygon tbrk values exceed tolerance from reference",
        )

    def test_polygon_rms_sig_match(self):
        computed = self.computed_polygon["rms_sig"].values
        reference = self.ref_polygon["rms_sig"].values
        np.testing.assert_allclose(
            computed,
            reference,
            rtol=0,
            atol=self.FLOAT_TOLERANCE,
            err_msg="polygon rms_sig values exceed tolerance from reference",
        )

    def test_polygon_rms_res_match(self):
        computed = self.computed_polygon["rms_res"].values
        reference = self.ref_polygon["rms_res"].values
        np.testing.assert_allclose(
            computed,
            reference,
            rtol=0,
            atol=self.FLOAT_TOLERANCE,
            err_msg="polygon rms_res values exceed tolerance from reference",
        )

    def test_polygon_nint_match(self):
        computed = self.computed_polygon["nint"].values
        reference = self.ref_polygon["nint"].values
        np.testing.assert_allclose(
            computed,
            reference,
            rtol=0,
            atol=self.FLOAT_TOLERANCE,
            err_msg="polygon nint values exceed tolerance from reference",
        )

    # ── Correction tests ───────────────────────────────────────────

    def test_correction_time_length(self):
        self.assertEqual(
            len(self.computed_correction["time"]),
            len(self.ref_correction["time"]),
            "correction time dimension length mismatch",
        )

    def test_correction_has_oer(self):
        self.assertIn("oer", self.computed_correction.data_vars)

    def test_correction_oer_match(self):
        computed = self.computed_correction["oer"].values
        reference = self.ref_correction["oer"].values
        np.testing.assert_allclose(
            computed,
            reference,
            rtol=0,
            atol=self.OER_TOLERANCE,
            err_msg="correction oer values exceed tolerance from reference",
        )

    # ── Applied correction (p2 daily file) tests ───────────────────

    def test_p2_has_oer(self):
        self.assertIn("oer", self.computed_p2.data_vars)

    def test_p2_oer_match(self):
        computed = self.computed_p2["oer"].values
        reference = self.ref_p2["oer"].values
        np.testing.assert_allclose(
            computed,
            reference,
            rtol=0,
            atol=self.OER_TOLERANCE,
            err_msg="p2 oer values exceed tolerance from reference",
        )

    def test_p2_ssha_match(self):
        computed = self.computed_p2["ssha"].values
        reference = self.ref_p2["ssha"].values
        np.testing.assert_allclose(
            computed,
            reference,
            rtol=0,
            atol=self.FLOAT_TOLERANCE,
            err_msg="p2 ssha values exceed tolerance from reference",
        )

    def test_p2_ssha_smoothed_match(self):
        computed = self.computed_p2["ssha_smoothed"].values
        reference = self.ref_p2["ssha_smoothed"].values
        np.testing.assert_allclose(
            computed,
            reference,
            rtol=0,
            atol=self.FLOAT_TOLERANCE,
            err_msg="p2 ssha_smoothed values exceed tolerance from reference",
        )

    def test_p2_product_generation_step(self):
        self.assertEqual(
            self.computed_p2.attrs["product_generation_step"],
            "2",
        )

    def test_p2_ssha_orbit_error_attr(self):
        self.assertIn(
            "orbit_error_correction",
            self.computed_p2["ssha"].attrs,
        )

    def test_p2_ssha_smoothed_orbit_error_attr(self):
        self.assertIn(
            "orbit_error_correction",
            self.computed_p2["ssha_smoothed"].attrs,
        )


class EmptyInputTestCase(unittest.TestCase):
    """Test create_polygon when no crossover data falls within the target day."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.date = datetime(2025, 1, 1)
        cls.source = "S6"

        # Build a minimal crossover dataset where all times are far outside
        # the polygon window (> pgon_t_margin from the target day).
        n = 10
        ref_timestamp = datetime(1990, 1, 1).timestamp()
        # Place times 30 days before the target day
        far_past = cls.date.timestamp() - 30 * 86400 - ref_timestamp
        times = np.linspace(far_past, far_past + 3600, n)

        cls.xover_ds = xr.Dataset(
            {
                "time1": ("time1", times),
                "time2": ("time1", times + 100),
                "ssh1": ("time1", np.random.default_rng(42).normal(0, 0.01, n)),
                "ssh2": ("time1", np.random.default_rng(43).normal(0, 0.01, n)),
                "cycle1": ("time1", np.ones(n)),
                "cycle2": ("time1", np.ones(n)),
                "pass1": ("time1", np.full(n, 100.0)),
                "pass2": ("time1", np.full(n, 200.0)),
            }
        )
        cls.polygon = create_polygon(cls.xover_ds, cls.date, cls.source)

    def test_zero_polynomial(self):
        """When no data falls in the day window, all coefficients should be zero."""
        coef = self.polygon["coef"].values
        self.assertTrue(
            np.all(coef == 0),
            "Expected all-zero coefficients for empty day window",
        )

    def test_dimensions_present(self):
        self.assertIn("N_order", self.polygon.dims)
        self.assertIn("N_intervals", self.polygon.dims)
        self.assertIn("N_breaks", self.polygon.dims)

    def test_tbrk_range(self):
        """Default tbrk should span from -3 to 27 hours."""
        tbrk = self.polygon["tbrk"].values
        self.assertEqual(tbrk[0], -3)
        self.assertEqual(tbrk[-1], 27)

    def test_rms_values_zero(self):
        self.assertTrue(np.all(self.polygon["rms_sig"].values == 0))
        self.assertTrue(np.all(self.polygon["rms_res"].values == 0))

    def test_nint_values_zero(self):
        self.assertTrue(np.all(self.polygon["nint"].values == 0))


class ApplyCorrectionTestCase(unittest.TestCase):
    """Test apply_correction edge cases."""

    def test_mismatched_time_raises(self):
        daily = xr.Dataset(
            {
                "time": ("time", [1, 2, 3]),
                "ssha": ("time", [0.1, 0.2, 0.3]),
                "ssha_smoothed": ("time", [0.1, 0.2, 0.3]),
            }
        )
        correction = xr.Dataset(
            {
                "time": ("time", [1, 2]),
                "oer": ("time", [0.01, 0.02]),
            }
        )
        with self.assertRaises(ValueError):
            apply_correction(daily, correction)

    def test_empty_time_no_mutation(self):
        """When time dimension is empty, ssha values should not change."""
        daily = xr.Dataset(
            {
                "time": ("time", np.array([], dtype="float64")),
                "ssha": ("time", np.array([], dtype="float64")),
                "ssha_smoothed": ("time", np.array([], dtype="float64")),
            }
        )
        correction = xr.Dataset(
            {
                "time": ("time", np.array([], dtype="float64")),
                "oer": ("time", np.array([], dtype="float64")),
            }
        )
        result = apply_correction(daily, correction)
        self.assertIn("oer", result.data_vars)
        self.assertEqual(result.attrs["product_generation_step"], "2")

    def test_oer_added_to_ssha(self):
        """OER values should be added to both ssha and ssha_smoothed."""
        ssha_orig = np.array([1.0, 2.0, 3.0])
        ssha_sm_orig = np.array([1.1, 2.1, 3.1])
        oer_vals = np.array([0.01, 0.02, 0.03])

        daily = xr.Dataset(
            {
                "time": ("time", [1, 2, 3]),
                "ssha": ("time", ssha_orig.copy()),
                "ssha_smoothed": ("time", ssha_sm_orig.copy()),
            }
        )
        correction = xr.Dataset(
            {
                "time": ("time", [1, 2, 3]),
                "oer": ("time", oer_vals),
            }
        )
        result = apply_correction(daily, correction)

        np.testing.assert_allclose(
            result["ssha"].values,
            ssha_orig + oer_vals,
            err_msg="ssha should equal original + oer",
        )
        np.testing.assert_allclose(
            result["ssha_smoothed"].values,
            ssha_sm_orig + oer_vals,
            err_msg="ssha_smoothed should equal original + oer",
        )


if __name__ == "__main__":
    unittest.main()
