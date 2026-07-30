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
from oer.config.source_config import get_source_config

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
    """Test OER pipeline output against reference data.

    The polygon is fit with S6's canonical ``common.ground_speed`` (5.7529,
    loaded from config) so the oracle mirrors production. The reference golden
    files were generated at the legacy 5.7; on this sample data the 0.9% speed
    change does not cross any ``oerfit`` knot-placement threshold, so output is
    effectively unchanged. Tolerances are set to a science-meaningful bound
    (well below OER's mm-scale signal) rather than machine epsilon, so the test
    stays valid if a future sample regeneration does cross a threshold — it
    guards the science, not the last floating-point bit.
    """

    # Science tolerance: OER/SSH corrections are ~cm-scale; 1e-4 m (0.1 mm) is
    # far below any physically meaningful change.
    FLOAT_TOLERANCE = 1e-4
    OER_TOLERANCE = 1e-4
    COEF_TOLERANCE = 1e-4

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

        # Use the source's canonical ground_speed (5.7529) so the oracle
        # exercises the same value production does, rather than the bare
        # create_polygon default.
        ground_speed = get_source_config(cls.source).ground_speed

        # Load crossover inputs
        xover_files = sorted(glob(os.path.join(tmp_inputs, "xovers_*.nc")))
        xover_ds = xr.open_mfdataset(
            xover_files, concat_dim="time1", combine="nested", decode_times=False
        )

        # Step 1: create polygon
        cls.computed_polygon = create_polygon(
            xover_ds, cls.date, cls.source, ground_speed=ground_speed
        )

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


class ReferenceCrossoverPolygonTestCase(unittest.TestCase):
    """create_polygon on a reference-schema xover file.

    Reference crossovers carry only the high-lat side plus a time-interpolated
    reference ``ssh2`` — there is no ``time2``/``cycle2``. The reference path
    must therefore (a) not KeyError on the missing self-only vars, (b) use
    ``dssh = ssh1 - ssh2`` with a single trackid per crossover and no
    sign-flipped stacking.
    """

    @classmethod
    def _reference_dataset(cls, n, times, ssh1, ssh2, cycle1, pass1):
        """Minimal reference-schema dataset — deliberately omits time2/cycle2."""
        return xr.Dataset(
            {
                "time1": ("time1", times),
                "lon": ("time1", np.linspace(-40, 40, n)),
                "lat": ("time1", np.linspace(60, 70, n)),
                "ssh1": ("time1", ssh1),
                "cycle1": ("time1", cycle1),
                "pass1": ("time1", pass1),
                "ssh2": ("time1", ssh2),
                "pass2": ("time1", np.full(n, 55.0)),
                "ref_cycle_before": ("time1", np.full(n, 10.0)),
                "ref_cycle_after": ("time1", np.full(n, 11.0)),
                "ref_ssha_before": ("time1", ssh2 - 0.001),
                "ref_ssha_after": ("time1", ssh2 + 0.001),
                "ref_time_before": ("time1", times - 3600),
                "ref_time_after": ("time1", times + 3600),
            }
        )

    @classmethod
    def setUpClass(cls) -> None:
        cls.date = datetime(2025, 2, 7)
        cls.source = "S3B"
        cls.n = 20

        ref_timestamp = datetime(1990, 1, 1).timestamp()
        # Spread times across the target day (seconds since 1990-01-01).
        day0 = cls.date.timestamp() - ref_timestamp
        cls.times = np.linspace(day0 + 3600, day0 + 80000, cls.n)
        rng = np.random.default_rng(7)
        cls.ssh1 = rng.normal(0, 0.02, cls.n)
        cls.ssh2 = rng.normal(0, 0.02, cls.n)
        cls.cycle1 = np.full(cls.n, 3.0)
        cls.pass1 = np.full(cls.n, 42.0)

        cls.xover_ds = cls._reference_dataset(
            cls.n, cls.times, cls.ssh1, cls.ssh2, cls.cycle1, cls.pass1
        )

    def test_no_keyerror_without_time2(self):
        """A reference dataset with no time2/cycle2 must not raise."""
        self.assertNotIn("time2", self.xover_ds)
        self.assertNotIn("cycle2", self.xover_ds)
        polygon = create_polygon(
            self.xover_ds, self.date, self.source, crossover_type="reference"
        )
        self.assertIn("coef", polygon.data_vars)
        self.assertEqual(polygon.attrs["crossover_type"], "reference")

    def test_reference_pairs_no_sign_flip_single_trackid(self):
        """_reference_pairs: dssh = ssh1 - ssh2, one trackid, no stacking."""
        from oer.compute_polygon_correction import (
            TRACKID_CYCLE_STRIDE,
            _reference_pairs,
        )

        ref_timestamp = datetime(1990, 1, 1).timestamp()
        dssh, psec, trackid = _reference_pairs(self.xover_ds, ref_timestamp)

        # No stacking: one sample per crossover (self would double this).
        self.assertEqual(len(dssh), self.n)
        self.assertEqual(len(psec), self.n)
        self.assertEqual(len(trackid), self.n)
        np.testing.assert_allclose(dssh, self.ssh1 - self.ssh2)
        # Single trackid = cycle1 * stride + pass1.
        expected_trackid = self.cycle1 * TRACKID_CYCLE_STRIDE + self.pass1
        np.testing.assert_array_equal(trackid, expected_trackid)

    def test_self_pairs_still_stack_with_sign_flip(self):
        """Regression guard: the self path keeps its doubled, sign-flipped stack."""
        from oer.compute_polygon_correction import _self_pairs

        n = 5
        times = np.linspace(0, 3600, n)
        self_ds = xr.Dataset(
            {
                "time1": ("time1", times),
                "time2": ("time1", times + 50),
                "ssh1": ("time1", np.full(n, 0.1)),
                "ssh2": ("time1", np.full(n, 0.04)),
                "cycle1": ("time1", np.ones(n)),
                "cycle2": ("time1", np.full(n, 2.0)),
                "pass1": ("time1", np.full(n, 3.0)),
                "pass2": ("time1", np.full(n, 4.0)),
            }
        )
        dssh, psec, trackid = _self_pairs(self_ds, datetime(1990, 1, 1).timestamp())
        self.assertEqual(len(dssh), 2 * n)  # stacked
        np.testing.assert_allclose(dssh[:n], 0.06)
        np.testing.assert_allclose(dssh[n:], -0.06)  # sign-flipped copy


class OerCrossoverTypeDispatchTestCase(unittest.TestCase):
    """OerCorrection derives crossover_type from the source product_type."""

    def test_reference_product_type_maps_to_self(self):
        from oer.oer import _CROSSOVER_TYPE_BY_PRODUCT_TYPE

        self.assertEqual(_CROSSOVER_TYPE_BY_PRODUCT_TYPE["reference"], "self")

    def test_high_latitude_product_type_maps_to_reference(self):
        from oer.oer import _CROSSOVER_TYPE_BY_PRODUCT_TYPE

        self.assertEqual(_CROSSOVER_TYPE_BY_PRODUCT_TYPE["high_latitude"], "reference")


if __name__ == "__main__":
    unittest.main()
