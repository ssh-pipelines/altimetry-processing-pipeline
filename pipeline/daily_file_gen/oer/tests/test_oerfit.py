"""
Unit tests for the oerfit spline fitting algorithm.

InputValidationTestCase verifies that mismatched array inputs are rejected.

OutputShapeTestCase validates the structure of oerfit return values using
a small synthetic dataset.
"""
import unittest

import numpy as np

from oer.oerfit import oerfit


class InputValidationTestCase(unittest.TestCase):
    """Test that oerfit rejects invalid inputs."""

    def test_ptime_dssh_size_mismatch(self):
        ptime = np.array([1.0, 2.0, 3.0])
        dssh = np.array([0.01, 0.02])
        trackid = np.array([10001, 10001, 10001])
        with self.assertRaises(ValueError):
            oerfit(ptime, dssh, trackid)

    def test_dssh_trackid_size_mismatch(self):
        ptime = np.array([1.0, 2.0, 3.0])
        dssh = np.array([0.01, 0.02, 0.03])
        trackid = np.array([10001, 10001])
        with self.assertRaises(ValueError):
            oerfit(ptime, dssh, trackid)


class OutputShapeTestCase(unittest.TestCase):
    """Test oerfit output structure with synthetic data."""

    @classmethod
    def setUpClass(cls) -> None:
        rng = np.random.default_rng(42)
        cls.n = 200
        # Simulate two passes with small SSH differences
        ptime = np.sort(rng.uniform(0, 24, cls.n))
        dssh = rng.normal(0, 0.05, cls.n)
        # Two distinct track IDs
        trackid = np.where(ptime < 12, 10001, 10002).astype(float)

        cls.coef, cls.tbrk, cls.rms_sig, cls.rms_res, cls.nint = oerfit(
            ptime, dssh, trackid
        )

    def test_coef_has_4_rows(self):
        """Coefficients should have 4 rows (cubic polynomial: a, b, c, d)."""
        self.assertEqual(self.coef.shape[0], 4)

    def test_coef_columns_match_intervals(self):
        """Number of coefficient columns should equal number of intervals."""
        n_intervals = len(self.tbrk) - 1
        self.assertEqual(self.coef.shape[1], n_intervals)

    def test_tbrk_sorted(self):
        """Break points should be sorted in ascending order."""
        self.assertTrue(
            np.all(np.diff(self.tbrk) > 0),
            "Break points are not strictly ascending",
        )

    def test_rms_sig_shape(self):
        self.assertEqual(len(self.rms_sig), len(self.tbrk) - 1)

    def test_rms_res_shape(self):
        self.assertEqual(len(self.rms_res), len(self.tbrk) - 1)

    def test_nint_shape(self):
        self.assertEqual(len(self.nint), len(self.tbrk) - 1)

    def test_nint_total_matches_input(self):
        """Total data points across all intervals should equal input size."""
        self.assertEqual(int(self.nint.sum()), self.n)


if __name__ == "__main__":
    unittest.main()
