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


class GroundSpeedTestCase(unittest.TestCase):
    """Test that the ground_speed parameter drives knot placement.

    oerfit converts each pass's knot time-span into an along-track distance via
    ``delt = (tbrk2 - tbrk1) * 3600 * ground_speed / 10000`` and compares it to
    0.5 and 1.0 to decide whether to keep the mid/end knots. Changing
    ground_speed therefore changes the break layout for the same data.
    """

    @classmethod
    def setUpClass(cls) -> None:
        rng = np.random.default_rng(3)
        # Two short (~0.4 h) passes with >20 points each, separated by ~10 h.
        # At this duration the along-track distance delt straddles the 0.5/1.0
        # thresholds across the speeds tested, so the retained mid/end knots —
        # and therefore len(tbrk) — differ per speed (2.0→4, 5.7→6, 10.0→8).
        p1 = np.sort(rng.uniform(2.0, 2.4, 30))
        p2 = np.sort(rng.uniform(12.0, 12.4, 30))
        cls.ptime = np.concatenate([p1, p2])
        cls.dssh = rng.normal(0, 0.03, 60)
        cls.trackid = np.concatenate([np.full(30, 10001.0), np.full(30, 10002.0)])

    def test_default_matches_explicit_5_7(self):
        """Omitting ground_speed reproduces the historical 5.7 result exactly."""
        default = oerfit(self.ptime, self.dssh, self.trackid)
        explicit = oerfit(self.ptime, self.dssh, self.trackid, ground_speed=5.7)
        for a, b in zip(default, explicit):
            np.testing.assert_array_equal(a, b)

    def test_ground_speed_changes_breaks(self):
        """A different ground_speed changes the spline break placement."""
        _, tbrk_fast, *_ = oerfit(
            self.ptime, self.dssh, self.trackid, ground_speed=10.0
        )
        _, tbrk_slow, *_ = oerfit(
            self.ptime, self.dssh, self.trackid, ground_speed=2.0
        )
        self.assertNotEqual(
            len(tbrk_fast),
            len(tbrk_slow),
            "ground_speed did not alter knot placement",
        )


if __name__ == "__main__":
    unittest.main()
