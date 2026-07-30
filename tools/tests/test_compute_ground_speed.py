"""Unit tests for tools/compute_ground_speed.py.

Feed synthetic passes with a known constant along-track speed and assert the
recovered speed; assert short passes are excluded; assert duplicate / reversed
timestamps do not divide-by-zero.
"""
import os
import sys
import unittest

import numpy as np

# tools/ is the parent of this tests/ dir.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from compute_ground_speed import (  # noqa: E402
    _MIN_PASS_POINTS,
    compute_ground_speed,
    haversine_km,
    pass_ground_speed,
)


def _synthetic_pass(n, speed_km_s, start_lat=0.0, lon=0.0, t0=0):
    """Build a straight north-going meridian pass of `n` points at 1 Hz whose
    along-track ground speed is `speed_km_s`.

    A step of `speed_km_s` km along a meridian is `speed_km_s / 111.194...` deg
    of latitude per second (deg per km = 180 / (pi * R)).
    """
    deg_per_km = 180.0 / (np.pi * 6371.0088)
    dlat = speed_km_s * deg_per_km
    lat = start_lat + dlat * np.arange(n)
    lon_arr = np.full(n, lon)
    time = (t0 + np.arange(n)).astype("datetime64[s]").astype("datetime64[ns]")
    return time, lat, lon_arr


class HaversineTestCase(unittest.TestCase):
    def test_known_meridian_distance(self):
        # One degree of latitude ~= 111.19 km on a sphere of R = 6371.0088.
        d = haversine_km(np.array([0.0]), np.array([0.0]), np.array([1.0]), np.array([0.0]))
        self.assertAlmostEqual(d[0], np.pi / 180.0 * 6371.0088, places=6)

    def test_zero_distance(self):
        d = haversine_km(np.array([12.0]), np.array([34.0]), np.array([12.0]), np.array([34.0]))
        self.assertAlmostEqual(d[0], 0.0, places=9)


class PassGroundSpeedTestCase(unittest.TestCase):
    def test_recovers_known_speed(self):
        time, lat, lon = _synthetic_pass(2000, speed_km_s=6.4)
        self.assertAlmostEqual(pass_ground_speed(time, lat, lon), 6.4, places=3)

    def test_unsorted_input_is_sorted(self):
        time, lat, lon = _synthetic_pass(500, speed_km_s=5.7)
        order = np.random.permutation(len(time))
        self.assertAlmostEqual(
            pass_ground_speed(time[order], lat[order], lon[order]), 5.7, places=3
        )

    def test_duplicate_timestamps_do_not_divide_by_zero(self):
        # Two identical points -> dt == 0 -> dropped, no samples survive.
        time = np.array(["2020-01-01T00:00:00", "2020-01-01T00:00:00"], dtype="datetime64[ns]")
        lat = np.array([0.0, 0.1])
        lon = np.array([0.0, 0.0])
        self.assertIsNone(pass_ground_speed(time, lat, lon))

    def test_data_gap_samples_dropped(self):
        # Points 1 s apart except one 100 s gap; the gap sample is dropped and
        # the recovered speed still matches the true per-second speed.
        time, lat, lon = _synthetic_pass(1000, speed_km_s=5.9)
        t = time.astype("datetime64[s]").astype("int64")
        t[500:] += 100  # inject a gap after point 500
        time = t.astype("datetime64[s]").astype("datetime64[ns]")
        self.assertAlmostEqual(pass_ground_speed(time, lat, lon), 5.9, places=3)


class ComputeGroundSpeedTestCase(unittest.TestCase):
    def _stack(self, passes):
        """passes: list of (time, lat, lon, cycle, pass_no)."""
        t = np.concatenate([p[0] for p in passes])
        la = np.concatenate([p[1] for p in passes])
        lo = np.concatenate([p[2] for p in passes])
        cy = np.concatenate([np.full(len(p[0]), p[3]) for p in passes])
        ps = np.concatenate([np.full(len(p[0]), p[4]) for p in passes])
        return t, la, lo, cy, ps

    def test_median_across_long_passes(self):
        n = _MIN_PASS_POINTS + 500
        p1 = (*_synthetic_pass(n, 5.6, lon=0.0), 1, 1)
        p2 = (*_synthetic_pass(n, 5.8, lon=10.0), 1, 2)
        p3 = (*_synthetic_pass(n, 6.0, lon=20.0), 1, 3)
        speed, used = compute_ground_speed(*self._stack([p1, p2, p3]))
        self.assertAlmostEqual(speed, 5.8, places=2)
        self.assertEqual(len(used), 3)

    def test_short_passes_excluded(self):
        long_n = _MIN_PASS_POINTS + 500
        long_pass = (*_synthetic_pass(long_n, 6.1, lon=0.0), 1, 1)
        # A short pass with a wildly different speed that must NOT sway the result.
        short_pass = (*_synthetic_pass(50, 20.0, lon=10.0), 1, 2)
        speed, used = compute_ground_speed(*self._stack([long_pass, short_pass]))
        self.assertAlmostEqual(speed, 6.1, places=2)
        self.assertEqual(len(used), 1)

    def test_no_qualifying_pass_raises(self):
        short = (*_synthetic_pass(100, 6.0, lon=0.0), 1, 1)
        with self.assertRaises(ValueError):
            compute_ground_speed(*self._stack([short]))


if __name__ == "__main__":
    unittest.main()
