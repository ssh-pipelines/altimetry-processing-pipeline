"""Unit tests for tools/compute_ground_speed.py.

Feed synthetic passes with a known constant along-track speed and assert the
recovered speed; assert short passes are excluded; assert duplicate / reversed
timestamps do not divide-by-zero. Feed passes at a known non-1 Hz sample period
and assert the recovered period, and that the speed is unaffected by the rate.
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
    compute_sample_period,
    haversine_km,
    pass_ground_speed,
    pass_sample_period,
    sample_period_mismatch,
)

# ERS-1's measured sample period: not 1 Hz, and not on whole seconds.
ERS1_PERIOD_S = 0.980468


def _synthetic_pass(n, speed_km_s, start_lat=0.0, lon=0.0, t0=0, period_s=1.0):
    """Build a straight north-going meridian pass of `n` points, `period_s`
    seconds apart, whose along-track ground speed is `speed_km_s`.

    A step of `speed_km_s` km along a meridian is `speed_km_s / 111.194...` deg
    of latitude per second (deg per km = 180 / (pi * R)).
    """
    deg_per_km = 180.0 / (np.pi * 6371.0088)
    dlat = speed_km_s * period_s * deg_per_km
    lat = start_lat + dlat * np.arange(n)
    lon_arr = np.full(n, lon)
    # Real timestamps are ~1e9 s from the epoch, where float seconds lose
    # sub-microsecond precision; build them in integer nanoseconds.
    t0_ns = np.datetime64("1992-10-25T00:00:00", "ns").astype("int64") + int(t0 * 1e9)
    time = (t0_ns + np.round(np.arange(n) * period_s * 1e9).astype("int64")).astype(
        "datetime64[ns]"
    )
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

    def test_speed_independent_of_sample_period(self):
        time, lat, lon = _synthetic_pass(2000, speed_km_s=6.7267, period_s=ERS1_PERIOD_S)
        self.assertAlmostEqual(pass_ground_speed(time, lat, lon), 6.7267, places=3)


class PassSamplePeriodTestCase(unittest.TestCase):
    def test_recovers_non_1hz_period(self):
        time, _, _ = _synthetic_pass(2000, speed_km_s=6.7, period_s=ERS1_PERIOD_S)
        self.assertAlmostEqual(pass_sample_period(time), ERS1_PERIOD_S, places=7)

    def test_unsorted_input_is_sorted(self):
        time, _, _ = _synthetic_pass(500, speed_km_s=6.7, period_s=ERS1_PERIOD_S)
        order = np.random.permutation(len(time))
        self.assertAlmostEqual(pass_sample_period(time[order]), ERS1_PERIOD_S, places=7)

    def test_gaps_do_not_bias_period(self):
        # Land crossings omit records; the long steps must not pull the median.
        time, _, _ = _synthetic_pass(1000, speed_km_s=6.7, period_s=ERS1_PERIOD_S)
        keep = np.ones(len(time), dtype=bool)
        keep[200:208] = False
        keep[600:900] = False
        self.assertAlmostEqual(pass_sample_period(time[keep]), ERS1_PERIOD_S, places=7)

    def test_duplicate_timestamps_return_none(self):
        time = np.array(["2020-01-01T00:00:00", "2020-01-01T00:00:00"], dtype="datetime64[ns]")
        self.assertIsNone(pass_sample_period(time))


class ComputeSamplePeriodTestCase(unittest.TestCase):
    def test_median_across_passes(self):
        periods = [0.980224, ERS1_PERIOD_S, ERS1_PERIOD_S]
        passes = [
            _synthetic_pass(300, 6.7, t0=i * 10000, period_s=p) for i, p in enumerate(periods)
        ]
        time = np.concatenate([p[0] for p in passes])
        cycle = np.ones(len(time), dtype=int)
        pass_ = np.repeat([1, 2, 3], 300)
        period, per_pass = compute_sample_period(time, cycle, pass_)
        self.assertAlmostEqual(period, ERS1_PERIOD_S, places=7)
        self.assertEqual(len(per_pass), 3)

    def test_short_passes_count(self):
        # Unlike ground speed, a pass truncated at a day boundary still has
        # regular steps and contributes.
        time, _, _ = _synthetic_pass(50, 6.7, period_s=ERS1_PERIOD_S)
        period, per_pass = compute_sample_period(time, np.ones(50), np.ones(50))
        self.assertAlmostEqual(period, ERS1_PERIOD_S, places=7)
        self.assertEqual(len(per_pass), 1)

    def test_steps_do_not_span_passes(self):
        # Two passes interleaved in time: grouping by pass recovers each one's
        # period, where differencing the concatenation would halve it.
        a, _, _ = _synthetic_pass(400, 6.7, period_s=ERS1_PERIOD_S)
        b = a + np.timedelta64(490, "ms")
        time = np.concatenate([a, b])
        pass_ = np.repeat([1, 2], 400)
        period, _ = compute_sample_period(time, np.ones(800), pass_)
        self.assertAlmostEqual(period, ERS1_PERIOD_S, places=7)

    def test_no_regular_step_raises(self):
        time = (np.arange(10) * 100).astype("datetime64[s]").astype("datetime64[ns]")
        with self.assertRaises(ValueError):
            compute_sample_period(time, np.ones(10), np.ones(10))


class SamplePeriodMismatchTestCase(unittest.TestCase):
    def test_rounding_boundary_is_not_a_mismatch(self):
        # ERS-1 measured 0.980469 s on a 1995 window against a configured
        # 0.980468 s taken from 1992: the same rate either side of a rounding
        # boundary.
        self.assertFalse(sample_period_mismatch(0.980469, ERS1_PERIOD_S))

    def test_default_on_non_1hz_source_is_a_mismatch(self):
        self.assertTrue(sample_period_mismatch(ERS1_PERIOD_S, 1.0))

    def test_other_ers1_clock_rate_is_a_mismatch(self):
        # ERS-1's second clock population, 2.4e-4 s away, must still register.
        self.assertTrue(sample_period_mismatch(0.980224, ERS1_PERIOD_S))


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
