"""Unit tests for tools/compute_intermission_bias.py.

Exercise the robust estimator (median, MAD outlier rejection, NaN handling),
the daily-mean gap-fill helper, and the low-pass smoother.
"""
import os
import sys
import unittest

import numpy as np
import pandas as pd

# tools/ is the parent of this tests/ dir.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from compute_intermission_bias import (  # noqa: E402
    binned_by_latitude,
    compute_intermission_bias,
    daily_means,
    fit_trend,
    rejected_mask,
    smooth_bias,
)


class ComputeIntermissionBiasTestCase(unittest.TestCase):
    def test_recovers_constant_offset(self):
        rng = np.random.default_rng(0)
        dssh = 0.02 + 0.1 * rng.standard_normal(5000)
        stats = compute_intermission_bias(dssh)
        self.assertAlmostEqual(stats["median"], 0.02, places=2)
        self.assertEqual(stats["count"], 5000)

    def test_mad_rejects_outliers(self):
        # A realistic noisy cluster around 0.02 plus a handful of gross blunders.
        # The plain mean is skewed; the outlier-rejected mean and median are not.
        rng = np.random.default_rng(2)
        core = 0.02 + 0.1 * rng.standard_normal(1000)
        blunders = np.array([50.0, -50.0, 100.0])
        dssh = np.concatenate([core, blunders])
        stats = compute_intermission_bias(dssh)
        self.assertAlmostEqual(stats["median"], 0.02, places=2)
        self.assertAlmostEqual(stats["outlier_rejected_mean"], 0.02, places=2)
        self.assertEqual(stats["n_rejected"], 3)
        self.assertGreater(abs(stats["mean"] - 0.02), 0.01)  # plain mean skewed

    def test_nans_ignored(self):
        dssh = np.array([0.02, np.nan, 0.02, np.nan, 0.02])
        stats = compute_intermission_bias(dssh)
        self.assertAlmostEqual(stats["median"], 0.02, places=6)
        self.assertEqual(stats["count"], 3)

    def test_all_nan_raises(self):
        with self.assertRaises(ValueError):
            compute_intermission_bias(np.array([np.nan, np.nan]))

    def test_identical_values_no_rejection(self):
        stats = compute_intermission_bias(np.full(100, 0.05))
        self.assertEqual(stats["n_rejected"], 0)
        self.assertAlmostEqual(stats["outlier_rejected_mean"], 0.05, places=6)
        # Degenerate spread -> infinite band -> nothing outside it.
        self.assertFalse(np.isfinite(stats["reject_lower"]))
        self.assertFalse(np.isfinite(stats["reject_upper"]))

    def test_reject_band_reported(self):
        rng = np.random.default_rng(3)
        dssh = 0.02 + 0.1 * rng.standard_normal(2000)
        stats = compute_intermission_bias(dssh)
        # Band is symmetric about the median at 5 robust-sigma.
        self.assertGreater(stats["robust_sigma"], 0)
        self.assertAlmostEqual(
            stats["reject_upper"] - stats["median"],
            stats["median"] - stats["reject_lower"],
            places=9,
        )
        self.assertAlmostEqual(
            stats["reject_upper"] - stats["median"], 5.0 * stats["robust_sigma"], places=9
        )


class RejectedMaskTestCase(unittest.TestCase):
    def test_flags_points_outside_band(self):
        core = np.full(1000, 0.02)
        blunders = np.array([50.0, -50.0])
        dssh = np.concatenate([core, blunders])
        # Force a finite band so the (degenerate-core) MAD doesn't zero it out.
        stats = {"reject_lower": -1.0, "reject_upper": 1.0}
        mask = rejected_mask(dssh, stats)
        self.assertEqual(int(mask.sum()), 2)
        self.assertTrue(mask[-1] and mask[-2])

    def test_nan_never_rejected(self):
        dssh = np.array([0.02, np.nan, 100.0])
        stats = {"reject_lower": -1.0, "reject_upper": 1.0}
        mask = rejected_mask(dssh, stats)
        self.assertFalse(mask[1])  # NaN not flagged
        self.assertTrue(mask[2])   # 100.0 flagged

    def test_mask_matches_estimator_count(self):
        rng = np.random.default_rng(4)
        dssh = np.concatenate([0.02 + 0.1 * rng.standard_normal(2000), [50.0, -50.0, 80.0]])
        stats = compute_intermission_bias(dssh)
        self.assertEqual(int(rejected_mask(dssh, stats).sum()), stats["n_rejected"])


class BinnedByLatitudeTestCase(unittest.TestCase):
    def test_detects_latitude_trend(self):
        # dssh that ramps with latitude -> binned medians should increase.
        lat = np.linspace(50, 80, 3000)
        dssh = 0.01 * lat  # monotonic in latitude
        centers, medians, mads, counts = binned_by_latitude(lat, dssh, bin_width_deg=5.0)
        self.assertGreater(centers.size, 2)
        self.assertTrue(np.all(np.diff(medians) > 0))  # increasing with latitude
        self.assertEqual(int(counts.sum()), 3000)

    def test_flat_bias_flat_bins(self):
        rng = np.random.default_rng(5)
        lat = rng.uniform(55, 75, 3000)
        dssh = 0.02 + 0.05 * rng.standard_normal(3000)  # no latitude structure
        centers, medians, mads, counts = binned_by_latitude(lat, dssh, bin_width_deg=5.0)
        self.assertLess(float(np.ptp(medians)), 0.02)  # bins agree to within noise

    def test_empty_input(self):
        centers, medians, mads, counts = binned_by_latitude(
            np.array([]), np.array([])
        )
        self.assertEqual(centers.size, 0)

    def test_nans_dropped(self):
        lat = np.array([60.0, np.nan, 70.0])
        dssh = np.array([0.02, 0.02, np.nan])
        centers, medians, mads, counts = binned_by_latitude(lat, dssh, bin_width_deg=5.0)
        # Only the (60.0, 0.02) pair is usable.
        self.assertEqual(int(counts.sum()), 1)


class DailyMeansTestCase(unittest.TestCase):
    def test_gap_filled_daily_grid(self):
        # Two observed days a week apart -> reindexed to a complete daily grid
        # with the interior interpolated and no NaNs left.
        times = np.array(
            ["2020-01-01T03:00:00", "2020-01-01T09:00:00", "2020-01-08T12:00:00"],
            dtype="datetime64[ns]",
        )
        ssh1 = np.array([1.0, 3.0, 10.0])  # day1 mean = 2.0
        ssh2 = np.array([0.0, 0.0, 0.0])
        df = daily_means(times, ssh1, ssh2)
        # 8 calendar days Jan 1..Jan 8 inclusive
        self.assertEqual(len(df), 8)
        self.assertFalse(df.isna().any().any())
        self.assertAlmostEqual(df["high_lat_daily_mean"].iloc[0], 2.0, places=6)
        self.assertAlmostEqual(df["high_lat_daily_mean"].iloc[-1], 10.0, places=6)
        # midpoint is a linear ramp between 2.0 and 10.0
        self.assertTrue(2.0 < df["high_lat_daily_mean"].iloc[3] < 10.0)

    def test_keep_mask_excludes_outliers(self):
        # A day with a gross outlier: excluding it via keep should change that
        # day's mean toward the good points.
        times = np.array(
            ["2020-01-01T03:00:00", "2020-01-01T09:00:00", "2020-01-01T15:00:00"],
            dtype="datetime64[ns]",
        )
        ssh1 = np.array([0.02, 0.02, 100.0])  # third is a blunder
        ssh2 = np.zeros(3)
        keep = np.array([True, True, False])
        df = daily_means(times, ssh1, ssh2, keep=keep)
        self.assertAlmostEqual(df["high_lat_daily_mean"].iloc[0], 0.02, places=6)


class FitTrendTestCase(unittest.TestCase):
    def _days(self, n):
        return pd.date_range("2019-01-01", periods=n, freq="D")

    def test_recovers_known_slope(self):
        # A clear 5 mm/yr ramp over 4 years with small noise -> slope recovered
        # near 0.005 m/yr, CI clear of zero, total drift ~20 mm over 4 yr.
        n = 4 * 365
        days = self._days(n)
        years = np.arange(n) / 365.25
        rng = np.random.default_rng(0)
        diff = 0.02 + 0.005 * years + 0.002 * rng.standard_normal(n)
        trend = fit_trend(days, diff)
        self.assertIsNotNone(trend)
        self.assertAlmostEqual(trend["slope_m_per_yr"], 0.005, places=3)
        self.assertGreater(trend["slope_ci_low"], 0.0)
        self.assertAlmostEqual(
            trend["total_drift_m"], trend["slope_m_per_yr"] * trend["span_years"], places=9
        )

    def test_flat_series_ci_straddles_zero(self):
        # No trend, just noise -> CI includes zero, slope ~0.
        n = 4 * 365
        days = self._days(n)
        rng = np.random.default_rng(1)
        diff = 0.02 + 0.01 * rng.standard_normal(n)
        trend = fit_trend(days, diff)
        self.assertIsNotNone(trend)
        self.assertLessEqual(trend["slope_ci_low"], 0.0)
        self.assertGreaterEqual(trend["slope_ci_high"], 0.0)
        # No verdict field is emitted — the tool leaves the call to the reader.
        self.assertNotIn("significant", trend)

    def test_short_series_returns_none(self):
        # Under two years -> can't distinguish trend from wobble.
        days = self._days(300)
        diff = np.full(300, 0.02)
        self.assertIsNone(fit_trend(days, diff))

    def test_robust_to_outliers(self):
        # A flat series with a few gross spikes: Theil-Sen slope stays ~0.
        n = 4 * 365
        days = self._days(n)
        diff = np.full(n, 0.02)
        diff[::200] = 5.0  # periodic blunders
        trend = fit_trend(days, diff)
        self.assertIsNotNone(trend)
        self.assertAlmostEqual(trend["slope_m_per_yr"], 0.0, places=4)


class SmoothBiasTestCase(unittest.TestCase):
    def test_lowpass_attenuates_noise(self):
        # A flat signal plus high-frequency noise: the low-pass output should be
        # far tighter than the raw series and centered on the true level.
        rng = np.random.default_rng(1)
        n = 2000
        signal = 0.02 + 0.1 * rng.standard_normal(n)
        smoothed = smooth_bias(signal, cutoff_days=90)
        self.assertLess(np.std(smoothed), np.std(signal))
        self.assertAlmostEqual(float(np.mean(smoothed)), 0.02, places=2)

    def test_lowpass_tracks_ramp(self):
        # A slow linear ramp survives a low-pass filter (monotonic in, ~monotonic out).
        n = 2000
        ramp = np.linspace(0.0, 0.1, n)
        smoothed = smooth_bias(ramp, cutoff_days=90)
        self.assertGreater(smoothed[-1], smoothed[0])


if __name__ == "__main__":
    unittest.main()
