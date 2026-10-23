import unittest
from dataclasses import replace
from unittest.mock import patch

import numpy as np
import xarray as xr
from daily_files.config.source_config import get_source_config
from daily_files.processing import smoothing
from daily_files.processing.smoothing import (
    create_filter,
    sample_offsets,
    smooth,
    ssha_smoothing,
)


def _times(seconds) -> np.ndarray:
    base = np.datetime64("1992-10-23T00:00:00", "ns")
    return base + (np.asarray(seconds) * 1e9).astype("timedelta64[ns]")


class EndToEndSmoothingTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.n = 100

        cls.ssha: np.ndarray = np.ones(cls.n + 18)
        cls.flag: np.ndarray = np.full_like(cls.ssha, 0)
        cls.time: np.ndarray = np.arange(
            "2020-01-01", "2020-01-02", 1, dtype="datetime64[s]"
        ).astype("datetime64[ns]")[: cls.n + 18]
        cls.og_ds: xr.Dataset = xr.Dataset(
            {"ssha": (("time"), cls.ssha), "nasa_flag": (("time"), cls.flag)},
            {"time": cls.time},
        )
        cls.smooth_ds: xr.Dataset = ssha_smoothing(cls.og_ds, "GSFC")

    def test_mirror_nans(self):
        arr = np.array([1, np.nan, 2, 3])
        arr[np.isnan(arr)[::-1]] = np.nan
        self.assertEqual(arr.all(), np.array([1, np.nan, np.nan, 3]).all())
        arr = np.array([1, np.nan, 2, np.nan, 3])
        arr[np.isnan(arr)[::-1]] = np.nan
        self.assertEqual(arr.all(), arr.all())

    def test_no_nans(self):
        arr = np.full(19, 1, dtype="float32")
        smoothed_val = smooth(arr)
        self.assertEqual(1, smoothed_val)

    def test_all_nans(self):
        arr = np.full(19, np.nan)
        smoothed_val = smooth(arr)
        self.assertIs(np.nan, smoothed_val)

    def test_center_nans(self):
        arr = np.full(19, 1, dtype="float32")
        arr[9] = np.nan
        smoothed_val = smooth(arr)
        self.assertAlmostEqual(1, smoothed_val, 5)

    def test_smoothing(self):
        arr = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1])
        smoothed_val = smooth(arr)
        # Oracle reflects GSFC ground_speed=5.7582 (config-derived filter width).
        self.assertAlmostEqual(8.56695, smoothed_val, 4)

    def test_case_1(self):
        arr = np.array(
            [
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                1.89804412,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
            ]
        )
        smoothed_val = smooth(arr)
        self.assertIs(np.nan, smoothed_val)

    def test_case_2(self):
        arr = np.array(
            [
                -0.17185227,
                -0.09885015,
                -0.00343418,
                0.08667804,
                0.26273039,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                1.89804412,
                np.nan,
                np.nan,
            ]
        )
        smoothed_val = smooth(arr)
        self.assertAlmostEqual(0.94406, smoothed_val, 4)


class SampleOffsetsTestCase(unittest.TestCase):
    """Each omitted sample gets one NaN slot, so the 19-point window spans 19
    sample periods across a land crossing at any sampling rate."""

    def test_regular_1hz_has_no_slots(self):
        offsets = sample_offsets(_times(np.arange(50.0)), 1.0)
        np.testing.assert_array_equal(offsets, np.arange(50))

    def test_regular_sub_second_has_no_slots(self):
        # ERS-1's period; no timestamp lands on a whole second.
        offsets = sample_offsets(_times(0.37 + np.arange(50) * 0.980468), 0.980468)
        np.testing.assert_array_equal(offsets, np.arange(50))

    def test_small_gap_fills_one_slot_per_omitted_sample(self):
        dt = 0.980468
        seconds = np.delete(np.arange(60) * dt, [30, 31, 32])
        offsets = sample_offsets(_times(seconds), dt)
        self.assertEqual(np.diff(offsets)[29], 4)
        self.assertEqual(offsets[-1] + 1 - len(seconds), 3)

    def test_wide_gap_is_capped_at_half_window(self):
        """Nothing more than 9 slots away reaches into a 19-point window."""
        seconds = np.concatenate([np.arange(30.0), 500 + np.arange(30.0)])
        offsets = sample_offsets(_times(seconds), 1.0)
        self.assertEqual(np.diff(offsets)[29], 10)

    def test_ambiguous_gap_rounds_to_nearest(self):
        """ERS-1 C0015 P0001's 8.202 s gap is 8.366 local periods: the timestamps
        do not determine whether 7 or 8 samples were omitted. Pinned to the
        nearest whole period count: 8 periods, so 7 omitted samples."""
        dt = 0.980468
        seconds = np.concatenate([np.arange(30) * dt, 29 * dt + 8.202148 + np.arange(30) * dt])
        with self.assertLogs(level="DEBUG") as logs:
            offsets = sample_offsets(_times(seconds), dt)
        self.assertEqual(np.diff(offsets)[29], 8)
        self.assertIn("ambiguous", "\n".join(logs.output))

    def test_local_period_follows_clock_drift(self):
        """A period 2% off the configured value still counts gaps correctly:
        the count uses the local median step, not sample_period."""
        dt = 0.98
        seconds = np.delete(np.arange(60) * dt, [30, 31])
        offsets = sample_offsets(_times(seconds), 1.0)
        self.assertEqual(np.diff(offsets)[29], 3)

    def test_single_point(self):
        np.testing.assert_array_equal(sample_offsets(_times([5.0]), 1.0), [0])


class FilterWidthTestCase(unittest.TestCase):
    """Filter taps are one sample apart, i.e. ground_speed * sample_period km."""

    def _filter(self, **overrides):
        cfg = replace(get_source_config("GSFC"), **overrides)
        with patch.object(smoothing, "get_source_config", return_value=cfg):
            return create_filter("GSFC")

    def test_unit_period_matches_speed_only_filter(self):
        speed, sigma = 5.7582, 15
        expected = np.exp(-(((np.arange(-9, 10) * speed) / sigma) ** 2))
        np.testing.assert_array_equal(self._filter(), expected / expected.sum())

    def test_period_scales_tap_spacing(self):
        np.testing.assert_allclose(
            self._filter(sample_period=0.5),
            self._filter(ground_speed=5.7582 * 0.5),
            rtol=1e-15,
        )


class SubSecondSmoothingTestCase(unittest.TestCase):
    """The integer-second grid raised KeyError on any source whose timestamps
    miss whole seconds."""

    def _run(self, seconds, ssha):
        ds = xr.Dataset(
            {"ssha": ("time", ssha), "nasa_flag": ("time", np.zeros(len(ssha), np.int8))},
            coords={"time": _times(seconds)},
        )
        cfg = replace(get_source_config("GSFC"), sample_period=0.980468)
        with patch.object(smoothing, "get_source_config", return_value=cfg):
            return ssha_smoothing(ds, "GSFC")["ssha_smoothed"].values

    # A point with no data on one side of its window is NaN'd by the mirroring
    # step, so the first and last sample of each contiguous run are excluded.

    def test_constant_field_smooths_to_itself(self):
        out = self._run(0.37 + np.arange(100) * 0.980468, np.ones(100))
        np.testing.assert_allclose(out[1:-1], 1.0, rtol=1e-12)

    def test_points_across_a_wide_gap_do_not_mix(self):
        seconds = np.concatenate([np.arange(40) * 0.980468, 200 + np.arange(40) * 0.980468])
        out = self._run(seconds, np.concatenate([np.zeros(40), np.ones(40)]))
        np.testing.assert_allclose(out[1:39], 0.0, atol=1e-12)
        np.testing.assert_allclose(out[41:79], 1.0, rtol=1e-12)
        self.assertTrue(np.isnan(out[[0, 39, 40, 79]]).all())
