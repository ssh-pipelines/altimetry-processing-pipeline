import gzip
import os
import shutil
import tempfile
import unittest
from dataclasses import replace
from unittest.mock import patch

import numpy as np
import xarray as xr
from daily_files.config.source_config import get_source_config
from daily_files.processing import smoothing
from daily_files.processing.smoothing import ssha_smoothing

SAMPLE_DATA = os.path.join(os.path.dirname(__file__), "sample_data")


def _open_gz(name: str) -> xr.Dataset:
    with tempfile.TemporaryDirectory() as td:
        path = os.path.join(td, name.removesuffix(".gz"))
        with gzip.open(os.path.join(SAMPLE_DATA, name)) as f, open(path, "wb") as g:
            shutil.copyfileobj(f, g)
        with xr.open_dataset(path) as ds:
            return ds.load()


class S6GoldenTestCase(unittest.TestCase):
    """A full S6 day (85,815 points, all on whole seconds, five land gaps of 62 s
    and 132 s) must smooth byte-identically to the integer-second-grid smoother.

    `S6_ssha_smoothed_20250101.nc.gz` holds `ssha` and `nasa_flag` from the OER
    stage's S6 P1 input, with `ssha_smoothed` produced by the grid-reindexing
    smoother at S6's `ground_speed` 5.7529. That input's own `ssha_smoothed`
    cannot serve: it was written at the earlier 5.745.
    """

    @classmethod
    def setUpClass(cls):
        cls.golden = _open_gz("S6_ssha_smoothed_20250101.nc.gz")
        cls.out = ssha_smoothing(cls.golden[["ssha", "nasa_flag"]].copy(), "S6")

    def test_ssha_smoothed_is_byte_identical(self):
        np.testing.assert_array_equal(
            self.out["ssha_smoothed"].values, self.golden["ssha_smoothed"].values
        )

    def test_fixture_is_whole_second_1hz(self):
        t = self.golden["time"].values.astype("int64")
        self.assertTrue((t % 10**9 == 0).all())
        self.assertEqual(np.median(np.diff(t)), 10**9)


class S6OmittedSamplesGoldenTestCase(unittest.TestCase):
    """Gaps with valid data on both sides, which the full S6 day lacks: each of its
    five gaps has no unflagged sample within 9 points, so it passes whatever count
    is assigned to them.

    `S6_ssha_smoothed_omitted_samples.nc.gz` is 1200 consecutive points from the
    same S6 day, starting at index 36912 (the day's longest unflagged run), with
    1, 2, 3, 5, 8, 9, 10, 17, and 40 samples deleted at offsets 100 through 900.
    `ssha_smoothed` is from the grid-reindexing smoother at `ground_speed` 5.7529.
    """

    @classmethod
    def setUpClass(cls):
        cls.golden = _open_gz("S6_ssha_smoothed_omitted_samples.nc.gz")
        cls.out = ssha_smoothing(cls.golden[["ssha", "nasa_flag"]].copy(), "S6")

    def test_ssha_smoothed_is_byte_identical(self):
        np.testing.assert_array_equal(
            self.out["ssha_smoothed"].values, self.golden["ssha_smoothed"].values
        )

    def test_fixture_has_the_omitted_sample_counts(self):
        steps = np.diff(self.golden["time"].values.astype("int64")) // 10**9
        np.testing.assert_array_equal(
            np.sort(steps[steps > 1]) - 1, [1, 2, 3, 5, 8, 9, 10, 17, 40]
        )


class ERS1GoldenTestCase(unittest.TestCase):
    """One real non-1 Hz pass: ERS-1 C0015 P0001, 1685 points at 0.980468 s, none
    on a whole second, 47 gaps of which 8 have ambiguous omitted-sample counts.

    `ERS1_C0015_P0001_ssha_smoothed.nc.gz` holds `ssha` (`sea_level_anomaly`) and
    `nasa_flag` (`validation_flag != 0`) from the AVISO L2P granule, with
    `ssha_smoothed` produced by the count-based smoother at the single-pass
    measurements `ground_speed` 6.7270 and `sample_period` 0.980468. There is no
    independent oracle: the grid smoother raised KeyError on this pass. The
    fixture pins the gap counts, including the ambiguous ones, against change.
    """

    @classmethod
    def setUpClass(cls):
        cls.golden = _open_gz("ERS1_C0015_P0001_ssha_smoothed.nc.gz")
        cfg = replace(
            get_source_config("S3B"), ground_speed=6.7270, sample_period=0.980468
        )
        with patch.object(smoothing, "get_source_config", return_value=cfg):
            cls.out = ssha_smoothing(cls.golden[["ssha", "nasa_flag"]].copy(), "ERS1")

    def test_ssha_smoothed_matches(self):
        """1e-12 m: far below any physical signal, above summation-order noise."""
        np.testing.assert_allclose(
            self.out["ssha_smoothed"].values,
            self.golden["ssha_smoothed"].values,
            rtol=0,
            atol=1e-12,
        )

    def test_most_points_are_smoothed(self):
        """The grid smoother raised before producing anything; on the old grid
        every window here would have been all-NaN."""
        self.assertGreater(np.isfinite(self.out["ssha_smoothed"].values).mean(), 0.9)

    def test_fixture_is_sub_second(self):
        t = self.golden["time"].values.astype("int64")
        self.assertFalse((t % 10**9 == 0).any())


if __name__ == "__main__":
    unittest.main()
