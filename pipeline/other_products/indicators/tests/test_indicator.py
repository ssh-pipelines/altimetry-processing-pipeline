import sys
import unittest
from unittest.mock import MagicMock

# Mock external dependencies not available in test venv
_aws_mock = MagicMock()
sys.modules.setdefault("utilities", _aws_mock)
sys.modules.setdefault("utilities.aws_utils", _aws_mock)

_pyresample_mock = MagicMock()
sys.modules.setdefault("pyresample", _pyresample_mock)
sys.modules.setdefault("pyresample.utils", _pyresample_mock)

from app import build_sg_key
from indicators.compute_indicators import IndicatorProcessor


class TestBuildSgKey(unittest.TestCase):
    def test_basic_key(self):
        key = build_sg_key("2024-03-15", "my-bucket", "NASA-SSH")
        self.assertEqual(
            key,
            "s3://my-bucket/simple_grids/NASA-SSH/2024/"
            "NASA-SSH_alt_ref_simple_grid_v1_20240315.nc",
        )

    def test_different_source(self):
        key = build_sg_key("2025-01-06", "prod-bucket", "S6")
        self.assertEqual(
            key,
            "s3://prod-bucket/simple_grids/S6/2025/"
            "S6_alt_ref_simple_grid_v1_20250106.nc",
        )

    def test_year_boundary(self):
        key = build_sg_key("1993-01-01", "b", "GSFC")
        self.assertIn("/1993/", key)
        self.assertIn("19930101", key)


class TestMergeIndicators(unittest.TestCase):
    def test_empty_cache_returns_new(self):
        new = [
            {"time": 2024.5, "raw_gmsl": 1.0, "enso": 0.1, "pdo": 0.2, "iod": 0.3},
        ]
        result = IndicatorProcessor.merge_indicators([], new)
        self.assertEqual(result, new)

    def test_empty_new_returns_cached(self):
        cached = [
            {"time": 2024.0, "raw_gmsl": 1.0, "enso": 0.1, "pdo": 0.2, "iod": 0.3},
        ]
        result = IndicatorProcessor.merge_indicators(cached, [])
        self.assertEqual(result, cached)

    def test_both_empty(self):
        result = IndicatorProcessor.merge_indicators([], [])
        self.assertEqual(result, [])

    def test_new_overwrites_cached_at_same_time(self):
        cached = [
            {"time": 2024.0, "raw_gmsl": 1.0, "enso": 0.1, "pdo": 0.2, "iod": 0.3},
            {"time": 2024.5, "raw_gmsl": 2.0, "enso": 0.4, "pdo": 0.5, "iod": 0.6},
        ]
        new = [
            {"time": 2024.5, "raw_gmsl": 9.9, "enso": 9.9, "pdo": 9.9, "iod": 9.9},
        ]
        result = IndicatorProcessor.merge_indicators(cached, new)
        self.assertEqual(len(result), 2)
        # The time=2024.5 record should have the new values
        record_2024_5 = [r for r in result if r["time"] == 2024.5][0]
        self.assertAlmostEqual(record_2024_5["raw_gmsl"], 9.9)

    def test_append_new_times(self):
        cached = [{"time": 2024.0, "raw_gmsl": 1.0, "enso": 0.1, "pdo": 0.2, "iod": 0.3}]
        new = [{"time": 2025.0, "raw_gmsl": 2.0, "enso": 0.4, "pdo": 0.5, "iod": 0.6}]
        result = IndicatorProcessor.merge_indicators(cached, new)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["time"], 2024.0)
        self.assertEqual(result[1]["time"], 2025.0)

    def test_sorted_output(self):
        cached = [
            {"time": 2025.0, "raw_gmsl": 3.0, "enso": 0.1, "pdo": 0.2, "iod": 0.3},
            {"time": 2023.0, "raw_gmsl": 1.0, "enso": 0.1, "pdo": 0.2, "iod": 0.3},
        ]
        new = [
            {"time": 2024.0, "raw_gmsl": 2.0, "enso": 0.4, "pdo": 0.5, "iod": 0.6},
        ]
        result = IndicatorProcessor.merge_indicators(cached, new)
        times = [r["time"] for r in result]
        self.assertEqual(times, sorted(times))


class TestGenerateDs(unittest.TestCase):
    """Test generate_ds without requiring ref_files (bypass __init__)."""

    @staticmethod
    def _make_processor():
        """Create an IndicatorProcessor without triggering file-dependent __init__."""
        proc = object.__new__(IndicatorProcessor)
        proc.source = "TEST"
        return proc

    def _make_records(self, times, raw_gmsl_values):
        """Helper to build indicator record dicts."""
        return [
            {
                "time": t,
                "raw_gmsl": g,
                "enso": 0.0,
                "pdo": 0.0,
                "iod": 0.0,
            }
            for t, g in zip(times, raw_gmsl_values)
        ]

    def test_1993_normalization(self):
        proc = self._make_processor()
        # Create records spanning 1993 with known raw_gmsl
        times = [1993.0, 1993.5, 1994.0, 1995.0]
        raw_vals = [10.0, 12.0, 14.0, 16.0]
        records = self._make_records(times, raw_vals)

        ds = proc.generate_ds(records)

        # Mean of 1993 raw_gmsl = (10 + 12) / 2 = 11
        expected_mean = 11.0
        for t, raw in zip(times, raw_vals):
            gmsl_val = float(ds["gmsl"].sel(time=t).values)
            self.assertAlmostEqual(gmsl_val, raw - expected_mean, places=5)

    def test_raw_gmsl_preserved(self):
        proc = self._make_processor()
        times = [1993.0, 1994.0]
        raw_vals = [10.0, 20.0]
        records = self._make_records(times, raw_vals)

        ds = proc.generate_ds(records)

        self.assertIn("raw_gmsl", ds)
        for t, raw in zip(times, raw_vals):
            self.assertAlmostEqual(
                float(ds["raw_gmsl"].sel(time=t).values), raw, places=5
            )

    def test_missing_1993_guard(self):
        proc = self._make_processor()
        # Only data from 1995+, no 1993 records
        times = [1995.0, 1996.0]
        raw_vals = [5.0, 7.0]
        records = self._make_records(times, raw_vals)

        ds = proc.generate_ds(records)

        # With no 1993 data, gmsl should equal raw_gmsl (mean_1993 = 0)
        for t, raw in zip(times, raw_vals):
            self.assertAlmostEqual(
                float(ds["gmsl"].sel(time=t).values), raw, places=5
            )

    def test_smoothed_gmsl_present(self):
        proc = self._make_processor()
        times = [1993.0, 1993.5, 1994.0]
        raw_vals = [10.0, 12.0, 14.0]
        records = self._make_records(times, raw_vals)

        ds = proc.generate_ds(records)
        self.assertIn("smoothed_gmsl", ds)
        self.assertEqual(len(ds["smoothed_gmsl"]), 3)

    def test_sorted_by_time(self):
        proc = self._make_processor()
        # Provide records out of order
        records = self._make_records([1995.0, 1993.0, 1994.0], [3.0, 1.0, 2.0])

        ds = proc.generate_ds(records)
        times = ds["time"].values.tolist()
        self.assertEqual(times, sorted(times))


if __name__ == "__main__":
    unittest.main()
