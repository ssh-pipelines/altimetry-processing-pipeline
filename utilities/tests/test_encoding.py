"""Tests for utilities.encoding.daily_file_encoding.

Pins the per-variable encoding rules (dtype / _FillValue / compression) the
function derives from variable names. Builds a small in-memory Dataset with
representative names so every branch is exercised without touching disk.
"""
import unittest

import numpy as np
import xarray as xr

from utilities.encoding import daily_file_encoding, simple_grid_encoding


class TestDailyFileEncoding(unittest.TestCase):

    def _dataset(self):
        n = 3
        dims = ("time",)
        data = {
            "ssh": (dims, np.zeros(n)),
            "dac": (dims, np.zeros(n)),
            "source_flag": (dims, np.zeros(n, dtype="int64")),
            "basin_flag": (dims, np.zeros(n, dtype="int64")),
            "pass": (dims, np.zeros(n, dtype="int64")),
            "cycle": (dims, np.zeros(n, dtype="int64")),
        }
        coords = {
            "time": np.arange(n),
            "latitude": (dims, np.zeros(n)),
            "longitude": (dims, np.zeros(n)),
        }
        return xr.Dataset(data, coords=coords)

    def test_time_encoding(self):
        enc = daily_file_encoding(self._dataset())
        self.assertEqual(enc["time"]["units"], "seconds since 1990-01-01 00:00:00")
        self.assertEqual(enc["time"]["dtype"], "float64")
        self.assertIsNone(enc["time"]["_FillValue"])

    def test_lat_lon_are_float32_uncompressed_fill(self):
        enc = daily_file_encoding(self._dataset())
        for coord in ("latitude", "longitude"):
            self.assertEqual(enc[coord]["dtype"], "float32")
            self.assertIsNone(enc[coord]["_FillValue"])
            self.assertTrue(enc[coord]["zlib"])

    def test_data_vars_are_compressed(self):
        enc = daily_file_encoding(self._dataset())
        self.assertEqual(enc["ssh"]["complevel"], 5)
        self.assertTrue(enc["ssh"]["zlib"])

    def test_flag_vars_use_int8(self):
        enc = daily_file_encoding(self._dataset())
        self.assertEqual(enc["source_flag"]["dtype"], "int8")
        self.assertEqual(enc["source_flag"]["_FillValue"], np.iinfo(np.int8).max)

    def test_basin_pass_cycle_use_int32(self):
        enc = daily_file_encoding(self._dataset())
        for var in ("basin_flag", "pass", "cycle"):
            self.assertEqual(enc[var]["dtype"], "int32")
            self.assertEqual(enc[var]["_FillValue"], np.iinfo(np.int32).max)

    def test_ssh_dac_use_float64(self):
        enc = daily_file_encoding(self._dataset())
        for var in ("ssh", "dac"):
            self.assertEqual(enc[var]["dtype"], "float64")
            self.assertEqual(enc[var]["_FillValue"], np.finfo(np.float64).max)


class TestSimpleGridEncoding(unittest.TestCase):

    def _dataset(self):
        n = 3
        dims = ("time",)
        data = {
            "SSHA": (dims, np.zeros(n)),
            "basin_flag": (dims, np.zeros(n, dtype="int64")),
            "counts": (dims, np.zeros(n, dtype="int64")),
        }
        coords = {
            "time": np.arange(n),
            "latitude": (dims, np.zeros(n)),
            "longitude": (dims, np.zeros(n)),
        }
        return xr.Dataset(data, coords=coords)

    def test_time_encoding(self):
        enc = simple_grid_encoding(self._dataset())
        self.assertEqual(enc["time"]["dtype"], "float64")
        self.assertIsNone(enc["time"]["_FillValue"])

    def test_lat_lon_are_float32(self):
        enc = simple_grid_encoding(self._dataset())
        for coord in ("latitude", "longitude"):
            self.assertEqual(enc[coord]["dtype"], "float32")
            self.assertIsNone(enc[coord]["_FillValue"])

    def test_basin_flag_and_counts_use_int32(self):
        enc = simple_grid_encoding(self._dataset())
        for var in ("basin_flag", "counts"):
            self.assertEqual(enc[var]["dtype"], "int32")
            self.assertEqual(enc[var]["_FillValue"], np.iinfo(np.int32).max)

    def test_ssha_uses_float64(self):
        enc = simple_grid_encoding(self._dataset())
        self.assertEqual(enc["SSHA"]["dtype"], "float64")
        self.assertEqual(enc["SSHA"]["_FillValue"], np.finfo(np.float64).max)


if __name__ == "__main__":
    unittest.main()
