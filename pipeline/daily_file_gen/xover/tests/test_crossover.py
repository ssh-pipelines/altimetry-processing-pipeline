"""
Tests for crossover processing.

ConsistencyTestCase ensures output remains identical throughout refactoring
by comparing against known-good reference output.

EmptyInputTestCase verifies correct behavior when no input data is provided.
"""
import gzip
import logging
import os
import shutil
import tempfile
import unittest
from glob import glob

import numpy as np
import xarray as xr
from crossover.parallel_crossovers import Crossover, CrossoverData


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
    """Test crossover output against reference data."""

    # Tolerances for floating point comparisons
    FLOAT_TOLERANCE = 1e-10
    SSH_TOLERANCE = 1e-10
    # 128ns max diff observed from float64->int64 rounding in xover_ssh interpolation
    TIME_TOLERANCE_NS = 200

    @classmethod
    def setUpClass(cls) -> None:
        """Decompress sample data, process it, and load reference output."""
        cls.tmpdir = tempfile.mkdtemp()
        tmp_inputs = os.path.join(cls.tmpdir, "inputs")
        tmp_output = os.path.join(cls.tmpdir, "output")
        _decompress_gz_files(os.path.join(SAMPLE_DATA_DIR, "sample_inputs"), tmp_inputs)
        _decompress_gz_files(os.path.join(SAMPLE_DATA_DIR, "sample_output"), tmp_output)

        cls.day = np.datetime64("2025-01-01")
        cls.source = "S6"
        cls.df_version = "p1"

        cls.processor = Crossover(cls.day, cls.source, cls.df_version)
        cls.processor.crossover_data = CrossoverData.init()
        cls.processor.streams = sorted(glob(os.path.join(tmp_inputs, "*.nc")))

        if len(cls.processor.streams) > 0:
            cls.processor.extract_and_set_data()
            cls.processor.search_day_for_crossovers()

        cls.computed_ds = cls.processor.create_dataset()

        cls.reference_ds = xr.open_dataset(
            os.path.join(tmp_output, "xovers_S6-2025-01-01.nc"),
            engine="h5netcdf",
        )

    @classmethod
    def tearDownClass(cls) -> None:
        if hasattr(cls, "reference_ds"):
            cls.reference_ds.close()
        if hasattr(cls, "tmpdir"):
            shutil.rmtree(cls.tmpdir, ignore_errors=True)

    def test_array_lengths_match(self):
        computed_len = len(self.computed_ds["time1"])
        reference_len = len(self.reference_ds["time1"])
        self.assertEqual(
            computed_len,
            reference_len,
            f"Array length mismatch: computed={computed_len}, reference={reference_len}",
        )

    def test_time1_match(self):
        computed = self.computed_ds["time1"].values.astype("int64")
        reference = self.reference_ds["time1"].values.astype("int64")
        diff = np.abs(computed - reference)
        max_diff = diff.max()
        self.assertLessEqual(
            max_diff,
            self.TIME_TOLERANCE_NS,
            f"time1 max diff {max_diff}ns exceeds {self.TIME_TOLERANCE_NS}ns tolerance",
        )

    def test_time2_match(self):
        computed = self.computed_ds["time2"].values.astype("int64")
        reference = self.reference_ds["time2"].values.astype("int64")
        diff = np.abs(computed - reference)
        max_diff = diff.max()
        self.assertLessEqual(
            max_diff,
            self.TIME_TOLERANCE_NS,
            f"time2 max diff {max_diff}ns exceeds {self.TIME_TOLERANCE_NS}ns tolerance",
        )

    def test_lon_within_tolerance(self):
        computed = self.computed_ds["lon"].values
        reference = self.reference_ds["lon"].values
        np.testing.assert_allclose(
            computed,
            reference,
            rtol=0,
            atol=self.FLOAT_TOLERANCE,
            err_msg="lon values exceed tolerance from reference",
        )

    def test_lat_within_tolerance(self):
        computed = self.computed_ds["lat"].values
        reference = self.reference_ds["lat"].values
        np.testing.assert_allclose(
            computed,
            reference,
            rtol=0,
            atol=self.FLOAT_TOLERANCE,
            err_msg="lat values exceed tolerance from reference",
        )

    def test_ssh1_within_tolerance(self):
        computed = self.computed_ds["ssh1"].values
        reference = self.reference_ds["ssh1"].values
        np.testing.assert_allclose(
            computed,
            reference,
            rtol=0,
            atol=self.SSH_TOLERANCE,
            err_msg="ssh1 values exceed tolerance from reference",
        )

    def test_ssh2_within_tolerance(self):
        computed = self.computed_ds["ssh2"].values
        reference = self.reference_ds["ssh2"].values
        np.testing.assert_allclose(
            computed,
            reference,
            rtol=0,
            atol=self.SSH_TOLERANCE,
            err_msg="ssh2 values exceed tolerance from reference",
        )

    def test_cycle1_exact_match(self):
        computed = self.computed_ds["cycle1"].values
        reference = self.reference_ds["cycle1"].values
        np.testing.assert_array_equal(
            computed,
            reference,
            err_msg="cycle1 values do not match reference",
        )

    def test_cycle2_exact_match(self):
        computed = self.computed_ds["cycle2"].values
        reference = self.reference_ds["cycle2"].values
        np.testing.assert_array_equal(
            computed,
            reference,
            err_msg="cycle2 values do not match reference",
        )

    def test_pass1_exact_match(self):
        computed = self.computed_ds["pass1"].values
        reference = self.reference_ds["pass1"].values
        np.testing.assert_array_equal(
            computed,
            reference,
            err_msg="pass1 values do not match reference",
        )

    def test_pass2_exact_match(self):
        computed = self.computed_ds["pass2"].values
        reference = self.reference_ds["pass2"].values
        np.testing.assert_array_equal(
            computed,
            reference,
            err_msg="pass2 values do not match reference",
        )

    def test_sorted_by_time1(self):
        time1_values = self.computed_ds["time1"].values.astype("int64")
        self.assertTrue(
            np.all(np.diff(time1_values) >= 0),
            "Output is not sorted by time1",
        )

    def test_time1_within_processing_day(self):
        time1_min = self.computed_ds["time1"].values.min()
        time1_max = self.computed_ds["time1"].values.max()
        next_day = self.day + np.timedelta64(1, "D")

        self.assertGreaterEqual(
            time1_min,
            self.day,
            f"time1 min {time1_min} is before processing day {self.day}",
        )
        self.assertLess(
            time1_max,
            next_day,
            f"time1 max {time1_max} is on or after next day {next_day}",
        )


class EmptyInputTestCase(unittest.TestCase):
    """Test crossover output when no input streams are provided."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.day = np.datetime64("2022-01-01")
        cls.source = "GSFC"
        cls.df_version = "p1"

        cls.processor = Crossover(cls.day, cls.source, cls.df_version)
        cls.processor.crossover_data = CrossoverData.init()
        cls.processor.streams = []
        cls.ds = cls.processor.create_dataset()

    def test_valid_length(self):
        self.assertEqual(len(self.ds.time1), 0)

    def test_netcdf_vars(self):
        self.assertIn("time1", self.ds.dims)
        self.assertIn("time2", self.ds.data_vars)
        self.assertIn("ssh1", self.ds.data_vars)
        self.assertIn("ssh2", self.ds.data_vars)
        self.assertIn("cycle1", self.ds.data_vars)
        self.assertIn("cycle2", self.ds.data_vars)
        self.assertIn("pass1", self.ds.data_vars)
        self.assertIn("pass2", self.ds.data_vars)
        self.assertIn("lon", self.ds.data_vars)
        self.assertIn("lat", self.ds.data_vars)

    def test_netcdf_attrs(self):
        self.assertIn("GSFC self-crossovers", self.ds.attrs["title"])
        self.assertEqual(self.ds.attrs["input_product_generation_steps"], "1")
        self.assertEqual(self.ds.attrs["satellite_names"], "GSFC")


if __name__ == "__main__":
    unittest.main()
