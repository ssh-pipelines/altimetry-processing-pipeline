"""
Tests for crossover processing.

ConsistencyTestCase ensures output remains identical throughout refactoring
by comparing against known-good reference output.

EmptyInputTestCase verifies correct behavior when no input data is provided.
"""
import gzip
import logging
import os
import re
import shutil
import tempfile
import unittest
from glob import glob
from unittest import mock

import numpy as np
import xarray as xr
from crossover.config.source_config import get_source_config
from crossover.loader import load_track_window
from crossover.processor import SPECS, CrossoverProcessor
from crossover.results import build_self_dataset, filter_and_sort, pack_records
from crossover.search import SelfCrossover, find_self_crossovers

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


def _self_dataset_from_streams(streams, day, source, df_version):
    """Drive the composed self path (load -> search -> accumulate -> build)
    from a list of local .nc streams, returning the crossover Dataset.

    Exercises the same seams CrossoverProcessor.run() uses, minus S3 I/O, so the
    consistency assertions run against the refactored modules."""
    config = get_source_config(source)
    next_day = day + np.timedelta64(1, "D")
    window_start = day
    window_end = day + np.timedelta64(config.window_size + config.window_padding, "D")

    window = load_track_window(streams)
    records = list(find_self_crossovers(window, config, day))
    columns = pack_records(records, SelfCrossover)
    columns = filter_and_sort(columns, next_day)
    return build_self_dataset(
        columns, source, day, df_version, window_start, window_end, config
    )


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

        streams = sorted(glob(os.path.join(tmp_inputs, "*.nc")))
        cls.computed_ds = _self_dataset_from_streams(
            streams, cls.day, cls.source, cls.df_version
        )

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


class RunEndToEndTestCase(unittest.TestCase):
    """Drive the full self path through Crossover.run() as a single entrypoint.

    The ConsistencyTestCase calls extract/search/create by hand, bypassing run(),
    stream_files, save_to_netcdf, and upload_xover. This test mocks aws_manager at
    the S3 boundary so run() executes end-to-end, then compares the NetCDF that run()
    saved-and-uploaded against the same known-good reference. It pins the orchestration
    wiring (step order, filter_and_sort, df_version threading) that the composed-processor
    refactor will move.
    """

    FLOAT_TOLERANCE = 1e-10
    TIME_TOLERANCE_NS = 200

    @classmethod
    def setUpClass(cls) -> None:
        cls.tmpdir = tempfile.mkdtemp()
        tmp_inputs = os.path.join(cls.tmpdir, "inputs")
        tmp_output = os.path.join(cls.tmpdir, "output")
        _decompress_gz_files(os.path.join(SAMPLE_DATA_DIR, "sample_inputs"), tmp_inputs)
        _decompress_gz_files(os.path.join(SAMPLE_DATA_DIR, "sample_output"), tmp_output)

        # Map the 8-digit date token in a requested daily-file key to its local sample.
        cls.by_date = {}
        for path in glob(os.path.join(tmp_inputs, "*.nc")):
            m = re.search(r"(\d{8})", os.path.basename(path))
            if m:
                cls.by_date[m.group(1)] = path

        cls.day = np.datetime64("2025-01-01")
        cls.source = "S6"
        cls.df_version = "p1"

        def fake_key_exists(key):
            m = re.search(r"(\d{8})", key)
            return bool(m) and m.group(1) in cls.by_date

        def fake_stream_obj(key):
            return cls.by_date[re.search(r"(\d{8})", key).group(1)]

        cls.uploaded = []

        processor = CrossoverProcessor(
            cls.day, cls.source, cls.df_version, SPECS["self"]
        )
        # aws_manager is imported into loader (stream/key_exists) and processor
        # (upload); patch both so run() executes end-to-end without S3.
        with mock.patch("crossover.loader.aws_manager") as loader_mgr, mock.patch(
            "crossover.processor.aws_manager"
        ) as proc_mgr:
            loader_mgr.key_exists.side_effect = fake_key_exists
            loader_mgr.stream_obj.side_effect = fake_stream_obj
            proc_mgr.upload_obj.side_effect = lambda src, dest: cls.uploaded.append(src)
            processor.run(bucket="test-bucket")

        cls.local_path = cls.uploaded[0]
        cls.computed_ds = xr.open_dataset(cls.local_path, engine="h5netcdf")
        cls.reference_ds = xr.open_dataset(
            os.path.join(tmp_output, "xovers_S6-2025-01-01.nc"),
            engine="h5netcdf",
        )

    @classmethod
    def tearDownClass(cls) -> None:
        for ds_attr in ("computed_ds", "reference_ds"):
            if hasattr(cls, ds_attr):
                getattr(cls, ds_attr).close()
        if getattr(cls, "local_path", None) and os.path.exists(cls.local_path):
            os.remove(cls.local_path)
        if hasattr(cls, "tmpdir"):
            shutil.rmtree(cls.tmpdir, ignore_errors=True)

    def test_uploaded_exactly_once(self):
        self.assertEqual(len(self.uploaded), 1)

    def test_run_output_matches_reference_length(self):
        self.assertEqual(
            len(self.computed_ds["time1"]), len(self.reference_ds["time1"])
        )

    def test_run_output_matches_reference_fields(self):
        for field, atol in (
            ("lon", self.FLOAT_TOLERANCE),
            ("lat", self.FLOAT_TOLERANCE),
            ("ssh1", self.FLOAT_TOLERANCE),
            ("ssh2", self.FLOAT_TOLERANCE),
        ):
            np.testing.assert_allclose(
                self.computed_ds[field].values,
                self.reference_ds[field].values,
                rtol=0,
                atol=atol,
                err_msg=f"{field} from run() diverges from reference",
            )
        for field in ("cycle1", "cycle2", "pass1", "pass2"):
            np.testing.assert_array_equal(
                self.computed_ds[field].values,
                self.reference_ds[field].values,
                err_msg=f"{field} from run() diverges from reference",
            )
        for field in ("time1", "time2"):
            diff = np.abs(
                self.computed_ds[field].values.astype("int64")
                - self.reference_ds[field].values.astype("int64")
            )
            self.assertLessEqual(diff.max(), self.TIME_TOLERANCE_NS)

    def test_run_output_sorted_and_within_day(self):
        time1 = self.computed_ds["time1"].values
        self.assertTrue(np.all(np.diff(time1.astype("int64")) >= 0))
        self.assertGreaterEqual(time1.min(), self.day)
        self.assertLess(time1.max(), self.day + np.timedelta64(1, "D"))


class EmptyInputTestCase(unittest.TestCase):
    """Test crossover output when no input streams are provided."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.day = np.datetime64("2022-01-01")
        cls.source = "GSFC"
        cls.df_version = "p1"

        cls.ds = _self_dataset_from_streams([], cls.day, cls.source, cls.df_version)

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
        self.assertEqual(self.ds.attrs["crossover_type"], "self")


class AllNaNInputTestCase(unittest.TestCase):
    """Test crossover output when daily files exist but all SSH values are NaN."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.tmpdir = tempfile.mkdtemp()
        cls.day = np.datetime64("2006-11-01")
        cls.source = "GSFC"
        cls.df_version = "p1"

        # Create a minimal daily file where all SSH values are NaN
        n = 10
        nan_ds = xr.Dataset(
            {
                "ssha_smoothed": ("time", np.full(n, np.nan)),
                "longitude": ("time", np.linspace(0, 10, n)),
                "latitude": ("time", np.linspace(-5, 5, n)),
                "cycle": ("time", np.ones(n, dtype="int32")),
                "pass": ("time", np.ones(n, dtype="int32")),
            },
            coords={"time": cls.day + np.arange(n).astype("timedelta64[D]")},
        )
        cls.nan_file = os.path.join(cls.tmpdir, "nan_daily.nc")
        nan_ds.to_netcdf(cls.nan_file, engine="h5netcdf")
        nan_ds.close()

        cls.ds = _self_dataset_from_streams(
            [cls.nan_file], cls.day, cls.source, cls.df_version
        )

    @classmethod
    def tearDownClass(cls) -> None:
        if hasattr(cls, "tmpdir"):
            shutil.rmtree(cls.tmpdir, ignore_errors=True)

    def test_valid_length(self):
        self.assertEqual(len(self.ds.time1), 0)

    def test_netcdf_vars(self):
        self.assertIn("time1", self.ds.dims)
        self.assertIn("time2", self.ds.data_vars)
        self.assertIn("ssh1", self.ds.data_vars)
        self.assertIn("ssh2", self.ds.data_vars)

    def test_saves_to_netcdf(self):
        out_path = os.path.join(self.tmpdir, "output.nc")
        self.ds.to_netcdf(out_path, engine="h5netcdf")
        loaded = xr.open_dataset(out_path, engine="h5netcdf")
        self.assertEqual(len(loaded.time1), 0)
        loaded.close()


if __name__ == "__main__":
    unittest.main()
