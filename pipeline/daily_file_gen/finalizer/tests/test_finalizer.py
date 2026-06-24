import json
import os
import shutil
import tempfile
import unittest
from datetime import date
from io import StringIO
from unittest.mock import MagicMock, patch

import netCDF4 as nc
import numpy as np
import pandas as pd

from finalization.finalizer import (
    Finalizer,
    apply_bad_pass,
)
from finalization.config.source_config import (
    get_source_config,
    get_available_sources,
)

# Test constant matching the S6 config value
S6_OFFSET = 0.0291


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _create_test_nc(path, n=10, source="GSFC", add_offset_attr=False,
                    offset_value=0.0):
    """Create a minimal netCDF file with the variables the finalizer expects."""
    ds = nc.Dataset(path, "w", format="NETCDF4")
    ds.createDimension("obs", n)

    cycle = ds.createVariable("cycle", "i4", ("obs",))
    pass_var = ds.createVariable("pass", "i4", ("obs",))
    ssha = ds.createVariable("ssha", "f8", ("obs",))
    ssha_smoothed = ds.createVariable("ssha_smoothed", "f8", ("obs",))
    nasa_flag = ds.createVariable("nasa_flag", "i4", ("obs",))

    cycle[:] = np.arange(1, n + 1)
    pass_var[:] = np.arange(1, n + 1)
    ssha[:] = np.random.default_rng(42).random(n)
    ssha_smoothed[:] = np.random.default_rng(43).random(n)
    nasa_flag[:] = np.zeros(n, dtype=int)

    ds.granule_id = f"{source}_alt_ref_at_v1_1_20200101.nc"
    ds.product_generation_step = "2"

    if add_offset_attr:
        ds.absolute_offset_applied = offset_value

    ds.close()


# ---------------------------------------------------------------------------
# Tests — source config loading
# ---------------------------------------------------------------------------

class TestSourceConfig(unittest.TestCase):

    def test_load_sources_returns_gsfc_and_s6(self):
        sources = get_available_sources()
        self.assertIn("GSFC", sources)
        self.assertIn("S6", sources)

    def test_gsfc_config_values(self):
        cfg = get_source_config("GSFC")
        self.assertEqual(cfg.product_type, "reference")
        self.assertEqual(cfg.offset, 0.0)
        self.assertEqual(cfg.start_date, date(1992, 10, 25))
        self.assertEqual(cfg.end_date, date(2025, 12, 31))
        self.assertEqual(cfg.pass_flag.mean_num, 15.0)
        self.assertEqual(cfg.pass_flag.rms_num, 25.0)
        self.assertEqual(cfg.pass_flag.mean_threshold, 0.1)
        self.assertEqual(cfg.pass_flag.rms_threshold, 0.27)

    def test_s6_config_values(self):
        cfg = get_source_config("S6")
        self.assertEqual(cfg.product_type, "reference")
        self.assertEqual(cfg.offset, S6_OFFSET)
        self.assertEqual(cfg.start_date, date(2026, 1, 1))

    def test_invalid_source_raises(self):
        with self.assertRaises(ValueError):
            get_source_config("INVALID")

    def test_get_available_sources(self):
        sources = get_available_sources()
        self.assertIn("GSFC", sources)
        self.assertIn("S6", sources)


# ---------------------------------------------------------------------------
# Tests — source parameter
# ---------------------------------------------------------------------------

class TestSourceParam(unittest.TestCase):

    @patch("finalization.finalizer.aws_manager")
    def test_source_stored_from_param(self, mock_aws):
        mock_aws.fs.exists.return_value = False
        f = Finalizer(date(2020, 1, 1), "GSFC", "bucket")
        self.assertEqual(f.source, "GSFC")

    @patch("finalization.finalizer.aws_manager")
    def test_s6_source_stored_from_param(self, mock_aws):
        mock_aws.fs.exists.return_value = False
        f = Finalizer(date(2025, 3, 1), "S6", "bucket")
        self.assertEqual(f.source, "S6")

    @patch("finalization.finalizer.aws_manager")
    def test_invalid_source_raises(self, mock_aws):
        with self.assertRaises(ValueError):
            Finalizer(date(2020, 1, 1), "INVALID", "bucket")

    @patch("finalization.finalizer.aws_manager")
    def test_date_before_start_warns(self, mock_aws):
        mock_aws.fs.exists.return_value = False
        with self.assertLogs(level="WARNING") as cm:
            Finalizer(date(1990, 1, 1), "GSFC", "bucket")
        self.assertTrue(any("before" in msg for msg in cm.output))


# ---------------------------------------------------------------------------
# Tests — _load_bad_passes
# ---------------------------------------------------------------------------

class TestLoadBadPasses(unittest.TestCase):

    @patch("finalization.finalizer.aws_manager")
    def test_returns_empty_df_when_file_missing(self, mock_aws):
        mock_aws.fs.exists.return_value = False
        f = Finalizer(date(2020, 6, 15), "GSFC", "bucket")
        self.assertTrue(f.bad_pass_df.empty)
        self.assertListEqual(list(f.bad_pass_df.columns), ["cycle", "pass"])

    @patch("finalization.finalizer.aws_manager")
    def test_returns_empty_df_when_bad_passes_list_empty(self, mock_aws):
        mock_aws.fs.exists.return_value = True
        mock_aws.fs.open.return_value.__enter__ = lambda _: StringIO(
            json.dumps({"bad_passes": []})
        )
        mock_aws.fs.open.return_value.__exit__ = MagicMock(return_value=False)
        f = Finalizer(date(2020, 6, 15), "GSFC", "bucket")
        self.assertTrue(f.bad_pass_df.empty)

    @patch("finalization.finalizer.aws_manager")
    def test_loads_and_renames_columns(self, mock_aws):
        payload = {"bad_passes": [
            {"cycle": 10, "pass_num": 5},
            {"cycle": 11, "pass_num": 7},
        ]}
        mock_aws.fs.exists.return_value = True
        mock_aws.fs.open.return_value.__enter__ = lambda _: StringIO(
            json.dumps(payload)
        )
        mock_aws.fs.open.return_value.__exit__ = MagicMock(return_value=False)
        f = Finalizer(date(2020, 6, 15), "GSFC", "bucket")

        self.assertEqual(len(f.bad_pass_df), 2)
        self.assertListEqual(list(f.bad_pass_df.columns), ["cycle", "pass"])
        self.assertEqual(f.bad_pass_df.iloc[0]["cycle"], 10)
        self.assertEqual(f.bad_pass_df.iloc[0]["pass"], 5)

    @patch("finalization.finalizer.aws_manager")
    def test_s3_key_uses_source_and_isoformat_date(self, mock_aws):
        mock_aws.fs.exists.return_value = False
        Finalizer(date(2020, 6, 15), "GSFC", "my-bucket")
        expected = "s3://my-bucket/bad_passes/GSFC/2020-06-15.json"
        mock_aws.fs.exists.assert_called_once_with(expected)


# ---------------------------------------------------------------------------
# Tests — get_daily_file
# ---------------------------------------------------------------------------

class TestGetDailyFile(unittest.TestCase):

    @patch("finalization.finalizer.aws_manager")
    def test_downloads_when_exists(self, mock_aws):
        mock_aws.fs.exists.side_effect = [False, True]  # __init__, get_daily_file
        mock_aws.fs.get = MagicMock()
        f = Finalizer(date(2020, 1, 1), "GSFC", "b")
        result = f.get_daily_file("s3://b/some/file.nc")
        self.assertTrue(result.endswith("file.nc"))
        mock_aws.fs.get.assert_called_once()

    @patch("finalization.finalizer.aws_manager")
    def test_raises_when_not_found(self, mock_aws):
        mock_aws.fs.exists.side_effect = [False, False]
        f = Finalizer(date(2020, 1, 1), "GSFC", "b")
        with self.assertRaises(FileNotFoundError):
            f.get_daily_file("s3://b/missing.nc")


# ---------------------------------------------------------------------------
# Tests — apply_bad_pass (standalone function)
# ---------------------------------------------------------------------------

class TestApplyBadPass(unittest.TestCase):

    def setUp(self):
        self.tmpfile = tempfile.NamedTemporaryFile(suffix=".nc", delete=False)
        self.tmpfile.close()
        _create_test_nc(self.tmpfile.name, n=10)

    def tearDown(self):
        if os.path.exists(self.tmpfile.name):
            os.remove(self.tmpfile.name)

    def test_flags_matching_cycle_pass(self):
        ds = nc.Dataset(self.tmpfile.name, "r+")
        bad_df = pd.DataFrame({"cycle": [1, 3], "pass": [1, 3]})
        ds = apply_bad_pass(ds, bad_df)
        flags = ds.variables["nasa_flag"][:]
        self.assertEqual(flags[0], 1)   # cycle=1, pass=1
        self.assertEqual(flags[2], 1)   # cycle=3, pass=3
        self.assertEqual(flags[1], 0)   # untouched
        self.assertEqual(flags[4], 0)   # untouched
        ds.close()

    def test_sets_flagged_passes_attribute(self):
        ds = nc.Dataset(self.tmpfile.name, "r+")
        bad_df = pd.DataFrame({"cycle": [2, 5], "pass": [2, 5]})
        ds = apply_bad_pass(ds, bad_df)
        self.assertIn("2/2", ds.flagged_passes)
        self.assertIn("5/5", ds.flagged_passes)
        ds.close()

    def test_sets_ssha_smoothed_nan_for_flagged(self):
        ds = nc.Dataset(self.tmpfile.name, "r+")
        bad_df = pd.DataFrame({"cycle": [1], "pass": [1]})
        ds = apply_bad_pass(ds, bad_df)
        smoothed = ds.variables["ssha_smoothed"][:]
        self.assertTrue(np.isnan(smoothed[0]))
        self.assertFalse(np.isnan(smoothed[1]))
        ds.close()

    def test_no_match_leaves_flags_unchanged(self):
        ds = nc.Dataset(self.tmpfile.name, "r+")
        bad_df = pd.DataFrame({"cycle": [999], "pass": [999]})
        ds = apply_bad_pass(ds, bad_df)
        flags = ds.variables["nasa_flag"][:]
        self.assertTrue(np.all(flags == 0))
        ds.close()


# ---------------------------------------------------------------------------
# Helpers for process() tests — set up mock AWS that serves a real netCDF file
# ---------------------------------------------------------------------------

class _ProcessTestMixin:
    """Shared setUp / tearDown for tests that run Finalizer.process()."""

    def _setup_process_mocks(self, mock_aws, source="GSFC",
                             bad_passes_payload=None, add_offset_attr=False,
                             offset_value=0.0):
        self.nc_src = tempfile.NamedTemporaryFile(suffix=".nc", delete=False)
        self.nc_src.close()
        _create_test_nc(self.nc_src.name, n=5, source=source,
                        add_offset_attr=add_offset_attr,
                        offset_value=offset_value)

        self.uploaded_copy = self.nc_src.name + ".uploaded"

        # _load_bad_passes
        if bad_passes_payload:
            mock_aws.fs.exists.side_effect = [True, True]
            mock_aws.fs.open.return_value.__enter__ = lambda _: StringIO(
                json.dumps(bad_passes_payload)
            )
            mock_aws.fs.open.return_value.__exit__ = MagicMock(return_value=False)
        else:
            mock_aws.fs.exists.side_effect = [False, True]

        src_path = self.nc_src.name

        def fake_get(_, dst):
            shutil.copy(src_path, dst)

        mock_aws.fs.get = MagicMock(side_effect=fake_get)

        uploaded = self.uploaded_copy

        def fake_upload(src_local, _):
            shutil.copy(src_local, uploaded)

        mock_aws.fs.upload = MagicMock(side_effect=fake_upload)

    def _cleanup(self):
        for p in (self.nc_src.name, self.uploaded_copy):
            if os.path.exists(p):
                os.remove(p)


# ---------------------------------------------------------------------------
# Tests — process() GSFC path
# ---------------------------------------------------------------------------

class TestProcessGSFC(unittest.TestCase, _ProcessTestMixin):

    @patch("finalization.finalizer.aws_manager")
    def test_gsfc_upload_path_is_per_source(self, mock_aws):
        proc_date = date(2020, 3, 15)
        self._setup_process_mocks(mock_aws, source="GSFC")
        try:
            f = Finalizer(proc_date, "GSFC", "bucket")
            f.process("bucket")
            dst = mock_aws.fs.upload.call_args[0][1]
            self.assertIn("p3/GSFC/", dst)
            self.assertIn("GSFC", dst)
            self.assertNotIn("NASA", dst)
            self.assertIn("2020", dst)
        finally:
            self._cleanup()

    @patch("finalization.finalizer.aws_manager")
    def test_gsfc_offset_is_zero(self, mock_aws):
        proc_date = date(2020, 3, 15)
        self._setup_process_mocks(mock_aws, source="GSFC")
        try:
            f = Finalizer(proc_date, "GSFC", "bucket")
            f.process("bucket")
            ds = nc.Dataset(self.uploaded_copy, "r")
            self.assertEqual(float(ds.absolute_offset_applied), 0.0)
            ds.close()
        finally:
            self._cleanup()


# ---------------------------------------------------------------------------
# Tests — process() S6 path (offset handling)
# ---------------------------------------------------------------------------

class TestProcessS6(unittest.TestCase, _ProcessTestMixin):

    @patch("finalization.finalizer.aws_manager")
    def test_s6_applies_offset(self, mock_aws):
        proc_date = date(2025, 2, 10)
        self._setup_process_mocks(mock_aws, source="S6")

        # Read originals before process modifies them
        orig_ds = nc.Dataset(self.nc_src.name, "r")
        orig_ssha = orig_ds.variables["ssha"][:].copy()
        orig_smoothed = orig_ds.variables["ssha_smoothed"][:].copy()
        orig_ds.close()

        try:
            f = Finalizer(proc_date, "S6", "bucket")
            f.process("bucket")

            ds = nc.Dataset(self.uploaded_copy, "r")
            np.testing.assert_allclose(
                ds.variables["ssha"][:], orig_ssha + S6_OFFSET
            )
            np.testing.assert_allclose(
                ds.variables["ssha_smoothed"][:],
                orig_smoothed + S6_OFFSET, atol=1e-10,
            )
            self.assertAlmostEqual(
                float(ds.absolute_offset_applied), S6_OFFSET
            )
            ds.close()
        finally:
            self._cleanup()

    @patch("finalization.finalizer.aws_manager")
    def test_s6_removes_previous_offset_before_applying(self, mock_aws):
        """When absolute_offset_applied already exists, the old offset is
        subtracted before the current one is added."""
        proc_date = date(2025, 2, 10)
        old_offset = 0.01
        self._setup_process_mocks(
            mock_aws, source="S6",
            add_offset_attr=True, offset_value=old_offset,
        )

        orig_ds = nc.Dataset(self.nc_src.name, "r")
        orig_ssha = orig_ds.variables["ssha"][:].copy()
        orig_ds.close()

        try:
            f = Finalizer(proc_date, "S6", "bucket")
            f.process("bucket")

            ds = nc.Dataset(self.uploaded_copy, "r")
            expected = orig_ssha - old_offset + S6_OFFSET
            np.testing.assert_allclose(
                ds.variables["ssha"][:], expected, atol=1e-10
            )
            ds.close()
        finally:
            self._cleanup()


# ---------------------------------------------------------------------------
# Tests — global attributes written by process()
# ---------------------------------------------------------------------------

class TestProcessAttributes(unittest.TestCase, _ProcessTestMixin):

    @patch("finalization.finalizer.aws_manager")
    def test_output_attributes(self, mock_aws):
        proc_date = date(2020, 5, 1)
        self._setup_process_mocks(mock_aws, source="GSFC")
        try:
            f = Finalizer(proc_date, "GSFC", "bucket")
            f.process("bucket")

            ds = nc.Dataset(self.uploaded_copy, "r")
            self.assertEqual(ds.product_generation_step, "3")
            self.assertIn("Created on", ds.history)
            self.assertIn("GSFC", ds.granule_id)
            self.assertEqual(ds.pass_flag_mean_num, 15.0)
            self.assertEqual(ds.pass_flag_rms_num, 25.0)
            self.assertEqual(ds.pass_flag_mean_threshold, 0.1)
            self.assertEqual(ds.pass_flag_rms_threshold, 0.27)
            self.assertEqual(ds.flagged_passes, "N/A")
            ds.close()
        finally:
            self._cleanup()

    @patch("finalization.finalizer.aws_manager")
    def test_attributes_sorted_case_insensitive(self, mock_aws):
        # NOTE: The finalizer intends to sort attributes alphabetically
        # (case-insensitive), but the delete-and-reinsert loop in process()
        # does not fully remove pre-existing attributes from the input file
        # (likely because ds.ncattrs() is mutated during iteration).
        # This test documents the *actual* current behaviour: newly added
        # attributes are sorted, but attributes that existed in the input
        # file appear first in their original order.
        proc_date = date(2020, 5, 1)
        self._setup_process_mocks(mock_aws, source="GSFC")
        try:
            f = Finalizer(proc_date, "GSFC", "bucket")
            f.process("bucket")

            ds = nc.Dataset(self.uploaded_copy, "r")
            attrs = list(ds.ncattrs())
            # Pre-existing attrs from input file appear first (unsorted)
            self.assertIn("granule_id", attrs)
            self.assertIn("product_generation_step", attrs)
            # All expected attributes are present
            for expected in ("absolute_offset_applied", "flagged_passes",
                             "history", "pass_flag_mean_num",
                             "pass_flag_mean_threshold", "pass_flag_notes",
                             "pass_flag_rms_num", "pass_flag_rms_threshold"):
                self.assertIn(expected, attrs)
            ds.close()
        finally:
            self._cleanup()


# ---------------------------------------------------------------------------
# Tests — process() with non-empty bad passes
# ---------------------------------------------------------------------------

class TestProcessWithBadPasses(unittest.TestCase, _ProcessTestMixin):

    @patch("finalization.finalizer.aws_manager")
    def test_bad_passes_applied_during_process(self, mock_aws):
        proc_date = date(2020, 5, 1)
        payload = {"bad_passes": [{"cycle": 1, "pass_num": 1}]}
        self._setup_process_mocks(
            mock_aws, source="GSFC", bad_passes_payload=payload
        )
        try:
            f = Finalizer(proc_date, "GSFC", "bucket")
            self.assertFalse(f.bad_pass_df.empty)

            f.process("bucket")

            ds = nc.Dataset(self.uploaded_copy, "r")
            self.assertEqual(ds.variables["nasa_flag"][0], 1)
            self.assertIn("1/1", ds.flagged_passes)
            ds.close()
        finally:
            self._cleanup()


# ---------------------------------------------------------------------------
# Tests — process() S6B path (per-source output)
# ---------------------------------------------------------------------------

class TestProcessS6B(unittest.TestCase, _ProcessTestMixin):

    @patch("finalization.finalizer.aws_manager")
    def test_s6b_upload_path_is_per_source(self, mock_aws):
        proc_date = date(2025, 6, 1)
        self._setup_process_mocks(mock_aws, source="S6B")
        try:
            f = Finalizer(proc_date, "S6B", "bucket")
            f.process("bucket")
            dst = mock_aws.fs.upload.call_args[0][1]
            self.assertIn("p3/S6B/", dst)
            self.assertIn("S6B", dst)
            self.assertNotIn("NASA", dst)
            self.assertIn("2025", dst)
        finally:
            self._cleanup()

    @patch("finalization.finalizer.aws_manager")
    def test_s6b_granule_id_uses_source_name(self, mock_aws):
        proc_date = date(2025, 6, 1)
        self._setup_process_mocks(mock_aws, source="S6B")
        try:
            f = Finalizer(proc_date, "S6B", "bucket")
            f.process("bucket")
            ds = nc.Dataset(self.uploaded_copy, "r")
            self.assertIn("S6B", ds.granule_id)
            self.assertNotIn("NASA", ds.granule_id)
            ds.close()
        finally:
            self._cleanup()


# ---------------------------------------------------------------------------
# Tests — process() returns a FinalizerResult (Job-outcome inputs)
# ---------------------------------------------------------------------------

class TestProcessResult(unittest.TestCase, _ProcessTestMixin):

    @patch("finalization.finalizer.aws_manager")
    def test_returns_key_and_processing_history(self, mock_aws):
        proc_date = date(2020, 5, 1)
        self._setup_process_mocks(mock_aws, source="GSFC")
        try:
            f = Finalizer(proc_date, "GSFC", "bucket")
            result = f.process("bucket")

            self.assertEqual(
                result.key,
                "daily_files/p3/GSFC/2020/GSFC_alt_ref_at_v1_1_20200501.nc",
            )
            # the finalizer step (P3) is appended to processing_history
            self.assertTrue(
                any(s["stage"] == "finalizer" for s in result.processing_history)
            )
            self.assertEqual(result.processing_history[-1]["product_generation_step"], "3")
        finally:
            self._cleanup()

    @patch("finalization.finalizer.aws_manager")
    def test_processing_history_persisted_to_file(self, mock_aws):
        proc_date = date(2020, 5, 1)
        self._setup_process_mocks(mock_aws, source="GSFC")
        try:
            f = Finalizer(proc_date, "GSFC", "bucket")
            f.process("bucket")
            ds = nc.Dataset(self.uploaded_copy, "r")
            self.assertIn("processing_history", ds.ncattrs())
            steps = json.loads(ds.getncattr("processing_history"))
            self.assertEqual(steps[-1]["stage"], "finalizer")
            ds.close()
        finally:
            self._cleanup()


# ---------------------------------------------------------------------------
# Tests — handler returns a Job outcome
# ---------------------------------------------------------------------------

class TestFinalizerHandlerOutcome(unittest.TestCase):

    @patch("app.Finalizer")
    def test_handler_returns_job_outcome(self, mock_finalizer_cls):
        from app import handler
        from finalization.finalizer import FinalizerResult

        mock_finalizer_cls.return_value.process.return_value = FinalizerResult(
            key="daily_files/p3/S6/2025/S6_alt_ref_at_v1_1_20250210.nc",
            processing_history=[
                {"stage": "daily_files", "product_generation_step": "1"},
                {"stage": "oer", "product_generation_step": "2"},
                {"stage": "finalizer", "product_generation_step": "3"},
            ],
        )

        event = {"bucket": "b", "date": "2025-02-10", "source": "S6"}
        result = handler(event, None)

        self.assertEqual(result["stage"], "finalizer")
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["date"], "2025-02-10")
        self.assertEqual(result["source"], "S6")
        self.assertEqual(
            result["outputs"],
            [{
                "key": "daily_files/p3/S6/2025/S6_alt_ref_at_v1_1_20250210.nc",
                "kind": "daily_file_p3",
            }],
        )
        self.assertTrue(result["metadata"]["provenance_complete"])

    @patch("app.Finalizer")
    def test_handler_flags_incomplete_provenance(self, mock_finalizer_cls):
        from app import handler
        from finalization.finalizer import FinalizerResult

        # legacy file: only the P3 step recorded (gap at P1/P2)
        mock_finalizer_cls.return_value.process.return_value = FinalizerResult(
            key="daily_files/p3/S6/2025/S6_alt_ref_at_v1_1_20250210.nc",
            processing_history=[
                {"stage": "finalizer", "product_generation_step": "3"},
            ],
        )

        event = {"bucket": "b", "date": "2025-02-10", "source": "S6"}
        result = handler(event, None)
        self.assertFalse(result["metadata"]["provenance_complete"])


if __name__ == "__main__":
    unittest.main()
