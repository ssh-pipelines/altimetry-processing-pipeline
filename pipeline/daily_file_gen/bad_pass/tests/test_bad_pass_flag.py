import json
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, mock_open, patch

import numpy as np

from bad_passes.bad_pass_flag import XoverProcessor


class TestXoverProcessorInit(unittest.TestCase):
    """Tests for XoverProcessor.__init__."""

    def test_init_sets_window(self):
        date = datetime(2024, 1, 15)
        proc = XoverProcessor("GSFC", date)

        self.assertEqual(proc.source, "GSFC")
        self.assertEqual(proc.date, date)
        self.assertEqual(proc.windowlen, 10)
        self.assertEqual(proc.windowpad, 1)
        # window_start = date - 10 days - 1 day = Jan 4
        self.assertEqual(proc.window_start, datetime(2024, 1, 4))
        # window_end = date + 1 day = Jan 16
        self.assertEqual(proc.window_end, datetime(2024, 1, 16))


class TestGetFiles(unittest.TestCase):
    """Tests for XoverProcessor.get_files."""

    def setUp(self):
        self.proc = XoverProcessor("GSFC", datetime(2024, 1, 15))

    @patch("bad_passes.bad_pass_flag.aws_manager")
    def test_get_files_filters_by_existence(self, mock_aws):
        # Window is Jan 4 – Jan 16 = 13 days. Only say 2 exist.
        mock_aws.key_exists.side_effect = lambda path: "2024-01-10" in path or "2024-01-15" in path

        files = self.proc.get_files("test-bucket")

        self.assertEqual(len(files), 2)
        self.assertTrue(all("test-bucket" in f for f in files))

    @patch("bad_passes.bad_pass_flag.aws_manager")
    def test_get_files_correct_path_format(self, mock_aws):
        mock_aws.key_exists.return_value = True

        files = self.proc.get_files("my-bucket")

        for f in files:
            self.assertTrue(f.startswith("s3://my-bucket/crossovers/p2/GSFC/2024/xovers_GSFC-"))
            self.assertTrue(f.endswith(".nc"))

        # Should have one file per day in the window (inclusive)
        # Jan 4 through Jan 16 = 13 days
        self.assertEqual(len(files), 13)


class TestIdentifyBadPasses(unittest.TestCase):
    """Tests for XoverProcessor.identify_bad_passes with synthetic data."""

    def _make_processor(self):
        return XoverProcessor("GSFC", datetime(2024, 1, 15))

    def _set_data(self, proc, dssh, psec, trackid):
        proc.dssh = np.array(dssh, dtype=float)
        proc.psec = np.array(psec, dtype=float)
        proc.trackid = np.array(trackid, dtype=float)

    def test_flags_high_mean(self):
        proc = self._make_processor()
        currentdate = datetime(2024, 1, 15).timestamp()
        n = 20  # > nmean=15
        # All points in the time window, same track, large constant SSH diff
        psec = np.full(n, currentdate + 100)
        trackid = np.full(n, 50042.0)  # cycle=5, pass=42
        dssh = np.full(n, 0.5)  # >> max_mean=0.1

        self._set_data(proc, dssh, psec, trackid)
        result = proc.identify_bad_passes(currentdate)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["cycle"], "5")
        self.assertEqual(result[0]["pass_num"], "42")

    def test_flags_high_rms(self):
        proc = self._make_processor()
        currentdate = datetime(2024, 1, 15).timestamp()
        n = 30  # > nrms=25
        psec = np.full(n, currentdate + 100)
        trackid = np.full(n, 20010.0)  # cycle=2, pass=10
        # Mean near zero but high std dev (alternating large values)
        dssh = np.array([0.5 if i % 2 == 0 else -0.5 for i in range(n)])

        self._set_data(proc, dssh, psec, trackid)
        result = proc.identify_bad_passes(currentdate)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["cycle"], "2")
        self.assertEqual(result[0]["pass_num"], "10")

    def test_clean_data(self):
        proc = self._make_processor()
        currentdate = datetime(2024, 1, 15).timestamp()
        n = 30
        psec = np.full(n, currentdate + 100)
        trackid = np.full(n, 10005.0)
        dssh = np.full(n, 0.01)  # Small SSH diff, well below thresholds

        self._set_data(proc, dssh, psec, trackid)
        result = proc.identify_bad_passes(currentdate)

        self.assertEqual(len(result), 0)

    def test_insufficient_points(self):
        proc = self._make_processor()
        currentdate = datetime(2024, 1, 15).timestamp()
        n = 10  # < min(nmean=15, nrms=25) = 15
        psec = np.full(n, currentdate + 100)
        trackid = np.full(n, 50042.0)
        dssh = np.full(n, 5.0)  # Huge diff, but too few points

        self._set_data(proc, dssh, psec, trackid)
        result = proc.identify_bad_passes(currentdate)

        self.assertEqual(len(result), 0)


class TestWriteResultsToS3(unittest.TestCase):
    """Tests for XoverProcessor.write_results_to_s3."""

    @patch("bad_passes.bad_pass_flag.os.remove")
    @patch("bad_passes.bad_pass_flag.aws_manager")
    def test_writes_json_and_uploads(self, mock_aws, mock_remove):
        results = {
            "date": "2024-01-15",
            "source": "GSFC",
            "bad_passes": [{"cycle": "5", "pass_num": "100"}],
        }

        proc = XoverProcessor("GSFC", datetime(2024, 1, 15))
        m = mock_open()
        with patch("builtins.open", m):
            proc.write_results_to_s3(results, "my-bucket")

        expected_local = "/tmp/GSFC_2024-01-15_bad_passes.json"
        expected_s3 = "s3://my-bucket/aux_files/bad_passes/GSFC/2024-01-15.json"

        m.assert_called_once_with(expected_local, "w")
        handle = m()
        written_data = "".join(call.args[0] for call in handle.write.call_args_list)
        self.assertEqual(json.loads(written_data), results)

        mock_aws.upload_obj.assert_called_once_with(expected_local, expected_s3)
        mock_remove.assert_called_once_with(expected_local)


class TestProcess(unittest.TestCase):
    """Tests for XoverProcessor.process end-to-end."""

    @patch.object(XoverProcessor, "write_results_to_s3")
    @patch.object(XoverProcessor, "load_all_data")
    @patch.object(XoverProcessor, "get_files")
    @patch.object(XoverProcessor, "identify_bad_passes")
    def test_process_returns_formatted_dict(self, mock_identify, mock_get_files, mock_load, mock_write):
        mock_get_files.return_value = ["s3://bucket/file1.nc", "s3://bucket/file2.nc"]
        mock_identify.return_value = [{"cycle": "3", "pass_num": "77"}]

        proc = XoverProcessor("S6", datetime(2024, 6, 1))
        result = proc.process("test-bucket")

        mock_get_files.assert_called_once_with("test-bucket")
        mock_load.assert_called_once_with(["s3://bucket/file1.nc", "s3://bucket/file2.nc"])
        mock_identify.assert_called_once()
        mock_write.assert_called_once_with(result, "test-bucket")

        self.assertEqual(result["date"], "2024-06-01")
        self.assertEqual(result["source"], "S6")
        self.assertEqual(result["bad_passes"], [{"cycle": "3", "pass_num": "77"}])


if __name__ == "__main__":
    unittest.main()
