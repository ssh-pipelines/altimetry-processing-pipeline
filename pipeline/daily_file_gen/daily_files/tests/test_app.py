"""Tests for the daily_files Lambda handler (app.py).

Covers param validation, source-configuration checking, dispatch, and the
error envelope. daily_file_job.start_job is mocked; the processing itself is
exercised in test_daily_file_job.py.
"""
import json
import unittest
from unittest.mock import patch

from app import handler

from utilities.errors import PipelineError


class TestDailyFilesHandler(unittest.TestCase):

    @patch("app.daily_file_job")
    def test_handler_starts_job_and_returns_success(self, mock_job):
        event = {
            "bucket": "my-bucket",
            "date": "2025-02-10",
            "source": "S6",
            "granules": ["g1", "g2"],
        }
        result = handler(event, None)

        self.assertEqual(result, {"status": "success", "data": event})
        mock_job.start_job.assert_called_once_with("2025-02-10", "S6", "my-bucket", ["g1", "g2"])

    def test_missing_source_raises_runtimeerror(self):
        event = {"bucket": "my-bucket", "date": "2025-02-10", "granules": []}
        with self.assertRaises(RuntimeError):
            handler(event, None)

    def test_missing_granules_raises_runtimeerror(self):
        event = {"bucket": "my-bucket", "date": "2025-02-10", "source": "S6"}
        with self.assertRaises(RuntimeError):
            handler(event, None)

    def test_unconfigured_source_raises_runtimeerror(self):
        event = {
            "bucket": "my-bucket",
            "date": "2025-02-10",
            "source": "NONEXISTENT",
            "granules": [],
        }
        with self.assertRaises(RuntimeError) as ctx:
            handler(event, None)
        self.assertIn("not configured", str(ctx.exception))

    @patch("app.daily_file_job")
    def test_job_failure_wrapped_in_pipeline_error(self, mock_job):
        mock_job.start_job.side_effect = RuntimeError("boom")
        event = {
            "bucket": "my-bucket",
            "date": "2025-02-10",
            "source": "S6",
            "granules": [],
        }

        with self.assertRaises(PipelineError) as ctx:
            handler(event, None)

        payload = json.loads(str(ctx.exception))
        self.assertEqual(payload["errorType"], "RuntimeError")
        self.assertEqual(payload["errorMessage"], "boom")
        self.assertEqual(payload["input"], event)


if __name__ == "__main__":
    unittest.main()
