"""Tests for the oer Lambda handler (app.py).

Covers the validate -> dispatch -> return / error-envelope paths. The
OerCorrection processor is mocked; these tests pin the handler's contract,
not the correction math (see test_oer.py for that).
"""
import json
import unittest
from unittest.mock import patch

from app import handler

from utilities.errors import PipelineError


class TestOerHandler(unittest.TestCase):

    @patch("app.OerCorrection")
    def test_handler_runs_and_returns_success(self, mock_oer):
        mock_oer.return_value.run.return_value = True
        event = {"bucket": "my-bucket", "date": "2025-02-10", "source": "S6"}
        result = handler(event, None)

        self.assertEqual(result, {"status": "success", "data": event})
        mock_oer.assert_called_once()
        args, _ = mock_oer.call_args
        self.assertEqual(args[0], "S6")
        self.assertEqual(args[2], "my-bucket")
        mock_oer.return_value.run.assert_called_once_with()

    @patch("app.OerCorrection")
    def test_handler_returns_skipped_when_run_skips(self, mock_oer):
        mock_oer.return_value.run.return_value = False
        event = {"bucket": "my-bucket", "date": "2020-01-09", "source": "S3B"}
        result = handler(event, None)

        self.assertEqual(result, {"status": "skipped", "data": event})

    def test_missing_source_raises_valueerror(self):
        event = {"bucket": "my-bucket", "date": "2025-02-10"}
        with self.assertRaises(ValueError):
            handler(event, None)

    def test_missing_bucket_raises_valueerror(self):
        event = {"date": "2025-02-10", "source": "S6"}
        with self.assertRaises(ValueError):
            handler(event, None)

    @patch("app.OerCorrection")
    def test_processor_failure_wrapped_in_pipeline_error(self, mock_oer):
        mock_oer.return_value.run.side_effect = RuntimeError("boom")
        event = {"bucket": "my-bucket", "date": "2025-02-10", "source": "S6"}

        with self.assertRaises(PipelineError) as ctx:
            handler(event, None)

        payload = json.loads(str(ctx.exception))
        self.assertEqual(payload["status"], "error")
        self.assertEqual(payload["errorType"], "RuntimeError")
        self.assertEqual(payload["errorMessage"], "boom")
        self.assertEqual(payload["input"], event)


if __name__ == "__main__":
    unittest.main()
