"""Tests for the bad_pass Lambda handler (app.py).

Covers the validate -> dispatch -> return / error-envelope paths. The
XoverProcessor is mocked; these tests pin the handler's contract, not the
flagging math (see test_bad_pass_flag.py for that).
"""
import json
import unittest
from datetime import datetime
from unittest.mock import patch

from app import handler

from utilities.errors import PipelineError


class TestBadPassHandler(unittest.TestCase):

    @patch("app.XoverProcessor")
    def test_handler_returns_bad_pass_count(self, mock_proc):
        mock_proc.return_value.process.return_value = {"bad_passes": [1, 2, 3]}
        event = {"bucket": "my-bucket", "date": "2024-01-15", "source": "GSFC"}

        result = handler(event, None)

        self.assertEqual(result, {"date": "2024-01-15", "source": "GSFC", "count": 3})
        mock_proc.assert_called_once_with("GSFC", datetime(2024, 1, 15))
        mock_proc.return_value.process.assert_called_once_with("my-bucket")

    def test_missing_source_raises_valueerror(self):
        event = {"bucket": "my-bucket", "date": "2024-01-15"}
        with self.assertRaises(ValueError):
            handler(event, None)

    def test_missing_bucket_raises_valueerror(self):
        event = {"date": "2024-01-15", "source": "GSFC"}
        with self.assertRaises(ValueError):
            handler(event, None)

    @patch("app.XoverProcessor")
    def test_processor_failure_wrapped_in_pipeline_error(self, mock_proc):
        mock_proc.return_value.process.side_effect = RuntimeError("boom")
        event = {"bucket": "my-bucket", "date": "2024-01-15", "source": "GSFC"}

        with self.assertRaises(PipelineError) as ctx:
            handler(event, None)

        payload = json.loads(str(ctx.exception))
        self.assertEqual(payload["errorType"], "RuntimeError")
        self.assertEqual(payload["errorMessage"], "boom")
        self.assertEqual(payload["input"], event)


if __name__ == "__main__":
    unittest.main()
