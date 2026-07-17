"""Tests for the indicators Lambda handler (app.py).

Covers reading the jobs manifest from S3, the empty-manifest early return,
dispatch to IndicatorProcessor, and the error envelope. The module-level S3
client and the processor are mocked; build_sg_key is covered in
test_indicator.py.
"""
import json
import unittest
from unittest.mock import MagicMock, patch

from app import handler

from utilities.errors import PipelineError


def _s3_body(payload):
    body = MagicMock()
    body.read.return_value = json.dumps(payload).encode()
    return {"Body": body}


class TestIndicatorsHandler(unittest.TestCase):

    @patch("app.IndicatorProcessor")
    @patch("app.s3")
    def test_handler_processes_jobs(self, mock_s3, mock_proc):
        mock_s3.get_object.return_value = _s3_body(
            [{"date": "2024-03-15"}, {"date": "2024-03-22"}]
        )
        event = {"bucket": "my-bucket", "jobs_key": "jobs.json", "source": "NASA-SSH"}

        result = handler(event, None)

        self.assertEqual(result, {"status": "success"})
        mock_s3.get_object.assert_called_once_with(Bucket="my-bucket", Key="jobs.json")
        # Two dates -> two simple-grid keys handed to the processor.
        args, _ = mock_proc.call_args
        self.assertEqual(len(args[0]), 2)
        self.assertEqual(args[1], "NASA-SSH")
        mock_proc.return_value.run.assert_called_once_with("my-bucket")

    @patch("app.IndicatorProcessor")
    @patch("app.s3")
    def test_empty_manifest_returns_early(self, mock_s3, mock_proc):
        mock_s3.get_object.return_value = _s3_body([])
        event = {"bucket": "my-bucket", "jobs_key": "jobs.json", "source": "NASA-SSH"}

        result = handler(event, None)

        self.assertEqual(result, {"status": "success"})
        mock_proc.assert_not_called()

    @patch("app.IndicatorProcessor")
    @patch("app.s3")
    def test_processor_failure_wrapped_in_pipeline_error(self, mock_s3, mock_proc):
        mock_s3.get_object.return_value = _s3_body([{"date": "2024-03-15"}])
        mock_proc.return_value.run.side_effect = RuntimeError("boom")
        event = {"bucket": "my-bucket", "jobs_key": "jobs.json", "source": "NASA-SSH"}

        with self.assertRaises(PipelineError) as ctx:
            handler(event, None)

        payload = json.loads(str(ctx.exception))
        self.assertEqual(payload["errorType"], "RuntimeError")
        self.assertEqual(payload["errorMessage"], "boom")
        self.assertEqual(payload["input"], event)


if __name__ == "__main__":
    unittest.main()
