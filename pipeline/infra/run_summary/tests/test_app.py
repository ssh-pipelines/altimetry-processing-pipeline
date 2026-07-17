"""Tests for the run_summary Lambda handler (app.py).

The handler orchestrates summarizer.gather/build_summary + S3 write + SNS
publish, and (per ADR 0005) must never raise — a summarizer fault cannot fail
an otherwise-successful run. These tests mock the summarizer and the
module-level S3/SNS clients; summarizer's own logic is covered in
test_run_summary.py.

app reads SNS_TOPIC_ARN and constructs boto3 clients at module load, so the
env (topic ARN + region) must be set before the import — CI has no ambient AWS
region. Mirrors the convention in test_failure_handling.py / test_set_sg_jobs.py.
"""
import os
import unittest
from unittest.mock import patch

os.environ.setdefault("SNS_TOPIC_ARN", "arn:aws:sns:us-west-2:123456789012:test-topic")
os.environ.setdefault("AWS_REGION", "us-west-2")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")

from app import handler  # noqa: E402


def _summary():
    return {"source": "S6", "run_id": "20250528T120000"}


class TestRunSummaryHandler(unittest.TestCase):

    def test_missing_jobs_key_is_skipped(self):
        result = handler({"bucket": "my-bucket"}, None)
        self.assertEqual(result["status"], "skipped")

    def test_missing_bucket_is_skipped(self):
        result = handler({"jobs_key": "jobs.json"}, None)
        self.assertEqual(result["status"], "skipped")

    @patch("app.sns")
    @patch("app.s3")
    @patch("app.summarizer")
    def test_happy_path_writes_and_publishes(self, mock_sum, mock_s3, mock_sns):
        mock_sum.gather.return_value = ({}, {}, {})
        mock_sum.build_summary.return_value = _summary()
        mock_sum.summary_key.return_value = "runs/summary.json"
        mock_sum.render_notification.return_value = ("subject", "message")

        result = handler({"jobs_key": "jobs.json", "bucket": "my-bucket"}, None)

        self.assertEqual(result, {"status": "summarized", "source": "S6", "run_id": "20250528T120000"})
        mock_s3.put_object.assert_called_once()
        self.assertEqual(mock_s3.put_object.call_args.kwargs["Key"], "runs/summary.json")
        mock_sns.publish.assert_called_once()

    @patch("app.sns")
    @patch("app.s3")
    @patch("app.summarizer")
    def test_s3_and_sns_failures_are_swallowed(self, mock_sum, mock_s3, mock_sns):
        mock_sum.gather.return_value = ({}, {}, {})
        mock_sum.build_summary.return_value = _summary()
        mock_sum.summary_key.return_value = "runs/summary.json"
        mock_sum.render_notification.return_value = ("subject", "message")
        mock_s3.put_object.side_effect = RuntimeError("s3 down")
        mock_sns.publish.side_effect = RuntimeError("sns down")

        # Must still return a summarized result despite both I/O failures.
        result = handler({"jobs_key": "jobs.json", "bucket": "my-bucket"}, None)
        self.assertEqual(result["status"], "summarized")


if __name__ == "__main__":
    unittest.main()
