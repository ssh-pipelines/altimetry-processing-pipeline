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

EVENT = {
    "bucket": "my-bucket",
    "published_product": "NASA-SSH",
    "run_id": "20250528T120000",
    "contributors": ["GSFC", "S6"],
}


def _summary():
    return {"published_product": "NASA-SSH", "run_id": "20250528T120000"}


def _stub(mock_sum):
    mock_sum.gather.return_value = ({}, {}, {}, [])
    mock_sum.build_summary.return_value = _summary()
    mock_sum.summary_key.return_value = "runs/summary.json"
    mock_sum.render_notification.return_value = ("subject", "message")


class TestRunIdentityValidation(unittest.TestCase):
    """The run envelope, not a jobs_key. A `jobs_key`-only event is the
    pre-Phase-4 shape and must not be silently reconciled as some other run."""

    def test_missing_published_product_is_skipped(self):
        result = handler({"bucket": "my-bucket", "run_id": "R1"}, None)
        self.assertEqual(result["status"], "skipped")

    def test_missing_run_id_is_skipped(self):
        result = handler({"bucket": "my-bucket", "published_product": "NASA-SSH"}, None)
        self.assertEqual(result["status"], "skipped")

    def test_missing_bucket_is_skipped(self):
        result = handler({"published_product": "NASA-SSH", "run_id": "R1"}, None)
        self.assertEqual(result["status"], "skipped")

    def test_legacy_jobs_key_event_is_skipped(self):
        result = handler({"bucket": "my-bucket", "jobs_key": "jobs.json"}, None)
        self.assertEqual(result["status"], "skipped")


class TestRunSummaryHandler(unittest.TestCase):

    @patch("app.sns")
    @patch("app.s3")
    @patch("app.summarizer")
    def test_happy_path_writes_and_publishes(self, mock_sum, mock_s3, mock_sns):
        _stub(mock_sum)
        result = handler(EVENT, None)

        self.assertEqual(
            result,
            {
                "status": "summarized",
                "published_product": "NASA-SSH",
                "run_id": "20250528T120000",
            },
        )
        mock_s3.put_object.assert_called_once()
        self.assertEqual(mock_s3.put_object.call_args.kwargs["Key"], "runs/summary.json")
        mock_sns.publish.assert_called_once()

    @patch("app.sns")
    @patch("app.s3")
    @patch("app.summarizer")
    def test_contributors_are_threaded_to_the_summarizer(self, mock_sum, _s3, _sns):
        """The source tier is read per contributor; dropping the list here would
        reconcile the along-track section against nothing and report every date
        as missing on a clean run."""
        _stub(mock_sum)
        handler(EVENT, None)
        self.assertEqual(
            mock_sum.gather.call_args.args[2:],
            ("NASA-SSH", "20250528T120000", ["GSFC", "S6"]),
        )

    @patch("app.sns")
    @patch("app.s3")
    @patch("app.summarizer")
    def test_absent_contributors_is_an_empty_source_tier(self, mock_sum, _s3, _sns):
        """A product-tier-only run (the deferred composite grid) has none. That
        reconciles the gridded section against an empty source tier, which is
        correct rather than degraded."""
        _stub(mock_sum)
        result = handler({k: v for k, v in EVENT.items() if k != "contributors"}, None)
        self.assertEqual(result["status"], "summarized")
        self.assertEqual(mock_sum.gather.call_args.args[4], [])

    @patch("app.sns")
    @patch("app.s3")
    @patch("app.summarizer")
    def test_s3_and_sns_failures_are_swallowed(self, mock_sum, mock_s3, mock_sns):
        _stub(mock_sum)
        mock_s3.put_object.side_effect = RuntimeError("s3 down")
        mock_sns.publish.side_effect = RuntimeError("sns down")

        # Must still return a summarized result despite both I/O failures.
        result = handler(EVENT, None)
        self.assertEqual(result["status"], "summarized")


if __name__ == "__main__":
    unittest.main()
