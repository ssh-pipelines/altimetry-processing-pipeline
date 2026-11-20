"""Tests for the delivery_audit handler: event validation, and that every outcome
sends an email before any error is raised.

app reads SNS_TOPIC_ARN and constructs boto3 clients at module load, so the env
must be set before the import. The module-level S3 and SNS clients are patched.
"""
import os
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

os.environ.setdefault("SNS_TOPIC_ARN", "arn:aws:sns:us-west-2:123456789012:test-topic")
os.environ.setdefault("AWS_REGION", "us-west-2")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")

import app  # noqa: E402

from tests.fakes import AT, OLD, FakeS3, FakeSession, reference_cmr, reference_keys  # noqa: E402
from utilities.schedules import list_stage_schedules  # noqa: E402

NOW = datetime(2026, 11, 19, 12, tzinfo=timezone.utc)
EVENT = {"grace_hours": 192, "bucket": "b"}


class ParseEventTestCase(unittest.TestCase):
    def test_minimal_event(self):
        p = app.parse_event(EVENT)
        self.assertEqual(p["grace"].total_seconds(), 192 * 3600)
        self.assertIsNone(p["published_products"])
        self.assertEqual(p["bucket"], "b")

    def test_grace_is_required(self):
        with self.assertRaisesRegex(ValueError, "grace_hours is required"):
            app.parse_event({"bucket": "b"})

    def test_bad_grace_is_refused(self):
        for bad in (-1, "12", True, None):
            with self.subTest(grace_hours=bad), self.assertRaises(ValueError):
                app.parse_event({**EVENT, "grace_hours": bad})

    def test_unknown_key_is_refused(self):
        """A typo in the schedule input must not silently take a default."""
        with self.assertRaisesRegex(ValueError, "grace_hour"):
            app.parse_event({**EVENT, "grace_hour": 12})

    def test_redeliver_is_refused_until_the_delivery_lambda_exists(self):
        with self.assertRaisesRegex(ValueError, "redeliver"):
            app.parse_event({**EVENT, "redeliver": True})
        self.assertIsNotNone(app.parse_event({**EVENT, "redeliver": False}))

    def test_published_products_must_be_a_non_empty_list(self):
        for bad in ("NASA-SSH", []):
            with self.subTest(published_products=bad), self.assertRaises(ValueError):
                app.parse_event({**EVENT, "published_products": bad})

    def test_bucket_falls_back_to_env(self):
        with patch.dict(os.environ, {"BUCKET_NAME": "env-bucket"}):
            self.assertEqual(app.parse_event({"grace_hours": 1})["bucket"], "env-bucket")
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("BUCKET_NAME", None)
            with self.assertRaisesRegex(ValueError, "bucket"):
                app.parse_event({"grace_hours": 1})

    def test_mirrored_schedule_input_is_accepted(self):
        """utilities/schedules.yaml mirrors the AWS schedule's input; it must be
        an event this handler takes."""
        for s in list_stage_schedules():
            if s.target == "delivery_audit":
                with self.subTest(schedule=s.name):
                    app.parse_event({**s.input, "bucket": "b"})


class HandlerTestCase(unittest.TestCase):
    def setUp(self):
        self.sns = MagicMock()
        patcher_sns = patch.object(app, "sns", self.sns)
        patcher_sns.start()
        self.addCleanup(patcher_sns.stop)

    def run_handler(self, event, keys, cmr, **session_kw):
        with patch.object(app, "s3", FakeS3(keys)):
            return app.handler(event, None, http_session=FakeSession(cmr, **session_kw), now=NOW)

    def published(self):
        self.assertEqual(self.sns.publish.call_count, 1)
        kw = self.sns.publish.call_args.kwargs
        return kw["Subject"], kw["Message"]

    def test_clean_audit_emails_clean(self):
        result = self.run_handler(EVENT, reference_keys(), reference_cmr())
        self.assertEqual(result, {"status": "clean", "collections": 6, "problems": 0})
        subject, message = self.published()
        self.assertEqual(subject, "Delivery audit clean")
        self.assertIn("Audit clean", message)

    def test_problems_email_and_return_without_raising(self):
        """Problems are the audit doing its job, not a Lambda error."""
        keys = reference_keys()
        keys[f"products/NASA-SSH/along_track/2026/{AT}20261113.nc"] = (100, OLD)
        result = self.run_handler(EVENT, keys, reference_cmr())
        self.assertEqual(result["status"], "problems")
        subject, message = self.published()
        self.assertEqual(subject, "Delivery audit: 1 problems")
        self.assertIn(f"missing: {AT}20261113", message)

    def test_collection_failure_emails_then_raises(self):
        with self.assertRaisesRegex(app.AuditIncomplete, "NASA_SSH_ENSO_INDICATOR"):
            self.run_handler(EVENT, reference_keys(), reference_cmr(), status={"NASA_SSH_ENSO_INDICATOR": 500})
        subject, message = self.published()
        self.assertTrue(subject.startswith("Delivery audit incomplete"))
        self.assertIn("NASA_SSH_ENSO_INDICATOR: could not audit", message)
        self.assertIn("NASA_SSH_REF_ALONGTRACK_V11: released 1", message)

    def test_bad_event_emails_could_not_run_then_raises(self):
        with self.assertRaises(ValueError):
            self.run_handler({"bucket": "b"}, reference_keys(), reference_cmr())
        subject, message = self.published()
        self.assertEqual(subject, "Delivery audit could not run")
        self.assertIn("grace_hours is required", message)

    def test_undelivered_line_emails_could_not_run_then_raises(self):
        with self.assertRaisesRegex(ValueError, "NASA-SSH_S3B"):
            self.run_handler({**EVENT, "published_products": ["NASA-SSH_S3B"]}, reference_keys(), reference_cmr())
        subject, _ = self.published()
        self.assertEqual(subject, "Delivery audit could not run")

    def test_subject_fits_sns_limit(self):
        """SNS rejects subjects over 100 characters, which would lose the email."""
        with patch.object(app.audit, "render_report", return_value=("S" * 150, "body")):
            self.run_handler(EVENT, reference_keys(), reference_cmr())
        subject, _ = self.published()
        self.assertEqual(len(subject), 100)


if __name__ == "__main__":
    unittest.main()
