"""The seam between the two tiers: the along-track manifest is keyed on a
Source, the gridded manifest on the published product that Source feeds.
"""
import json
import os
import unittest

os.environ.setdefault("AWS_REGION", "us-west-2")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")

import app  # noqa: E402


class _Body:
    def __init__(self, data: bytes):
        self._data = data

    def read(self):
        return self._data


class FakeS3:
    def __init__(self, objects=None):
        self.objects = dict(objects or {})

    def get_object(self, Bucket, Key):
        return {"Body": _Body(self.objects[Key])}

    def put_object(self, **kwargs):
        body = kwargs["Body"]
        self.objects[kwargs["Key"]] = body.encode() if isinstance(body, str) else body


JOBS_KEY = "pipeline_runs/S6/20260624T201021/jobs.json"


class TestRewriteManifest(unittest.TestCase):
    def setUp(self):
        self._real_s3 = app.s3

    def tearDown(self):
        app.s3 = self._real_s3

    def _run(self, jobs, published_product="NASA-SSH"):
        app.s3 = FakeS3({JOBS_KEY: json.dumps(jobs).encode()})
        result = app.lambda_handler(
            {
                "bucket": "b",
                "jobs_key": JOBS_KEY,
                "published_product": published_product,
            },
            None,
        )
        return result, json.loads(app.s3.objects[result["jobs_key"]])

    def test_nests_the_gridded_manifest_under_the_published_product(self):
        """The two manifests of one run must not collide, and the run summary
        reads the published product back out of this path segment."""
        result, _ = self._run([{"date": "2026-02-09", "source": "S6", "bucket": "b"}])
        self.assertEqual(
            result["jobs_key"],
            "pipeline_runs/S6/20260624T201021/NASA-SSH/jobs.json",
        )
        self.assertEqual(result["published_product"], "NASA-SSH")

    def test_rekeys_each_job_off_the_source(self):
        """A gridded job is not per-source: several sources feed one line, and
        leaving a stale `source` behind invites a stage to key on the wrong thing."""
        _result, written = self._run(
            [
                {"date": "2026-02-09", "source": "S6", "bucket": "b"},
                {"date": "2026-02-10", "source": "S6", "bucket": "b"},
            ]
        )
        for job in written:
            self.assertEqual(job["published_product"], "NASA-SSH")
            self.assertNotIn("source", job)

    def test_preserves_the_dates(self):
        _result, written = self._run(
            [
                {"date": "2026-02-09", "source": "S6", "bucket": "b"},
                {"date": "2026-02-10", "source": "S6", "bucket": "b"},
            ]
        )
        self.assertEqual([j["date"] for j in written], ["2026-02-09", "2026-02-10"])

    def test_empty_manifest_round_trips(self):
        result, written = self._run([])
        self.assertEqual(written, [])
        self.assertTrue(result["jobs_key"].endswith("/NASA-SSH/jobs.json"))

    def test_high_latitude_line_gets_its_own_segment(self):
        result, written = self._run(
            [{"date": "2025-06-04", "source": "S3B", "bucket": "b"}],
            published_product="NASA-SSH_S3B",
        )
        self.assertEqual(
            result["jobs_key"],
            "pipeline_runs/S6/20260624T201021/NASA-SSH_S3B/jobs.json",
        )
        self.assertEqual(written[0]["published_product"], "NASA-SSH_S3B")


if __name__ == "__main__":
    unittest.main()
