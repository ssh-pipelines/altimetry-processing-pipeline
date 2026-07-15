import json
import os
import unittest
from datetime import date

os.environ.setdefault("AWS_REGION", "us-west-2")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")

import app  # noqa: E402

# ---------------------------------------------------------------------------
# Fake S3 client (minimal: get_object + put_object)
# ---------------------------------------------------------------------------

class _Body:
    def __init__(self, data: bytes):
        self._data = data

    def read(self):
        return self._data


class FakeS3:
    def __init__(self, objects: dict[str, bytes] | None = None):
        self.objects = dict(objects or {})
        self.put_calls = []

    def get_object(self, Bucket, Key):
        return {"Body": _Body(self.objects[Key])}

    def put_object(self, **kwargs):
        self.put_calls.append(kwargs)
        self.objects[kwargs["Key"]] = kwargs["Body"].encode() if isinstance(
            kwargs["Body"], str) else kwargs["Body"]


def _event(jobs, jobs_key="pipeline_runs/S6/20260624T201021/NASA-SSH/jobs.json"):
    return {
        "bucket": "nasa-ssh-staging-dev",
        "jobs_key": jobs_key,
        "source": "NASA-SSH",
    }, {jobs_key: json.dumps(jobs).encode()}


class TestSetSgJobs(unittest.TestCase):
    def setUp(self):
        self._real_s3 = app.s3
        self._real_last = app.last_sg_date

    def tearDown(self):
        app.s3 = self._real_s3
        app.last_sg_date = self._real_last

    def _run(self, jobs):
        event, objects = _event(jobs)
        app.s3 = FakeS3(objects)
        result = app.lambda_handler(event, None)
        written = json.loads(app.s3.objects[result["jobs_key"]])
        return result, written

    def test_empty_manifest_writes_empty_sg_jobs(self):
        """No new files to process: an empty manifest must not raise (regression
        for min()/max() over an empty iterable) and yields an empty sg_jobs.json."""
        result, written = self._run([])

        self.assertEqual(result["jobs_key"],
                         "pipeline_runs/S6/20260624T201021/NASA-SSH/sg_jobs.json")
        self.assertEqual(result["source"], "NASA-SSH")
        self.assertEqual(written, [])

    def test_nonempty_manifest_emits_mondays(self):
        # Pin "latest available Monday" so the overlap filter is deterministic.
        app.last_sg_date = lambda: date(2026, 2, 16)
        _result, written = self._run([
            {"date": "2026-02-09", "bucket": "b", "source": "NASA-SSH"},
            {"date": "2026-02-16", "bucket": "b", "source": "NASA-SSH"},
        ])

        dates = sorted(j["date"] for j in written)
        self.assertEqual(dates, ["2026-02-09", "2026-02-16"])
        for j in written:
            self.assertEqual(j["source"], "NASA-SSH")


if __name__ == "__main__":
    unittest.main()
