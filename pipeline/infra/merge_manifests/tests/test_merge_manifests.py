"""The tier seam: N Source-keyed manifests in, one Published-product-keyed
manifest out, and a contested date fails rather than resolving by luck.
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
        if Key not in self.objects:
            raise KeyError(Key)  # stands in for a ClientError NoSuchKey
        return {"Body": _Body(self.objects[Key])}

    def put_object(self, **kwargs):
        body = kwargs["Body"]
        self.objects[kwargs["Key"]] = body.encode() if isinstance(body, str) else body


RUN_ID = "20260624T201021"
PRODUCT_KEY = f"pipeline_runs/NASA-SSH/{RUN_ID}/jobs.json"


def _spec(date, source):
    return {"date": date, "source": source, "bucket": "b", "granules": []}


class _MergeCase(unittest.TestCase):
    def setUp(self):
        self._real_s3 = app.s3

    def tearDown(self):
        app.s3 = self._real_s3

    def run_handler(self, per_source, published_product="NASA-SSH", product_key=PRODUCT_KEY):
        """`per_source` maps a source name to its job specs."""
        objects = {}
        source_results = []
        for source, jobs in per_source.items():
            jobs_key = f"pipeline_runs/{source}/{RUN_ID}/jobs.json"
            objects[jobs_key] = json.dumps(jobs).encode()
            source_results.append({"source": source, "jobs_key": jobs_key})
        app.s3 = FakeS3(objects)
        result = app.lambda_handler(
            {
                "bucket": "b",
                "published_product": published_product,
                "product_jobs_key": product_key,
                "source_results": source_results,
            },
            None,
        )
        return result, json.loads(app.s3.objects[result["jobs_key"]])


class TestProductTierManifest(_MergeCase):
    def test_writes_at_the_product_run_path(self):
        """A sibling of its contributors' paths, not a child of one of them —
        which is what lets the ResultWriter prefix and the Run summary address
        the product tier by the product."""
        result, _ = self.run_handler({"S6": [_spec("2026-02-09", "S6")]})
        self.assertEqual(result["jobs_key"], PRODUCT_KEY)
        self.assertEqual(result["published_product"], "NASA-SSH")

    def test_rekeys_each_job_off_the_source(self):
        """A gridded job is not per-source: several Sources feed one line, and a
        stale `source` invites a stage to key on the wrong thing."""
        _result, written = self.run_handler(
            {"S6": [_spec("2026-02-09", "S6"), _spec("2026-02-10", "S6")]}
        )
        for job in written:
            self.assertEqual(job["published_product"], "NASA-SSH")
            self.assertEqual(job["bucket"], "b")
            self.assertNotIn("source", job)

    def test_unions_dates_across_contributors_in_order(self):
        """GSFC ends 2025-12-31 and S6 starts 2026-01-01, so a run spanning the
        handover sees both contributors with disjoint dates."""
        result, written = self.run_handler(
            {
                "S6": [_spec("2026-01-02", "S6"), _spec("2026-01-01", "S6")],
                "GSFC": [_spec("2025-12-31", "GSFC"), _spec("2025-12-30", "GSFC")],
            }
        )
        self.assertEqual(
            [j["date"] for j in written],
            ["2025-12-30", "2025-12-31", "2026-01-01", "2026-01-02"],
        )
        self.assertEqual(result["date_count"], 4)

    def test_granules_are_not_carried_through(self):
        """The gridded tier reads published along-track files, never upstream
        granules; carrying the URI lists would bloat the manifest for nothing."""
        _result, written = self.run_handler(
            {"S6": [{"date": "2026-02-09", "source": "S6", "bucket": "b",
                     "granules": ["s3://x/a.nc", "s3://x/b.nc"]}]}
        )
        self.assertNotIn("granules", written[0])

    def test_empty_manifests_produce_an_empty_product_manifest(self):
        """A week where nothing was planned. The parent skips the gridded tier on
        `date_count == 0`, but the manifest is still written so the Run summary
        reconciles against an explicit zero rather than a missing object."""
        result, written = self.run_handler({"S6": [], "GSFC": []})
        self.assertEqual(written, [])
        self.assertEqual(result["date_count"], 0)

    def test_no_contributors_at_all(self):
        result, written = self.run_handler({})
        self.assertEqual(written, [])
        self.assertEqual(result["date_count"], 0)

    def test_high_latitude_line_gets_its_own_path(self):
        result, written = self.run_handler(
            {"S3B": [_spec("2025-06-04", "S3B")]},
            published_product="NASA-SSH_S3B",
            product_key=f"pipeline_runs/NASA-SSH_S3B/{RUN_ID}/jobs.json",
        )
        self.assertEqual(
            result["jobs_key"], f"pipeline_runs/NASA-SSH_S3B/{RUN_ID}/jobs.json"
        )
        self.assertEqual(written[0]["published_product"], "NASA-SSH_S3B")


class TestContestedDates(_MergeCase):
    def test_two_contributors_claiming_one_date_fails(self):
        """S6B overlaps GSFC by five weeks. Under the derived-identity rule both
        resolve to the same published key, so without this the winner is decided
        by Map completion order — under a PO.DAAC-facing filename."""
        with self.assertRaises(ValueError) as cm:
            self.run_handler(
                {
                    "GSFC": [_spec("2025-12-01", "GSFC")],
                    "S6B": [_spec("2025-12-01", "S6B")],
                }
            )
        message = str(cm.exception)
        self.assertIn("2025-12-01", message)
        self.assertIn("GSFC", message)
        self.assertIn("S6B", message)

    def test_nothing_is_written_on_contention(self):
        """The failure must precede the write, or a partial product manifest is
        left behind for a hand-rerun to read."""
        with self.assertRaises(ValueError):
            self.run_handler(
                {
                    "GSFC": [_spec("2025-12-01", "GSFC")],
                    "S6B": [_spec("2025-12-01", "S6B")],
                }
            )
        self.assertNotIn(PRODUCT_KEY, app.s3.objects)

    def test_a_date_repeated_within_one_contributor_is_not_contention(self):
        """pipeline_init plans one job per date, but a hand-written manifest can
        repeat one. Same source twice is a duplicate, not a contest."""
        _result, written = self.run_handler(
            {"S6": [_spec("2026-02-09", "S6"), _spec("2026-02-09", "S6")]}
        )
        self.assertEqual([j["date"] for j in written], ["2026-02-09"])

    def test_disjoint_contributors_do_not_trip_the_guard(self):
        _result, written = self.run_handler(
            {
                "GSFC": [_spec("2025-12-31", "GSFC")],
                "S6": [_spec("2026-01-01", "S6")],
            }
        )
        self.assertEqual(len(written), 2)


class TestMalformedInput(_MergeCase):
    def test_a_missing_contributor_manifest_fails(self):
        """Not an empty contribution: every contributor in the fan-out ran
        pipeline_init, which writes a manifest even when it plans nothing. An
        absent one means the source tier did not get that far, and reading it as
        'no dates' would grid a window with a contributor silently missing."""
        app.s3 = FakeS3()
        with self.assertRaises(KeyError):
            app.lambda_handler(
                {
                    "bucket": "b",
                    "published_product": "NASA-SSH",
                    "product_jobs_key": PRODUCT_KEY,
                    "source_results": [
                        {"source": "S6", "jobs_key": "pipeline_runs/S6/R/jobs.json"}
                    ],
                },
                None,
            )

    def test_a_non_list_manifest_fails(self):
        app.s3 = FakeS3({"pipeline_runs/S6/R/jobs.json": b'{"date": "2026-02-09"}'})
        with self.assertRaises(ValueError):
            app.lambda_handler(
                {
                    "bucket": "b",
                    "published_product": "NASA-SSH",
                    "product_jobs_key": PRODUCT_KEY,
                    "source_results": [
                        {"source": "S6", "jobs_key": "pipeline_runs/S6/R/jobs.json"}
                    ],
                },
                None,
            )

    def test_job_specs_without_a_date_are_skipped(self):
        _result, written = self.run_handler(
            {"S6": [{"source": "S6", "bucket": "b"}, _spec("2026-02-09", "S6")]}
        )
        self.assertEqual([j["date"] for j in written], ["2026-02-09"])


if __name__ == "__main__":
    unittest.main()
