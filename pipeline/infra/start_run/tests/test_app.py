"""The fan-out decision, and the two ways it can be empty.

Tests run against the real Source registry rather than a stub: the point of this
Lambda is that it is the registry's one runtime reader, and a stubbed registry
would pin the wrapper without pinning the rule it wraps.
"""

import json
import unittest

import app


class FakeS3:
    def __init__(self):
        self.objects = {}

    def put_object(self, **kwargs):
        body = kwargs["Body"]
        self.objects[kwargs["Key"]] = body.encode() if isinstance(body, str) else body


class _StartRunCase(unittest.TestCase):
    def setUp(self):
        self._real_s3 = app.s3
        app.s3 = FakeS3()

    def tearDown(self):
        app.s3 = self._real_s3

    def run_handler(self, **event):
        event.setdefault("bucket", "b")
        return app.handler(event, None)


class TestReferenceLine(_StartRunCase):
    def test_fans_out_over_enabled_reference_sources(self):
        result = self.run_handler(published_product="NASA-SSH")
        self.assertEqual(result["contributors"], ["GSFC", "S6"])

    def test_excludes_registered_but_disabled_sources(self):
        """S6B and EXAMPLE_S3 are `reference` sources in the registry, so they
        resolve to NASA-SSH; neither is enabled. S6B overlaps GSFC by five weeks
        (constraint 3), so pulling it in by registry membership alone would
        contend for published dates."""
        result = self.run_handler(published_product="NASA-SSH")
        self.assertIn("S6B", app.registered_contributors("NASA-SSH"))
        self.assertNotIn("S6B", result["contributors"])
        self.assertNotIn("EXAMPLE_S3", result["contributors"])

    def test_declares_a_gridded_tier(self):
        result = self.run_handler(published_product="NASA-SSH")
        self.assertTrue(result["gridded"])

    def test_product_jobs_key_is_product_keyed(self):
        result = self.run_handler(published_product="NASA-SSH")
        self.assertEqual(
            result["product_jobs_key"],
            f"pipeline_runs/NASA-SSH/{result['run_id']}/jobs.json",
        )

    def test_run_id_is_a_utc_timestamp(self):
        result = self.run_handler(published_product="NASA-SSH")
        self.assertRegex(result["run_id"], r"^\d{8}T\d{6}$")


class TestHighLatitudeLine(_StartRunCase):
    """`products/NASA-SSH_S3B/` holds along-track only: there is at most one
    high-latitude *grid* product and it is composed from every satellite, so no
    single satellite's line has a product tier. `gridded: false` is what makes
    the per-satellite ENSO index unreachable rather than merely unused.

    S3B is `enabled: false` today, so the handler's own fan-out path for this
    line is covered in TestEmptyFanOut; the tier decision is exercised directly.
    """

    def test_no_gridded_tier(self):
        self.assertFalse(app.publishes_simple_grid("NASA-SSH_S3B"))

    def test_resolves_to_exactly_one_registered_satellite(self):
        """One contributor per line is what makes 15 satellites 15 schedules and
        zero new code."""
        self.assertEqual(app.registered_contributors("NASA-SSH_S3B"), ["S3B"])


class TestEmptyFanOut(_StartRunCase):
    def test_unknown_published_product_raises(self):
        with self.assertRaises(ValueError) as cm:
            self.run_handler(published_product="NASA-SSH-typo")
        self.assertIn("No registered source resolves", str(cm.exception))

    def test_registered_but_all_disabled_raises_distinctly(self):
        """A run over zero contributors succeeds having produced nothing, which
        is indistinguishable from a quiet week. The two empty cases get separate
        messages because one is a typo and the other is an `enabled` flag."""
        with self.assertRaises(ValueError) as cm:
            self.run_handler(published_product="NASA-SSH_S3B")
        self.assertIn("none are enabled", str(cm.exception))


class TestInputValidation(_StartRunCase):
    def test_missing_published_product_raises(self):
        with self.assertRaises(ValueError):
            self.run_handler()

    def test_source_keyed_trigger_is_not_silently_accepted(self):
        """The pre-Phase-4 input shape. Accepting it would skip fan-out and run
        one source while reporting a product run."""
        with self.assertRaises(ValueError) as cm:
            self.run_handler(source="S6")
        self.assertIn("published_product", str(cm.exception))

    def test_missing_bucket_raises(self):
        with self.assertRaises(ValueError):
            app.handler({"published_product": "NASA-SSH"}, None)


class TestRunParamsSidecar(_StartRunCase):
    def test_written_at_the_product_run_path(self):
        result = self.run_handler(published_product="NASA-SSH")
        key = f"pipeline_runs/NASA-SSH/{result['run_id']}/run_params.json"
        self.assertIn(key, app.s3.objects)

    def test_records_overrides_as_given(self):
        result = self.run_handler(
            published_product="NASA-SSH",
            start="2026-02-01",
            end="2026-02-28",
            force_update=True,
        )
        key = f"pipeline_runs/NASA-SSH/{result['run_id']}/run_params.json"
        params = json.loads(app.s3.objects[key])
        self.assertEqual(params["start"], "2026-02-01")
        self.assertEqual(params["end"], "2026-02-28")
        self.assertTrue(params["force_update"])
        self.assertFalse(params["defaults_used"])
        self.assertEqual(params["contributors"], ["GSFC", "S6"])

    def test_nominal_run_records_defaults_used(self):
        result = self.run_handler(published_product="NASA-SSH")
        key = f"pipeline_runs/NASA-SSH/{result['run_id']}/run_params.json"
        params = json.loads(app.s3.objects[key])
        self.assertTrue(params["defaults_used"])
        self.assertFalse(params["force_update"])

    def test_overrides_are_echoed_for_the_source_tier(self):
        """The Map threads these to each pipeline_init; dropping them here would
        silently turn a force_update backfill into a nominal run."""
        result = self.run_handler(
            published_product="NASA-SSH", start="2026-02-01", end="2026-02-28",
            force_update=True,
        )
        self.assertEqual(result["start"], "2026-02-01")
        self.assertEqual(result["end"], "2026-02-28")
        self.assertTrue(result["force_update"])


if __name__ == "__main__":
    unittest.main()
