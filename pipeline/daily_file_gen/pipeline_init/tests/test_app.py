"""The handler's return shape — what the orchestrator reads off the wire.

ASL cannot read the source registry, so the published product a run feeds has
to be resolved here and handed to the state machine. Nothing gates publication:
every source resolves to a line, and the Choice that used to test a `unify` flag
is gone.
"""
import unittest
from unittest.mock import patch

from app import handler


class TestHandlerReturnShape(unittest.TestCase):

    def _run(self, source, jobs=None):
        with patch("app.plan_jobs", return_value=jobs or []), patch(
            "app.write_manifest",
            return_value=f"pipeline_runs/{source}/20250101T000000/jobs.json",
        ):
            return handler({"bucket": "b", "source": source}, None)

    def test_reference_source_resolves_the_shared_line(self):
        result = self._run("S6")
        self.assertEqual(result["published_product"], "NASA-SSH")
        self.assertEqual(result["source"], "S6")

    def test_high_latitude_source_resolves_its_own_line(self):
        self.assertEqual(self._run("S3B")["published_product"], "NASA-SSH_S3B")

    def test_no_publish_gate_on_the_wire(self):
        """`unify` is deleted rather than rekeyed: with identity derived from
        product_type there is nothing left to opt into."""
        self.assertNotIn("unify", self._run("S6"))

    def test_run_identity_fields_survive(self):
        result = self._run("GSFC")
        self.assertEqual(
            set(result),
            {"jobs_key", "bucket", "source", "published_product", "job_count"},
        )
        self.assertEqual(result["job_count"], 0)

    def test_unconfigured_source_raises(self):
        with self.assertRaises(ValueError):
            handler({"bucket": "b", "source": "NASA-SSH"}, None)


if __name__ == "__main__":
    unittest.main()
