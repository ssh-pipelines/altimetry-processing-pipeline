"""The handler's return shape — what the orchestrator reads off the wire.

ASL cannot read the source registry, so the published product a run feeds has
to be resolved here and handed to the state machine. Nothing gates publication:
every source resolves to a line, and the Choice that used to test a `unify` flag
is gone.

The run identity now arrives rather than being minted: one product-keyed run
fans out over several Sources, and the product tier joins their manifests on a
shared `run_id`.
"""
import unittest
from unittest.mock import patch

from app import handler

RUN_ID = "20250101T000000"


class TestHandlerReturnShape(unittest.TestCase):

    def _run(self, source, jobs=None, **overrides):
        event = {"bucket": "b", "source": source, "run_id": RUN_ID, **overrides}
        with patch("app.plan_jobs", return_value=jobs or []), patch(
            "app.write_manifest",
            return_value=f"pipeline_runs/{source}/{RUN_ID}/jobs.json",
        ):
            return handler(event, None)

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
            {"jobs_key", "bucket", "source", "run_id", "published_product", "job_count"},
        )
        self.assertEqual(result["job_count"], 0)

    def test_unconfigured_source_raises(self):
        with self.assertRaises(ValueError):
            handler({"bucket": "b", "source": "NASA-SSH", "run_id": RUN_ID}, None)


class TestRunIdIsSuppliedNotMinted(unittest.TestCase):
    """A locally-generated run_id would put this Source's leg under a run path
    nothing else in the execution writes to, and `merge_manifests` would read a
    manifest address that does not exist — so the parameter is required rather
    than defaulted."""

    def test_missing_run_id_raises(self):
        with self.assertRaises(ValueError) as cm:
            handler({"bucket": "b", "source": "S6"}, None)
        self.assertIn("run_id", str(cm.exception))

    def test_run_id_is_threaded_to_the_manifest_writer(self):
        with patch("app.plan_jobs", return_value=[]), patch(
            "app.write_manifest", return_value="k"
        ) as write:
            handler({"bucket": "b", "source": "S6", "run_id": RUN_ID}, None)
        bucket, source, run_id, jobs, _params = write.call_args.args
        self.assertEqual((bucket, source, run_id, jobs), ("b", "S6", RUN_ID, []))

    def test_run_id_is_echoed_for_the_parent(self):
        self.assertEqual(self._handler_run_id(), RUN_ID)

    def _handler_run_id(self):
        with patch("app.plan_jobs", return_value=[]), patch(
            "app.write_manifest", return_value="k"
        ):
            return handler(
                {"bucket": "b", "source": "S6", "run_id": RUN_ID}, None
            )["run_id"]


class TestPerSourceParamsSidecar(unittest.TestCase):
    """`resolved_start`/`resolved_end` differ per contributor — GSFC and S6 cap
    to different date ranges from the same invocation — which is why this sidecar
    survives alongside the run-level one `start_run` writes."""

    def test_resolved_range_is_capped_to_the_source(self):
        with patch("app.plan_jobs", return_value=[]), patch(
            "app.write_manifest", return_value="k"
        ) as write:
            handler(
                {
                    "bucket": "b",
                    "source": "GSFC",
                    "run_id": RUN_ID,
                    "start": "1990-01-01",
                    "end": "2030-01-01",
                },
                None,
            )
        params = write.call_args.args[4]
        # GSFC starts 1992-10-25 and ends 2025-12-31; both bounds are clamped.
        self.assertEqual(params["start"], "1990-01-01")
        self.assertEqual(params["resolved_start"], "1992-10-25")
        self.assertEqual(params["resolved_end"], "2025-12-31")
        self.assertFalse(params["defaults_used"])


if __name__ == "__main__":
    unittest.main()
