import json
import unittest

import summarizer


# ---------------------------------------------------------------------------
# Fake S3 client
# ---------------------------------------------------------------------------

class _Body:
    def __init__(self, data: bytes):
        self._data = data

    def read(self):
        return self._data


class _Paginator:
    def __init__(self, store):
        self._store = store

    def paginate(self, Bucket, Prefix):
        contents = [{"Key": k} for k in self._store if k.startswith(Prefix)]
        yield {"Contents": contents}


class FakeS3:
    """Minimal in-memory S3 stub: get_object, get_paginator(list_objects_v2)."""

    def __init__(self, objects: dict[str, bytes] | None = None):
        self.objects = dict(objects or {})
        self.put_calls = []

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(Key)  # stands in for a ClientError NoSuchKey
        return {"Body": _Body(self.objects[Key])}

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return _Paginator(self.objects)

    def put_object(self, **kwargs):
        self.put_calls.append(kwargs)


def _succeeded_file(outcomes):
    """A ResultWriter SUCCEEDED_*.json body: list of {Input, Output} entries."""
    return json.dumps([{"Input": {}, "Output": o} for o in outcomes]).encode()


def _outcome(stage, date, kind, key, *, status="success", provenance_complete=None,
             skip_reason=None):
    metadata = {}
    if provenance_complete is not None:
        metadata["provenance_complete"] = provenance_complete
    if skip_reason is not None:
        metadata["skip_reason"] = skip_reason
    outputs = [] if status == "skipped" else [{"key": key, "kind": kind}]
    return {
        "schema_version": 1,
        "stage": stage,
        "status": status,
        "date": date,
        "source": "S6",
        "outputs": outputs,
        "metadata": metadata,
    }


# ---------------------------------------------------------------------------
# reconcile_pipeline
# ---------------------------------------------------------------------------

class TestReconcilePipeline(unittest.TestCase):

    def _specs(self):
        return summarizer.PRODUCT_PIPELINES["along_track"]["deliverables"]

    def test_clean_run_no_missing(self):
        expected = ["2025-01-01", "2025-01-02"]
        outcomes = {
            "finalizer": [
                _outcome("finalizer", "2025-01-01", "daily_file_p3", "k1", provenance_complete=True),
                _outcome("finalizer", "2025-01-02", "daily_file_p3", "k2", provenance_complete=True),
            ],
            "unifier": [
                _outcome("unifier", "2025-01-01", "nasa_ssh_p3", "u1"),
                _outcome("unifier", "2025-01-02", "nasa_ssh_p3", "u2"),
            ],
        }
        section = summarizer.reconcile_pipeline(self._specs(), expected, outcomes)
        self.assertEqual(section["expected"], 2)
        self.assertEqual(section["deliverables"]["daily_file_p3"]["produced"], 2)
        self.assertEqual(section["deliverables"]["nasa_ssh_p3"]["produced"], 2)
        self.assertEqual(section["missing"], [])
        self.assertEqual(section["deliverables"]["daily_file_p3"]["provenance_incomplete"], 0)

    def test_anchor_gap_is_missing(self):
        expected = ["2025-01-01", "2025-01-02", "2025-01-03"]
        outcomes = {
            "finalizer": [
                _outcome("finalizer", "2025-01-01", "daily_file_p3", "k1"),
            ],
        }
        section = summarizer.reconcile_pipeline(self._specs(), expected, outcomes)
        missing_dates = [m["date"] for m in section["missing"]]
        self.assertEqual(missing_dates, ["2025-01-02", "2025-01-03"])
        self.assertTrue(all(m["reason"] == "no outcome" for m in section["missing"]))

    def test_conditional_unifier_omitted_when_absent(self):
        # non-unified run: no unifier outcomes -> nasa_ssh_p3 not reported at all
        expected = ["2025-01-01"]
        outcomes = {"finalizer": [_outcome("finalizer", "2025-01-01", "daily_file_p3", "k1")]}
        section = summarizer.reconcile_pipeline(self._specs(), expected, outcomes)
        self.assertIn("daily_file_p3", section["deliverables"])
        self.assertNotIn("nasa_ssh_p3", section["deliverables"])

    def test_skip_appears_in_missing_with_reason(self):
        specs = summarizer.PRODUCT_PIPELINES["gridded"]["deliverables"]
        expected = ["2025-03-10", "2025-03-17"]
        outcomes = {
            "simple_grids": [
                _outcome("simple_grids", "2025-03-17", "simple_grid", "g2"),
                _outcome("simple_grids", "2025-03-10", "simple_grid", None,
                         status="skipped", skip_reason="no daily files available in window"),
            ],
        }
        section = summarizer.reconcile_pipeline(specs, expected, outcomes)
        self.assertEqual(section["deliverables"]["simple_grid"]["produced"], 1)
        self.assertEqual(section["deliverables"]["simple_grid"]["skipped"], 1)
        self.assertEqual(
            section["missing"],
            [{"date": "2025-03-10", "reason": "no daily files available in window"}],
        )

    def test_provenance_incomplete_counted_but_absence_is_not(self):
        expected = ["2025-01-01", "2025-01-02"]
        outcomes = {
            "finalizer": [
                _outcome("finalizer", "2025-01-01", "daily_file_p3", "k1", provenance_complete=False),
                _outcome("finalizer", "2025-01-02", "daily_file_p3", "k2"),  # no flag => unknown
            ],
        }
        section = summarizer.reconcile_pipeline(self._specs(), expected, outcomes)
        self.assertEqual(section["deliverables"]["daily_file_p3"]["provenance_incomplete"], 1)


# ---------------------------------------------------------------------------
# build_summary
# ---------------------------------------------------------------------------

class TestBuildSummary(unittest.TestCase):

    def test_artifact_shape_non_unified(self):
        jobs_key = "pipeline_runs/S6/20250528T120000/jobs.json"
        manifests = {
            "along_track": [{"date": "2025-01-01"}],
            "gridded": [{"date": "2025-01-06"}],
        }
        outcomes = {
            "along_track": {"finalizer": [_outcome("finalizer", "2025-01-01", "daily_file_p3", "k1")]},
            "gridded": {
                "simple_grids": [_outcome("simple_grids", "2025-01-06", "simple_grid", "g1")],
                "enso": [_outcome("enso", "2025-01-06", "enso", "e1")],
            },
        }
        summary = summarizer.build_summary(jobs_key, manifests, outcomes, completed_at="2025-05-28T14:00:00Z")
        self.assertEqual(summary["schema_version"], 1)
        self.assertEqual(summary["source"], "S6")
        self.assertEqual(summary["run_id"], "20250528T120000")
        self.assertIsNone(summary["unified_source"])
        self.assertEqual(
            summary["product_pipelines"]["along_track"]["manifest"], jobs_key
        )
        self.assertEqual(
            summary["product_pipelines"]["gridded"]["manifest"],
            "pipeline_runs/S6/20250528T120000/sg_jobs.json",
        )

    def test_unified_source_detected(self):
        jobs_key = "pipeline_runs/S6/20250528T120000/NASA-SSH/jobs.json"
        summary = summarizer.build_summary(jobs_key, {}, {}, completed_at="t")
        self.assertEqual(summary["unified_source"], "NASA-SSH")
        self.assertEqual(
            summary["product_pipelines"]["gridded"]["manifest"],
            "pipeline_runs/S6/20250528T120000/NASA-SSH/sg_jobs.json",
        )


# ---------------------------------------------------------------------------
# S3 I/O helpers
# ---------------------------------------------------------------------------

class TestS3IO(unittest.TestCase):

    def test_read_manifest_missing_is_empty(self):
        s3 = FakeS3()
        self.assertEqual(summarizer.read_manifest(s3, "b", "missing.json"), [])

    def test_read_manifest_parses_list(self):
        s3 = FakeS3({"m.json": json.dumps([{"date": "2025-01-01"}]).encode()})
        self.assertEqual(summarizer.read_manifest(s3, "b", "m.json"), [{"date": "2025-01-01"}])

    def test_read_outcomes_parses_succeeded_only(self):
        prefix = "pipeline_runs/S6/run/results/finalizer/"
        s3 = FakeS3({
            prefix + "abc/SUCCEEDED_0.json": _succeeded_file(
                [_outcome("finalizer", "2025-01-01", "daily_file_p3", "k1")]
            ),
            prefix + "abc/FAILED_0.json": b"[]",
            prefix + "manifest.json": b"{}",
        })
        outcomes = summarizer.read_outcomes(s3, "b", prefix)
        self.assertEqual(len(outcomes), 1)
        self.assertEqual(outcomes[0]["stage"], "finalizer")

    def test_read_outcomes_handles_output_as_json_string(self):
        prefix = "p/"
        outcome = _outcome("finalizer", "2025-01-01", "daily_file_p3", "k1")
        body = json.dumps([{"Input": {}, "Output": json.dumps(outcome)}]).encode()
        s3 = FakeS3({prefix + "x/SUCCEEDED_0.json": body})
        outcomes = summarizer.read_outcomes(s3, "b", prefix)
        self.assertEqual(len(outcomes), 1)
        self.assertEqual(outcomes[0]["date"], "2025-01-01")

    def test_read_run_params_missing_is_empty(self):
        s3 = FakeS3()
        jobs_key = "pipeline_runs/S6/20250528T120000/jobs.json"
        self.assertEqual(summarizer.read_run_params(s3, "b", jobs_key), {})

    def test_read_run_params_parses_sidecar(self):
        jobs_key = "pipeline_runs/S6/20250528T120000/NASA-SSH/jobs.json"
        # derived from the original-source segment, not the unified one
        params_key = "pipeline_runs/S6/20250528T120000/run_params.json"
        s3 = FakeS3({params_key: json.dumps(
            {"source": "S6", "start": "2025-05-01", "end": "2025-05-31", "force_update": True}
        ).encode()})
        params = summarizer.read_run_params(s3, "b", jobs_key)
        self.assertEqual(params["start"], "2025-05-01")
        self.assertTrue(params["force_update"])


# ---------------------------------------------------------------------------
# Notification rendering
# ---------------------------------------------------------------------------

class TestRenderNotification(unittest.TestCase):

    def test_subject_and_body(self):
        summary = {
            "source": "S6",
            "unified_source": "NASA-SSH",
            "run_id": "R1",
            "completed_at": "t",
            "product_pipelines": {
                "along_track": {
                    "expected": 2,
                    "deliverables": {
                        "daily_file_p3": {"stage": "finalizer", "produced": 2,
                                          "skipped": 0, "provenance_incomplete": 1, "outputs": []},
                    },
                    "missing": [],
                },
            },
        }
        subject, body = summarizer.render_notification(summary)
        self.assertIn("Pipeline success: S6 / R1", subject)
        self.assertIn("S6 → NASA-SSH", body)
        self.assertIn("incomplete-lineage", body)
        # nominal run with no parameters sidecar
        self.assertIn("Parameters: none (scheduled defaults)", body)

    def test_params_overrides_rendered(self):
        summary = {
            "source": "S6", "unified_source": None, "run_id": "R1", "completed_at": "t",
            "parameters": {"start": "2025-05-01", "end": "2025-05-31", "force_update": True},
            "product_pipelines": {},
        }
        _, body = summarizer.render_notification(summary)
        self.assertIn("Parameters: start=2025-05-01, end=2025-05-31, force_update=true", body)

    def test_along_track_folds_unifier_when_complete(self):
        summary = {
            "source": "S6", "unified_source": "NASA-SSH", "run_id": "R1", "completed_at": "t",
            "parameters": {},
            "product_pipelines": {
                "along_track": {
                    "expected": 2,
                    "deliverables": {
                        "daily_file_p3": {
                            "stage": "finalizer", "produced": 2, "skipped": 0,
                            "provenance_incomplete": 0,
                            "outputs": [
                                {"key": "daily_files/p3/S6/2026/S6_x_20260201.nc", "kind": "daily_file_p3"},
                                {"key": "daily_files/p3/S6/2026/S6_x_20260202.nc", "kind": "daily_file_p3"},
                            ],
                        },
                        "nasa_ssh_p3": {
                            "stage": "unifier", "produced": 2, "skipped": 0,
                            "provenance_incomplete": 0, "outputs": [],
                        },
                    },
                    "missing": [],
                },
            },
        }
        _, body = summarizer.render_notification(summary)
        self.assertIn("p3 daily files [finalizer]: 2 produced → all unified to NASA-SSH", body)
        # the unifier is folded in, not shown as its own confusing row
        self.assertNotIn("nasa_ssh_p3", body)
        self.assertNotIn("[unifier]", body)
        # filenames are listed
        self.assertIn("S6_x_20260201.nc", body)

    def test_along_track_surfaces_unification_shortfall(self):
        summary = {
            "source": "S6", "unified_source": "NASA-SSH", "run_id": "R1", "completed_at": "t",
            "parameters": {},
            "product_pipelines": {
                "along_track": {
                    "expected": 16,
                    "deliverables": {
                        "daily_file_p3": {
                            "stage": "finalizer", "produced": 16, "skipped": 0,
                            "provenance_incomplete": 0, "outputs": [],
                        },
                        "nasa_ssh_p3": {
                            "stage": "unifier", "produced": 0, "skipped": 0,
                            "provenance_incomplete": 0, "outputs": [],
                        },
                    },
                    "missing": [],
                },
            },
        }
        _, body = summarizer.render_notification(summary)
        self.assertIn("16 produced → 0 of 16 unified to NASA-SSH", body)

    def test_filenames_capped(self):
        outputs = [{"key": f"d/f_{i}.nc", "kind": "simple_grid"} for i in range(summarizer._MAX_LISTED + 5)]
        summary = {
            "source": "S6", "unified_source": None, "run_id": "R1", "completed_at": "t",
            "parameters": {},
            "product_pipelines": {
                "gridded": {
                    "expected": len(outputs),
                    "deliverables": {
                        "simple_grid": {
                            "stage": "simple_grids", "produced": len(outputs),
                            "skipped": 0, "provenance_incomplete": 0, "outputs": outputs,
                        },
                    },
                    "missing": [],
                },
            },
        }
        _, body = summarizer.render_notification(summary)
        self.assertIn(f"… ({len(outputs)} total)", body)


class TestBuildSummaryParameters(unittest.TestCase):
    def test_parameters_threaded_into_artifact(self):
        jobs_key = "pipeline_runs/S6/20250528T120000/jobs.json"
        summary = summarizer.build_summary(
            jobs_key, {}, {}, run_params={"force_update": True}, completed_at="t"
        )
        self.assertEqual(summary["parameters"], {"force_update": True})

    def test_parameters_default_empty(self):
        jobs_key = "pipeline_runs/S6/20250528T120000/jobs.json"
        summary = summarizer.build_summary(jobs_key, {}, {}, completed_at="t")
        self.assertEqual(summary["parameters"], {})


if __name__ == "__main__":
    unittest.main()
