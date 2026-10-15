import json
import unittest

import summarizer

from utilities import job_outcome

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


def _bad_pass_succeeded_file(results):
    """A bad_pass ResultWriter SUCCEEDED_*.json body. The bad_pass Map has no
    `Output: $states.result.Payload` unwrap, so each entry's Output is the raw
    Lambda invoke envelope wrapping the handler return `{date, source, count}`."""
    return json.dumps([
        {"Input": {}, "Output": {"Payload": r, "StatusCode": 200}} for r in results
    ]).encode()


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
            "publisher_along_track": [
                _outcome("publisher", "2025-01-01", "published_along_track", "u1"),
                _outcome("publisher", "2025-01-02", "published_along_track", "u2"),
            ],
        }
        section = summarizer.reconcile_pipeline(self._specs(), expected, outcomes)
        self.assertEqual(section["expected"], 2)
        self.assertEqual(section["deliverables"]["daily_file_p3"]["produced"], 2)
        self.assertEqual(section["deliverables"]["published_along_track"]["produced"], 2)
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

    def test_publication_shortfall_is_reported_not_hidden(self):
        """Publication is unconditional, so a run that produced P3s but published
        nothing must show `0 produced` rather than omitting the row — the
        conditional path would have hidden exactly this failure."""
        expected = ["2025-01-01"]
        outcomes = {"finalizer": [_outcome("finalizer", "2025-01-01", "daily_file_p3", "k1")]}
        section = summarizer.reconcile_pipeline(self._specs(), expected, outcomes)
        self.assertIn("daily_file_p3", section["deliverables"])
        self.assertEqual(section["deliverables"]["published_along_track"]["produced"], 0)

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

    def test_artifact_shape(self):
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
        summary = summarizer.build_summary(
            "NASA-SSH", "20250528T120000", ["GSFC", "S6"], manifests, outcomes,
            completed_at="2025-05-28T14:00:00Z",
        )
        self.assertEqual(summary["schema_version"], 2)
        self.assertEqual(summary["published_product"], "NASA-SSH")
        self.assertEqual(summary["contributors"], ["GSFC", "S6"])
        self.assertEqual(summary["run_id"], "20250528T120000")

    def test_no_top_level_source(self):
        """A run is one published product's, not one Source's. A `source` field
        would have to name one of several contributors."""
        summary = summarizer.build_summary(
            "NASA-SSH", "R1", ["GSFC", "S6"], {}, {}, completed_at="t"
        )
        self.assertNotIn("source", summary)

    def test_schema_version_is_the_run_summarys_own(self):
        """Job outcomes did not change shape, so bumping the shared constant
        would have relabelled every one of them."""
        self.assertEqual(summarizer.RUN_SUMMARY_SCHEMA_VERSION, 2)
        self.assertEqual(job_outcome.SCHEMA_VERSION, 1)

    def test_along_track_names_every_contributors_manifest(self):
        """Naming only one of several would point a reader at an incomplete
        picture of the run."""
        summary = summarizer.build_summary(
            "NASA-SSH", "R1", ["GSFC", "S6"], {}, {}, completed_at="t"
        )
        self.assertEqual(
            summary["product_pipelines"]["along_track"]["manifests"],
            ["pipeline_runs/GSFC/R1/jobs.json", "pipeline_runs/S6/R1/jobs.json"],
        )

    def test_gridded_manifest_is_at_the_product_run_path(self):
        summary = summarizer.build_summary(
            "NASA-SSH", "R1", ["GSFC", "S6"], {}, {}, completed_at="t"
        )
        self.assertEqual(
            summary["product_pipelines"]["gridded"]["manifests"],
            ["pipeline_runs/NASA-SSH/R1/sg_jobs.json"],
        )

    def test_high_latitude_run_has_one_contributor_and_an_empty_gridded_section(self):
        summary = summarizer.build_summary(
            "NASA-SSH_S3B", "R1", ["S3B"], {}, {}, completed_at="t"
        )
        self.assertEqual(
            summary["product_pipelines"]["along_track"]["manifests"],
            ["pipeline_runs/S3B/R1/jobs.json"],
        )
        self.assertEqual(summary["product_pipelines"]["gridded"]["expected"], 0)


class TestManifestKeys(unittest.TestCase):
    def test_source_tier_is_per_contributor_and_product_tier_is_not(self):
        keys = summarizer.manifest_keys("NASA-SSH", "R1", ["GSFC", "S6"])
        self.assertEqual(len(keys["along_track"]), 2)
        self.assertEqual(len(keys["gridded"]), 1)

    def test_no_contributors_yields_no_along_track_manifests(self):
        keys = summarizer.manifest_keys("NASA-SSH", "R1", [])
        self.assertEqual(keys["along_track"], [])
        self.assertEqual(keys["gridded"], ["pipeline_runs/NASA-SSH/R1/sg_jobs.json"])


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

    def test_read_outcomes_unwraps_lambda_envelope(self):
        """Defensive: a Map invoke task missing the `Output: $states.result.Payload`
        unwrap writes the raw Lambda envelope; reconciliation must still see the outcome
        (regression guard for the promotion stage's `produced: 0` bug)."""
        prefix = "p/"
        outcome = _outcome("publisher", "2025-01-01", "published_along_track", "u1")
        envelope = {"Payload": outcome, "StatusCode": 200, "ExecutedVersion": "$LATEST"}
        body = json.dumps([{"Input": {}, "Output": envelope}]).encode()
        s3 = FakeS3({prefix + "x/SUCCEEDED_0.json": body})
        outcomes = summarizer.read_outcomes(s3, "b", prefix)
        self.assertEqual(len(outcomes), 1)
        self.assertEqual(outcomes[0]["status"], "success")
        self.assertEqual(outcomes[0]["stage"], "publisher")

    def test_read_run_params_missing_is_empty(self):
        s3 = FakeS3()
        self.assertEqual(
            summarizer.read_run_params(s3, "b", "NASA-SSH", "20250528T120000"), {}
        )

    def test_read_run_params_reads_the_product_tier_sidecar(self):
        """start_run's, not a contributor's: "how was this run invoked" has one
        answer per run, while each pipeline_init's resolved window differs per
        Source and a multi-contributor run has no single value for it."""
        s3 = FakeS3({
            "pipeline_runs/NASA-SSH/20250528T120000/run_params.json": json.dumps(
                {"published_product": "NASA-SSH", "start": "2025-05-01",
                 "end": "2025-05-31", "force_update": True}
            ).encode(),
            # A contributor's sidecar must not be picked up in its place.
            "pipeline_runs/S6/20250528T120000/run_params.json": json.dumps(
                {"source": "S6", "start": "2099-01-01"}
            ).encode(),
        })
        params = summarizer.read_run_params(s3, "b", "NASA-SSH", "20250528T120000")
        self.assertEqual(params["start"], "2025-05-01")
        self.assertTrue(params["force_update"])


# ---------------------------------------------------------------------------
# Notification rendering
# ---------------------------------------------------------------------------

class TestRenderNotification(unittest.TestCase):

    def test_subject_and_body(self):
        summary = {
            "published_product": "NASA-SSH",
            "contributors": ["GSFC", "S6"],
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
        # The product is the subject of the run; the contributors are how it was fed.
        self.assertIn("Pipeline success: NASA-SSH / R1", subject)
        self.assertIn("Product: NASA-SSH ← GSFC, S6", body)
        self.assertIn("incomplete-lineage", body)
        # nominal run with no parameters sidecar
        self.assertIn("Parameters: none (scheduled defaults)", body)

    def test_single_contributor_is_still_listed(self):
        """So the reference line's two-mission handover weeks read the same as
        its one-mission weeks."""
        summary = {
            "published_product": "NASA-SSH", "contributors": ["S6"], "run_id": "R1",
            "completed_at": "t", "parameters": {}, "product_pipelines": {},
        }
        _, body = summarizer.render_notification(summary)
        self.assertIn("Product: NASA-SSH ← S6", body)

    def test_no_contributors_renders_the_product_alone(self):
        summary = {
            "published_product": "NASA-SSH", "contributors": [], "run_id": "R1",
            "completed_at": "t", "parameters": {}, "product_pipelines": {},
        }
        _, body = summarizer.render_notification(summary)
        self.assertIn("Product: NASA-SSH", body)
        self.assertNotIn("←", body)

    def test_params_overrides_rendered(self):
        summary = {
            "published_product": "NASA-SSH", "contributors": ["S6"], "run_id": "R1", "completed_at": "t",
            "parameters": {"start": "2025-05-01", "end": "2025-05-31", "force_update": True},
            "product_pipelines": {},
        }
        _, body = summarizer.render_notification(summary)
        self.assertIn("Parameters: start=2025-05-01, end=2025-05-31, force_update=true", body)

    def test_along_track_folds_publication_when_complete(self):
        summary = {
            "published_product": "NASA-SSH", "contributors": ["S6"], "run_id": "R1", "completed_at": "t",
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
                        "published_along_track": {
                            "stage": "publisher_along_track", "produced": 2, "skipped": 0,
                            "provenance_incomplete": 0, "outputs": [],
                        },
                    },
                    "missing": [],
                },
            },
        }
        _, body = summarizer.render_notification(summary)
        self.assertIn("p3 daily files [finalizer]: 2 produced → all published to NASA-SSH", body)
        # the promotion is folded in, not shown as its own confusing row
        self.assertNotIn("published_along_track", body)
        self.assertNotIn("[publisher_along_track]", body)
        # filenames are listed
        self.assertIn("S6_x_20260201.nc", body)

    def test_along_track_surfaces_publication_shortfall(self):
        summary = {
            "published_product": "NASA-SSH", "contributors": ["S6"], "run_id": "R1", "completed_at": "t",
            "parameters": {},
            "product_pipelines": {
                "along_track": {
                    "expected": 16,
                    "deliverables": {
                        "daily_file_p3": {
                            "stage": "finalizer", "produced": 16, "skipped": 0,
                            "provenance_incomplete": 0, "outputs": [],
                        },
                        "published_along_track": {
                            "stage": "publisher_along_track", "produced": 0, "skipped": 0,
                            "provenance_incomplete": 0, "outputs": [],
                        },
                    },
                    "missing": [],
                },
            },
        }
        _, body = summarizer.render_notification(summary)
        self.assertIn("16 produced → 0 of 16 published to NASA-SSH", body)

    def test_filenames_capped(self):
        outputs = [{"key": f"d/f_{i}.nc", "kind": "simple_grid"} for i in range(summarizer._MAX_LISTED + 5)]
        summary = {
            "published_product": "NASA-SSH", "contributors": ["S6"], "run_id": "R1", "completed_at": "t",
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


# ---------------------------------------------------------------------------
# bad_pass aggregation + rendering
# ---------------------------------------------------------------------------

class TestSummarizeBadPasses(unittest.TestCase):

    def test_empty_results(self):
        bp = summarizer.summarize_bad_passes([])
        self.assertEqual(bp["dates_reported"], 0)
        self.assertEqual(bp["dates_flagged"], 0)
        self.assertEqual(bp["total_flagged"], 0)
        self.assertEqual(bp["by_date"], [])

    def test_aggregates_and_sorts_flagged_dates(self):
        results = [
            {"date": "2025-01-03", "source": "S6", "count": 2},
            {"date": "2025-01-01", "source": "S6", "count": 0},
            {"date": "2025-01-02", "source": "S6", "count": 5},
        ]
        bp = summarizer.summarize_bad_passes(results)
        # all three dates reported, but only two had flags
        self.assertEqual(bp["dates_reported"], 3)
        self.assertEqual(bp["dates_flagged"], 2)
        self.assertEqual(bp["total_flagged"], 7)
        self.assertEqual(
            bp["by_date"],
            [{"date": "2025-01-02", "count": 5}, {"date": "2025-01-03", "count": 2}],
        )

    def test_ignores_malformed_entries(self):
        results = [
            {"date": "2025-01-02", "source": "S6", "count": 3},
            {"source": "S6", "count": 4},          # no date
            {"date": "2025-01-05", "source": "S6"},  # no count
            "not-a-dict",
        ]
        bp = summarizer.summarize_bad_passes(results)
        self.assertEqual(bp["dates_reported"], 1)
        self.assertEqual(bp["total_flagged"], 3)


class TestGatherBadPasses(unittest.TestCase):

    def test_gather_reads_bad_pass_results(self):
        prefix = "pipeline_runs/S6/20250528T120000/results/bad_pass/"
        s3 = FakeS3({
            prefix + "run/SUCCEEDED_0.json": _bad_pass_succeeded_file([
                {"date": "2025-01-01", "source": "S6", "count": 2},
                {"date": "2025-01-02", "source": "S6", "count": 0},
            ]),
        })
        _, _, _, bad_pass_results = summarizer.gather(
            s3, "b", "NASA-SSH", "20250528T120000", ["S6"]
        )
        # the raw Lambda envelope is unwrapped back to the handler return
        counts = {r["date"]: r["count"] for r in bad_pass_results}
        self.assertEqual(counts, {"2025-01-01": 2, "2025-01-02": 0})


class TestGatherAcrossContributors(unittest.TestCase):
    """The source tier is read once per contributor and concatenated.

    Reconciling one contributor's leg would report a shortfall for every other
    contributor's dates — on a clean run. GSFC ends 2025-12-31 and S6 starts
    2026-01-01, so a run over the handover has both.
    """

    def _handover_bucket(self):
        objects = {}
        for source, dates in (("GSFC", ["2025-12-30", "2025-12-31"]),
                              ("S6", ["2026-01-01", "2026-01-02"])):
            objects[f"pipeline_runs/{source}/R1/jobs.json"] = json.dumps(
                [{"date": d, "source": source} for d in dates]
            ).encode()
            objects[f"pipeline_runs/{source}/R1/results/finalizer/m/SUCCEEDED_0.json"] = (
                _succeeded_file([
                    _outcome("finalizer", d, "daily_file_p3", f"{source}_{d}.nc")
                    for d in dates
                ])
            )
            objects[f"pipeline_runs/{source}/R1/results/publisher_along_track/m/SUCCEEDED_0.json"] = (
                _succeeded_file([
                    _outcome("publisher", d, "published_along_track", f"pub_{d}.nc")
                    for d in dates
                ])
            )
        objects["pipeline_runs/NASA-SSH/R1/sg_jobs.json"] = json.dumps(
            [{"date": "2025-12-29"}]
        ).encode()
        return FakeS3(objects)

    def test_manifest_dates_union_both_contributors(self):
        manifests, _o, _p, _bp = summarizer.gather(
            self._handover_bucket(), "b", "NASA-SSH", "R1", ["GSFC", "S6"]
        )
        self.assertEqual(
            summarizer.manifest_dates(manifests["along_track"]),
            ["2025-12-30", "2025-12-31", "2026-01-01", "2026-01-02"],
        )

    def test_outcomes_concatenate_across_contributors(self):
        _m, outcomes, _p, _bp = summarizer.gather(
            self._handover_bucket(), "b", "NASA-SSH", "R1", ["GSFC", "S6"]
        )
        self.assertEqual(len(outcomes["along_track"]["finalizer"]), 4)
        self.assertEqual(len(outcomes["along_track"]["publisher_along_track"]), 4)

    def test_a_clean_handover_run_reports_nothing_missing(self):
        s3 = self._handover_bucket()
        manifests, outcomes, params, bad_pass = summarizer.gather(
            s3, "b", "NASA-SSH", "R1", ["GSFC", "S6"]
        )
        summary = summarizer.build_summary(
            "NASA-SSH", "R1", ["GSFC", "S6"], manifests, outcomes,
            run_params=params, bad_pass_results=bad_pass, completed_at="t",
        )
        at = summary["product_pipelines"]["along_track"]
        self.assertEqual(at["expected"], 4)
        self.assertEqual(at["deliverables"]["daily_file_p3"]["produced"], 4)
        self.assertEqual(at["missing"], [])

    def test_reading_one_contributor_only_would_have_shown_a_shortfall(self):
        """The regression this guards: the pre-Phase-4 shape reconciled a single
        jobs_key, which under a product-keyed run is one contributor's leg."""
        s3 = self._handover_bucket()
        manifests, outcomes, _p, _bp = summarizer.gather(
            s3, "b", "NASA-SSH", "R1", ["GSFC"]
        )
        summary = summarizer.build_summary(
            "NASA-SSH", "R1", ["GSFC"], manifests, outcomes, completed_at="t"
        )
        self.assertEqual(summary["product_pipelines"]["along_track"]["expected"], 2)

    def test_bad_pass_results_are_read_per_contributor(self):
        s3 = self._handover_bucket()
        for source, count in (("GSFC", 2), ("S6", 5)):
            s3.objects[
                f"pipeline_runs/{source}/R1/results/bad_pass/m/SUCCEEDED_0.json"
            ] = _bad_pass_succeeded_file(
                [{"date": "2025-12-30", "source": source, "count": count}]
            )
        _m, _o, _p, bad_pass = summarizer.gather(s3, "b", "NASA-SSH", "R1", ["GSFC", "S6"])
        self.assertEqual(summarizer.summarize_bad_passes(bad_pass)["total_flagged"], 7)

    def test_a_missing_contributor_manifest_reconciles_as_empty(self):
        """Unlike merge_manifests, which fails on one: by the time the summary
        runs the tiers are done, and erroring here would turn a reportable
        shortfall into no report at all (ADR 0005: never fail a good run)."""
        manifests, _o, _p, _bp = summarizer.gather(
            FakeS3(), "b", "NASA-SSH", "R1", ["GSFC", "S6"]
        )
        self.assertEqual(manifests["along_track"], [])


class TestBuildSummaryBadPasses(unittest.TestCase):

    def test_bad_passes_threaded_into_artifact(self):
        summary = summarizer.build_summary(
            "NASA-SSH", "R1", ["S6"], {}, {},
            bad_pass_results=[{"date": "2025-01-02", "source": "S6", "count": 4}],
            completed_at="t",
        )
        self.assertEqual(summary["bad_passes"]["total_flagged"], 4)
        self.assertEqual(summary["bad_passes"]["by_date"], [{"date": "2025-01-02", "count": 4}])

    def test_bad_passes_default_empty(self):
        summary = summarizer.build_summary("NASA-SSH", "R1", ["S6"], {}, {}, completed_at="t")
        self.assertEqual(summary["bad_passes"]["total_flagged"], 0)
        self.assertEqual(summary["bad_passes"]["dates_reported"], 0)


class TestRenderBadPasses(unittest.TestCase):

    def _summary(self, bad_passes):
        # bad_pass lines render *within* the along_track section, so the summary
        # must carry one (mirrors the real artifact from build_summary).
        return {
            "published_product": "NASA-SSH", "contributors": ["S6"], "run_id": "R1", "completed_at": "t",
            "parameters": {},
            "product_pipelines": {
                "along_track": {
                    "expected": 1,
                    "deliverables": {
                        "daily_file_p3": {"stage": "finalizer", "produced": 1, "skipped": 0,
                                          "provenance_incomplete": 0, "outputs": []},
                    },
                    "missing": [],
                },
            },
            "bad_passes": bad_passes,
        }

    def test_flagged_rendered_with_breakdown(self):
        summary = self._summary({
            "dates_reported": 3, "dates_flagged": 2, "total_flagged": 7,
            "by_date": [{"date": "2025-01-02", "count": 5}, {"date": "2025-01-03", "count": 2}],
        })
        _, body = summarizer.render_notification(summary)
        self.assertIn("bad passes [bad_pass]: 7 flagged across 2 dates", body)
        self.assertIn("2025-01-02: 5", body)
        self.assertIn("2025-01-03: 2", body)
        # rendered inside the along_track section, before its trailing blank line
        at_idx = body.index("along_track (expected 1):")
        gridded_or_end = body.find("\n\n", at_idx)
        self.assertIn("bad passes", body[at_idx:gridded_or_end])

    def test_none_flagged_rendered(self):
        summary = self._summary({
            "dates_reported": 4, "dates_flagged": 0, "total_flagged": 0, "by_date": [],
        })
        _, body = summarizer.render_notification(summary)
        self.assertIn("bad passes [bad_pass]: none flagged", body)

    def test_absent_section_when_no_results(self):
        summary = self._summary({
            "dates_reported": 0, "dates_flagged": 0, "total_flagged": 0, "by_date": [],
        })
        _, body = summarizer.render_notification(summary)
        self.assertNotIn("bad passes", body)

    def test_singular_date_grammar(self):
        summary = self._summary({
            "dates_reported": 1, "dates_flagged": 1, "total_flagged": 3,
            "by_date": [{"date": "2025-01-02", "count": 3}],
        })
        _, body = summarizer.render_notification(summary)
        self.assertIn("across 1 date", body)
        self.assertNotIn("across 1 dates", body)


class TestBuildSummaryParameters(unittest.TestCase):
    def test_parameters_threaded_into_artifact(self):
        summary = summarizer.build_summary(
            "NASA-SSH", "R1", ["S6"], {}, {},
            run_params={"force_update": True}, completed_at="t",
        )
        self.assertEqual(summary["parameters"], {"force_update": True})

    def test_parameters_default_empty(self):
        summary = summarizer.build_summary("NASA-SSH", "R1", ["S6"], {}, {}, completed_at="t")
        self.assertEqual(summary["parameters"], {})


if __name__ == "__main__":
    unittest.main()
