import json
import os
import sys
import unittest

os.environ.setdefault("SNS_TOPIC_ARN", "arn:aws:sns:us-west-2:123456789012:test-topic")
os.environ.setdefault("AWS_REGION", "us-west-2")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")

import app  # noqa: E402


class TestClassify(unittest.TestCase):
    cases = [
        ("Lambda.Timeout", "", "Runtime failure"),
        ("Lambda.OutOfMemory", "", "Runtime failure"),
        ("Sandbox.Timedout", "Task timed out after 15.00 seconds", "Runtime failure"),
        ("States.Timeout", "", "Runtime failure"),
        ("KeyError", "'cycle'", "Code failure"),
        ("TypeError", "missing required argument", "Code failure"),
        ("", "", "Code failure"),
        (
            "ClientError",
            "An error occurred (401) when calling the GetObject operation: Unauthorized",
            "Auth failure",
        ),
        ("HTTPError", "403 Forbidden", "Auth failure"),
        ("RequestException", "Got Forbidden response from upstream", "Auth failure"),
        ("PipelineError", "KeyError: 'cycle'", "Code failure"),
    ]

    def test_classify_cases(self):
        for error_type, error_message, expected in self.cases:
            with self.subTest(error_type=error_type, message=error_message):
                self.assertEqual(app._classify(error_type, error_message), expected)


class TestParseFailedItem(unittest.TestCase):
    def test_empty_entry(self):
        result = app._parse_failed_item({})
        self.assertEqual(result["errorType"], "Unknown")
        self.assertEqual(result["errorMessage"], "")
        self.assertIsNone(result["input"])
        self.assertEqual(result["category"], "Code failure")

    def test_lambda_shape_only(self):
        cause = json.dumps({"errorType": "KeyError", "errorMessage": "'cycle'"})
        result = app._parse_failed_item({"Cause": cause, "Error": "Lambda.Function"})
        self.assertEqual(result["errorType"], "KeyError")
        self.assertEqual(result["errorMessage"], "'cycle'")
        self.assertIsNone(result["input"])
        self.assertEqual(result["category"], "Code failure")

    def test_pipeline_error_with_packaged_payload(self):
        """Handler raised PipelineError(json.dumps({errorType, errorMessage, input}))."""
        inner = json.dumps(
            {
                "errorType": "KeyError",
                "errorMessage": "'cycle' missing from granule metadata",
                "input": {"date": "2025-02-01", "source": "S3B"},
            }
        )
        outer = json.dumps({"errorType": "PipelineError", "errorMessage": inner})
        result = app._parse_failed_item({"Cause": outer, "Error": "Lambda.Function"})
        self.assertEqual(result["errorType"], "KeyError")
        self.assertEqual(result["errorMessage"], "'cycle' missing from granule metadata")
        self.assertEqual(result["input"], {"date": "2025-02-01", "source": "S3B"})
        self.assertEqual(result["category"], "Code failure")

    def test_sm_envelope_wrapping_lambda_failure(self):
        """Parent SM Catch sees a child-SM-failure envelope; the leaf is nested inside."""
        leaf = json.dumps(
            {
                "errorType": "Sandbox.Timedout",
                "errorMessage": "RequestId: abc Error: Task timed out after 15.00 seconds",
            }
        )
        sm_input = json.dumps(
            {"jobs_key": "pipeline_runs/S3B/run/jobs.json", "bucket": "test", "source": "S3B"}
        )
        sm_envelope = json.dumps(
            {
                "Cause": leaf,
                "Error": "Sandbox.Timedout",
                "ExecutionArn": "arn:aws:states:us-west-2:123:execution:test:abc",
                "Input": sm_input,
                "Status": "FAILED",
            }
        )
        result = app._parse_failed_item({"Cause": sm_envelope, "Error": "States.TaskFailed"})
        self.assertEqual(result["errorType"], "Sandbox.Timedout")
        self.assertIn("Task timed out", result["errorMessage"])
        self.assertEqual(result["category"], "Runtime failure")
        self.assertEqual(result["input"]["source"], "S3B")

    def test_auth_failure_via_pipeline_error(self):
        inner = json.dumps(
            {
                "errorType": "ClientError",
                "errorMessage": "An error occurred (401) when calling GetObject: Unauthorized",
                "input": {"date": "2025-02-01", "source": "S3B"},
            }
        )
        outer = json.dumps({"errorType": "PipelineError", "errorMessage": inner})
        result = app._parse_failed_item({"Cause": outer, "Error": "Lambda.Function"})
        self.assertEqual(result["category"], "Auth failure")
        self.assertEqual(result["input"]["source"], "S3B")

    def test_malformed_json_cause(self):
        result = app._parse_failed_item({"Cause": "not json at all", "Error": "Something"})
        self.assertEqual(result["errorType"], "Something")
        self.assertEqual(result["errorMessage"], "not json at all")

    def test_map_failed_entry_runtime_timeout_recovers_entry_input(self):
        """Realistic Distributed Map FAILED entry: handler never ran, so the
        per-item input is only available at the entry's top-level `Input` field."""
        entry = {
            "Status": "FAILED",
            "Error": "Sandbox.Timedout",
            "Cause": "Task timed out after 15.00 seconds",
            "Input": json.dumps({"date": "2025-02-01", "source": "S3B", "bucket": "test"}),
            "ExecutionArn": "arn:aws:states:us-west-2:123:execution:test:abc",
        }
        result = app._parse_failed_item(entry)
        self.assertEqual(result["errorType"], "Sandbox.Timedout")
        self.assertEqual(result["category"], "Runtime failure")
        self.assertEqual(result["input"], {"date": "2025-02-01", "source": "S3B", "bucket": "test"})

    def test_entry_input_does_not_override_packaged_input(self):
        """If the handler packaged an input (PipelineError path), prefer that
        over the entry-level Input field — they should agree, but the packaged
        one is what the handler actually saw at failure time."""
        inner = json.dumps({
            "errorType": "KeyError",
            "errorMessage": "'cycle'",
            "input": {"date": "2025-02-01", "source": "S3B", "from_handler": True},
        })
        outer = json.dumps({"errorType": "PipelineError", "errorMessage": inner})
        entry = {
            "Cause": outer,
            "Error": "Lambda.Function",
            "Input": json.dumps({"date": "2025-02-01", "source": "S3B", "from_handler": False}),
        }
        result = app._parse_failed_item(entry)
        self.assertTrue(result["input"]["from_handler"])

    def test_entry_input_non_json_string_preserved(self):
        """Defensive: if entry Input isn't JSON, keep it as a raw string."""
        result = app._parse_failed_item({
            "Status": "FAILED",
            "Error": "Sandbox.Timedout",
            "Cause": "Task timed out",
            "Input": "raw-string-not-json",
        })
        self.assertEqual(result["input"], "raw-string-not-json")


class TestRunIdFromJobsKey(unittest.TestCase):
    def test_valid_jobs_key(self):
        self.assertEqual(
            app._run_id_from_jobs_key("pipeline_runs/S6/20250528T120000/jobs.json"),
            "20250528T120000",
        )

    def test_short_path(self):
        self.assertEqual(app._run_id_from_jobs_key(""), "unknown")
        self.assertEqual(app._run_id_from_jobs_key("foo"), "unknown")


class TestResultsPrefixFromJobsKey(unittest.TestCase):
    def test_at_side_jobs_key(self):
        self.assertEqual(
            app._results_prefix_from_jobs_key(
                "pipeline_runs/S6/20250528T120000/jobs.json", "daily_file"
            ),
            "pipeline_runs/S6/20250528T120000/results/daily_file/",
        )

    def test_sg_side_jobs_key_uses_original_source_segment(self):
        """Post-unifier the SM input's `source` is "NASA-SSH" but the jobs_key
        path's $p[1] is the original source. The prefix must use $p[1] to find
        what the ResultWriter actually wrote."""
        self.assertEqual(
            app._results_prefix_from_jobs_key(
                "pipeline_runs/S6/20250528T120000/NASA-SSH/sg_jobs.json", "enso"
            ),
            "pipeline_runs/S6/20250528T120000/results/enso/",
        )

    def test_short_jobs_key_returns_none(self):
        self.assertIsNone(app._results_prefix_from_jobs_key("", "enso"))
        self.assertIsNone(app._results_prefix_from_jobs_key("a/b", "enso"))


class TestDedupe(unittest.TestCase):
    def test_groups_by_category_type_message(self):
        items = [
            {"category": "Code failure", "errorType": "KeyError", "errorMessage": "'cycle'",
             "input": {"date": "2025-02-01"}},
            {"category": "Code failure", "errorType": "KeyError", "errorMessage": "'cycle'",
             "input": {"date": "2025-02-02"}},
            {"category": "Code failure", "errorType": "KeyError", "errorMessage": "'cycle'",
             "input": {"date": "2025-02-03"}},
            {"category": "Runtime failure", "errorType": "Lambda.Timeout", "errorMessage": "",
             "input": {"date": "2025-02-04"}},
        ]
        result = app._dedupe(items)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["count"], 3)
        self.assertEqual(result[0]["category"], "Code failure")
        self.assertEqual(
            result[0]["affected_dates"], ["2025-02-01", "2025-02-02", "2025-02-03"]
        )
        self.assertEqual(result[1]["count"], 1)
        self.assertEqual(result[1]["category"], "Runtime failure")

    def test_handles_missing_input(self):
        items = [
            {"category": "Code failure", "errorType": "X", "errorMessage": "y", "input": None}
        ]
        result = app._dedupe(items)
        self.assertEqual(result[0]["affected_dates"], ["?"])


class TestChildExecutionArnFromCause(unittest.TestCase):
    def test_extracts_arn(self):
        cause = json.dumps(
            {"ExecutionArn": "arn:aws:states:us-west-2:123:execution:foo:bar", "Error": "X"}
        )
        self.assertEqual(
            app._child_execution_arn_from_cause(cause),
            "arn:aws:states:us-west-2:123:execution:foo:bar",
        )

    def test_returns_none_for_non_json(self):
        self.assertIsNone(app._child_execution_arn_from_cause("not json"))
        self.assertIsNone(app._child_execution_arn_from_cause(""))


class TestSampleInputForDisplay(unittest.TestCase):
    def test_strips_granules_list(self):
        sample = {"date": "2025-02-01", "source": "S3B", "granules": ["url1", "url2", "url3"]}
        result = app._sample_input_for_display(sample)
        self.assertEqual(result["granules"], "[3 URIs omitted]")
        self.assertEqual(result["date"], "2025-02-01")
        self.assertEqual(result["source"], "S3B")

    def test_passes_through_non_dict(self):
        self.assertEqual(app._sample_input_for_display("plain string"), "plain string")
        self.assertIsNone(app._sample_input_for_display(None))

    def test_leaves_input_without_granules_alone(self):
        sample = {"date": "2025-02-01", "source": "S3B"}
        self.assertEqual(app._sample_input_for_display(sample), sample)


class TestTopLevelEnvelopeError(unittest.TestCase):
    def test_extracts_error(self):
        env = json.dumps({"Error": "States.ExceedToleratedFailureThreshold", "Cause": "..."})
        self.assertEqual(
            app._top_level_envelope_error(env), "States.ExceedToleratedFailureThreshold"
        )

    def test_returns_empty_when_no_envelope(self):
        self.assertEqual(app._top_level_envelope_error(""), "")
        self.assertEqual(app._top_level_envelope_error("plain text"), "")


class TestOrigSourceFromJobsKey(unittest.TestCase):
    def test_at_side(self):
        self.assertEqual(
            app._orig_source_from_jobs_key("pipeline_runs/S6/20250528T120000/jobs.json"),
            "S6",
        )

    def test_sg_side_after_unification(self):
        self.assertEqual(
            app._orig_source_from_jobs_key("pipeline_runs/S6/20250528T120000/NASA-SSH/sg_jobs.json"),
            "S6",
        )

    def test_short_path(self):
        self.assertEqual(app._orig_source_from_jobs_key(""), "unknown")
        self.assertEqual(app._orig_source_from_jobs_key("pipeline_runs"), "unknown")


def _product(label, count, files, always):
    return {"label": label, "count": count, "files": files, "always": always}


class TestFormatSuccessMessage(unittest.TestCase):
    def test_unified_source_lists_filenames(self):
        msg = app._format_success_message(
            orig_source="S6",
            sg_source="NASA-SSH",
            run_id="20250528T120000",
            completed_at="2025-05-28T14:00:00Z",
            products=[
                _product("Along-track P3 files", 2,
                         ["NASA-SSH_alt_at_20250107.nc", "NASA-SSH_alt_at_20250108.nc"], True),
                _product("Simple grids", 1, ["NASA-SSH_simple_grid_20250107.nc"], True),
                _product("ENSO files", 1, ["ENSO_20250107.nc"], False),
            ],
        )
        self.assertIn("S6 → NASA-SSH", msg)
        self.assertIn("Along-track P3 files: 2", msg)
        self.assertIn("    NASA-SSH_alt_at_20250107.nc", msg)
        self.assertIn("    NASA-SSH_alt_at_20250108.nc", msg)
        self.assertIn("Simple grids: 1", msg)
        self.assertIn("    NASA-SSH_simple_grid_20250107.nc", msg)
        self.assertIn("ENSO files: 1", msg)
        self.assertIn("    ENSO_20250107.nc", msg)
        self.assertIn("Indicators: complete", msg)

    def test_no_bucket_location_lines(self):
        msg = app._format_success_message(
            orig_source="S6",
            sg_source="NASA-SSH",
            run_id="20250528T120000",
            completed_at="2025-05-28T14:00:00Z",
            products=[
                _product("Along-track P3 files", 1, ["NASA-SSH_alt_at_20250107.nc"], True),
            ],
        )
        self.assertNotIn("s3://", msg)
        self.assertNotIn("daily_files/p3/", msg)

    def test_no_unification_single_source(self):
        msg = app._format_success_message(
            orig_source="S6",
            sg_source="S6",
            run_id="20250528T120000",
            completed_at="2025-05-28T14:00:00Z",
            products=[_product("Simple grids", 1, ["S6_simple_grid_20250107.nc"], True)],
        )
        self.assertIn("Source: S6", msg)
        self.assertNotIn("→", msg)

    def test_zero_count_always_product_shows_fallback_text(self):
        msg = app._format_success_message(
            orig_source="S6",
            sg_source="S6",
            run_id="20250528T120000",
            completed_at="2025-05-28T14:00:00Z",
            products=[_product("Along-track P3 files", 0, [], True)],
        )
        self.assertIn("Along-track P3 files: 0 (or ResultWriter output not found)", msg)

    def test_no_enso_line_when_count_zero(self):
        msg = app._format_success_message(
            orig_source="S6",
            sg_source="S6",
            run_id="20250528T120000",
            completed_at="2025-05-28T14:00:00Z",
            products=[
                _product("Along-track P3 files", 1, ["S6_alt_at_20250107.nc"], True),
                _product("ENSO files", 0, [], False),
            ],
        )
        self.assertNotIn("ENSO", msg)

    def test_long_listing_is_capped(self):
        files = [f"S6_alt_at_2025{i:04d}.nc" for i in range(app.MAX_FILES_LISTED + 5)]
        msg = app._format_success_message(
            orig_source="S6",
            sg_source="S6",
            run_id="20250528T120000",
            completed_at="2025-05-28T14:00:00Z",
            products=[_product("Along-track P3 files", len(files), files, True)],
        )
        self.assertIn("...and 5 more", msg)


class TestYears(unittest.TestCase):
    def test_extracts_years(self):
        self.assertEqual(app._years({"2025-01-07", "2025-12-31", "2024-06-01"}), {2025, 2024})

    def test_ignores_malformed(self):
        self.assertEqual(app._years({"", "bad", "2025-01-07"}), {2025})

    def test_empty(self):
        self.assertEqual(app._years(set()), set())


class TestListProductFilesTokenMatch(unittest.TestCase):
    """_list_product_files matches files by the compact YYYYMMDD token; verify the
    token logic via a stubbed paginator so the date-scoping is covered without S3."""

    def _run(self, keys, dates):
        class _Paginator:
            def paginate(self, Bucket, Prefix):
                yield {"Contents": [{"Key": k} for k in keys]}

        orig = app.s3
        app.s3 = type("S", (), {"get_paginator": lambda self, name: _Paginator()})()
        try:
            return app._list_product_files("b", ["prefix/"], dates)
        finally:
            app.s3 = orig

    def test_filters_to_run_dates(self):
        keys = [
            "prefix/S6_alt_at_20250107.nc",
            "prefix/S6_alt_at_20250108.nc",
            "prefix/S6_alt_at_20250109.nc",  # not in run
        ]
        out = self._run(keys, {"2025-01-07", "2025-01-08"})
        self.assertEqual(out, ["S6_alt_at_20250107.nc", "S6_alt_at_20250108.nc"])

    def test_empty_dates_returns_empty(self):
        self.assertEqual(self._run(["prefix/S6_alt_at_20250107.nc"], set()), [])


if __name__ == "__main__":
    unittest.main()
