import json
import os
import sys
import unittest

os.environ.setdefault("SNS_TOPIC_ARN", "arn:aws:sns:us-west-2:123456789012:test-topic")
os.environ.setdefault("AWS_REGION", "us-west-2")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")

sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "failure_handling")),
)

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


class TestRunIdFromJobsKey(unittest.TestCase):
    def test_valid_jobs_key(self):
        self.assertEqual(
            app._run_id_from_jobs_key("pipeline_runs/S6/20250528T120000/jobs.json"),
            "20250528T120000",
        )

    def test_short_path(self):
        self.assertEqual(app._run_id_from_jobs_key(""), "unknown")
        self.assertEqual(app._run_id_from_jobs_key("foo"), "unknown")


class TestStageResultsPrefix(unittest.TestCase):
    def test_format(self):
        self.assertEqual(
            app._stage_results_prefix("S6", "20250528T120000", "daily_file"),
            "pipeline_runs/S6/20250528T120000/results/daily_file/",
        )


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


if __name__ == "__main__":
    unittest.main()
