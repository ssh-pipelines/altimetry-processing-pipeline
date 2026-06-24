import unittest

from utilities.job_outcome import (
    SCHEMA_VERSION,
    SKIPPED,
    SUCCESS,
    JobOutcome,
    Output,
)


class TestOutput(unittest.TestCase):
    def test_to_dict(self):
        out = Output(key="daily_files/p3/S6/2025/x.nc", kind="daily_file_p3")
        self.assertEqual(
            out.to_dict(),
            {"key": "daily_files/p3/S6/2025/x.nc", "kind": "daily_file_p3"},
        )


class TestJobOutcomeSuccess(unittest.TestCase):
    def test_success_shape(self):
        outcome = JobOutcome.success(
            stage="finalizer",
            date="2025-01-07",
            source="S6",
            outputs=[Output(key="k.nc", kind="daily_file_p3")],
            metadata={"provenance_complete": True},
        )
        d = outcome.to_dict()
        self.assertEqual(d["schema_version"], SCHEMA_VERSION)
        self.assertEqual(d["stage"], "finalizer")
        self.assertEqual(d["status"], SUCCESS)
        self.assertEqual(d["date"], "2025-01-07")
        self.assertEqual(d["source"], "S6")
        self.assertEqual(d["outputs"], [{"key": "k.nc", "kind": "daily_file_p3"}])
        self.assertEqual(d["metadata"], {"provenance_complete": True})

    def test_success_defaults_empty_metadata(self):
        outcome = JobOutcome.success(
            stage="enso", date="2025-01-07", source="S6", outputs=[]
        )
        self.assertEqual(outcome.to_dict()["metadata"], {})


class TestJobOutcomeSkipped(unittest.TestCase):
    def test_skipped_shape(self):
        d = JobOutcome.skipped(
            stage="simple_grids",
            date="2025-03-10",
            source="S6",
            reason="no daily files available in window",
        ).to_dict()
        self.assertEqual(d["status"], SKIPPED)
        self.assertEqual(d["outputs"], [])
        self.assertEqual(
            d["metadata"]["skip_reason"], "no daily files available in window"
        )

    def test_skipped_merges_extra_metadata(self):
        d = JobOutcome.skipped(
            stage="simple_grids",
            date="2025-03-10",
            source="S6",
            reason="low coverage",
            metadata={"coverage": 0.1},
        ).to_dict()
        self.assertEqual(d["metadata"]["skip_reason"], "low coverage")
        self.assertEqual(d["metadata"]["coverage"], 0.1)


if __name__ == "__main__":
    unittest.main()
