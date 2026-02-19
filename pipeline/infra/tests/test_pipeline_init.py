import unittest
from unittest.mock import patch, MagicMock, call
from datetime import datetime, date
import json
import sys
import os

# Add pipeline_init dir to path so "config" package is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline_init"))

sys.modules['cmr'] = MagicMock()
sys.modules['boto3'] = MagicMock()

from pipeline.infra.pipeline_init.app import (
    daily_file_end_date,
    chunk_dates_by_year,
    handler,
    s3,
)
from pipeline.infra.pipeline_init.config.source_config import (
    get_source_config,
    get_available_sources,
    PipelineInitSourceConfig,
    CollectionConfig,
)


def get_manifest_from_s3_mock(mock_s3):
    """Extract the jobs list written to S3 via put_object."""
    put_calls = [c for c in mock_s3.put_object.call_args_list]
    if not put_calls:
        return []
    body = put_calls[-1].kwargs.get("Body") or put_calls[-1][1].get("Body")
    return json.loads(body)


class TestDateUtilities(unittest.TestCase):
    """Test utility functions for date handling"""

    def test_daily_file_end_date_returns_past_datetime(self):
        result = daily_file_end_date()
        self.assertIsInstance(result, datetime)
        self.assertLess(result, datetime.today())

    def test_daily_file_end_date_is_friday(self):
        result = daily_file_end_date()
        self.assertEqual(result.weekday(), 4)

    def test_chunk_dates_by_year_single_year(self):
        dates = [
            datetime(2024, 1, 1),
            datetime(2024, 6, 15),
            datetime(2024, 12, 31),
        ]
        result = chunk_dates_by_year(dates)
        self.assertEqual(len(result), 1)
        self.assertIn(2024, result)
        self.assertEqual(len(result[2024]), 3)

    def test_chunk_dates_by_year_multiple_years(self):
        dates = [
            datetime(2022, 1, 1),
            datetime(2023, 6, 15),
            datetime(2023, 12, 31),
            datetime(2024, 3, 1),
        ]
        result = chunk_dates_by_year(dates)
        self.assertEqual(len(result), 3)
        self.assertEqual(len(result[2022]), 1)
        self.assertEqual(len(result[2023]), 2)
        self.assertEqual(len(result[2024]), 1)

    def test_chunk_dates_by_year_empty_list(self):
        result = chunk_dates_by_year([])
        self.assertEqual(len(result), 0)


class TestSourceConfig(unittest.TestCase):
    """Test pipeline_init source configuration"""

    def test_available_sources(self):
        sources = get_available_sources()
        self.assertIn("GSFC", sources)
        self.assertIn("S6", sources)
        self.assertIn("S6B", sources)

    def test_invalid_source_raises(self):
        with self.assertRaises(ValueError):
            get_source_config("NONEXISTENT")

    def test_gsfc_config_fields(self):
        cfg = get_source_config("GSFC")
        self.assertIsInstance(cfg, PipelineInitSourceConfig)
        self.assertEqual(cfg.source, "GSFC")
        self.assertEqual(cfg.satellite, "GSFC")
        self.assertEqual(cfg.start_date, date(1992, 10, 25))
        self.assertTrue(cfg.s3_prefix)
        self.assertTrue(cfg.filename_pattern)
        self.assertEqual(len(cfg.collections), 1)
        self.assertTrue(cfg.unify)

    def test_s6_config_has_multiple_collections(self):
        cfg = get_source_config("S6")
        self.assertEqual(cfg.satellite, "S6")
        self.assertGreater(len(cfg.collections), 1)
        priorities = [c.priority for c in cfg.collections]
        self.assertEqual(priorities, sorted(priorities))

    def test_s6b_config_fields(self):
        cfg = get_source_config("S6B")
        self.assertIsInstance(cfg, PipelineInitSourceConfig)
        self.assertEqual(cfg.source, "S6B")
        self.assertEqual(cfg.satellite, "S6B")
        self.assertIsInstance(cfg.start_date, date)
        self.assertFalse(cfg.unify)

    def test_collection_config_fields(self):
        cfg = get_source_config("S6")
        for col in cfg.collections:
            self.assertIsInstance(col, CollectionConfig)
            self.assertTrue(col.concept_id)
            self.assertIsInstance(col.priority, int)


class TestHandler(unittest.TestCase):
    """Test the main Lambda handler function"""

    def setUp(self):
        s3.put_object.reset_mock()

    @patch('pipeline.infra.pipeline_init.app.query_cmr')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_missing_bucket(self, mock_daily_files, mock_cmr):
        with self.assertRaises(ValueError) as ctx:
            handler({"source": "GSFC"}, None)
        self.assertIn("bucket", str(ctx.exception))

    @patch('pipeline.infra.pipeline_init.app.query_cmr')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_missing_source(self, mock_daily_files, mock_cmr):
        with self.assertRaises(ValueError) as ctx:
            handler({"bucket": "test-bucket"}, None)
        self.assertIn("source", str(ctx.exception))

    @patch('pipeline.infra.pipeline_init.app.query_cmr')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_invalid_source(self, mock_daily_files, mock_cmr):
        with self.assertRaises(ValueError) as ctx:
            handler({"bucket": "test-bucket", "source": "INVALID"}, None)
        self.assertIn("Invalid source", str(ctx.exception))

    @patch('pipeline.infra.pipeline_init.app.query_cmr')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_force_update(self, mock_daily_files, mock_cmr):
        event = {
            "bucket": "test-bucket",
            "source": "S6",
            "force_update": True,
            "start": "2024-01-21",
            "end": "2024-01-23",
        }
        result = handler(event, None)

        mock_daily_files.assert_not_called()
        mock_cmr.assert_not_called()

        # Return is manifest ref, not jobs array
        self.assertIn("jobs_key", result)
        self.assertEqual(result["bucket"], "test-bucket")
        self.assertEqual(result["source"], "S6")
        self.assertIn("unify", result)

        # Verify jobs written to S3
        jobs = get_manifest_from_s3_mock(s3)
        self.assertEqual(len(jobs), 3)
        self.assertEqual(jobs[0]["date"], "2024-01-21")
        self.assertEqual(jobs[1]["date"], "2024-01-22")
        self.assertEqual(jobs[2]["date"], "2024-01-23")
        for job in jobs:
            self.assertEqual(job["source"], "S6")
            self.assertEqual(job["bucket"], "test-bucket")
            self.assertNotIn("satellite", job)

    @patch('pipeline.infra.pipeline_init.app.query_cmr')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_creates_jobs_for_missing_files(self, mock_daily_files, mock_cmr):
        mock_daily_files.return_value = {}
        mock_cmr.return_value = {
            datetime(2024, 1, 21).date(): datetime(2024, 1, 21, 12, 0, 0),
        }

        event = {
            "bucket": "test-bucket",
            "source": "S6",
            "start": "2024-01-21",
            "end": "2024-01-21",
        }
        result = handler(event, None)

        jobs = get_manifest_from_s3_mock(s3)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["date"], "2024-01-21")
        self.assertEqual(jobs[0]["source"], "S6")

    @patch('pipeline.infra.pipeline_init.app.query_cmr')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_creates_jobs_for_updated_granules(self, mock_daily_files, mock_cmr):
        mock_daily_files.return_value = {
            datetime(2024, 1, 21).date(): datetime(2024, 1, 21, 10, 0, 0),
        }
        mock_cmr.return_value = {
            datetime(2024, 1, 21).date(): datetime(2024, 1, 21, 14, 0, 0),
        }

        event = {
            "bucket": "test-bucket",
            "source": "S6",
            "start": "2024-01-21",
            "end": "2024-01-21",
        }
        result = handler(event, None)

        jobs = get_manifest_from_s3_mock(s3)
        self.assertEqual(len(jobs), 1)

    @patch('pipeline.infra.pipeline_init.app.query_cmr')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_skips_up_to_date_files(self, mock_daily_files, mock_cmr):
        mock_daily_files.return_value = {
            datetime(2024, 1, 21).date(): datetime(2024, 1, 21, 14, 0, 0),
        }
        mock_cmr.return_value = {
            datetime(2024, 1, 21).date(): datetime(2024, 1, 21, 10, 0, 0),
        }

        event = {
            "bucket": "test-bucket",
            "source": "S6",
            "start": "2024-01-21",
            "end": "2024-01-21",
        }
        result = handler(event, None)

        jobs = get_manifest_from_s3_mock(s3)
        self.assertEqual(len(jobs), 0)

    @patch('pipeline.infra.pipeline_init.app.query_cmr')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_clamps_start_to_config_start_date(self, mock_daily_files, mock_cmr):
        """Start date before config.start_date gets clamped"""
        mock_daily_files.return_value = {}
        mock_cmr.return_value = {}

        event = {
            "bucket": "test-bucket",
            "source": "GSFC",
            "start": "1990-01-01",
            "end": "1992-10-26",
        }
        result = handler(event, None)

        jobs = get_manifest_from_s3_mock(s3)
        earliest = min(job["date"] for job in jobs)
        self.assertEqual(earliest, "1992-10-25")

    @patch('pipeline.infra.pipeline_init.app.query_cmr')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_default_range_uses_config_start(self, mock_daily_files, mock_cmr):
        """Default range (no start/end) uses config.start_date"""
        mock_daily_files.return_value = {}
        mock_cmr.return_value = {}

        event = {
            "bucket": "test-bucket",
            "source": "S6",
        }
        result = handler(event, None)

        # Return is manifest ref
        self.assertIn("jobs_key", result)
        self.assertEqual(result["source"], "S6")

        jobs = get_manifest_from_s3_mock(s3)
        if jobs:
            earliest = min(job["date"] for job in jobs)
            self.assertGreaterEqual(earliest, "2024-01-21")

    @patch('pipeline.infra.pipeline_init.app.query_cmr')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_year_boundary(self, mock_daily_files, mock_cmr):
        mock_daily_files.return_value = {}
        mock_cmr.return_value = {}

        event = {
            "bucket": "test-bucket",
            "source": "S6",
            "force_update": True,
            "start": "2024-12-30",
            "end": "2025-01-02",
        }
        result = handler(event, None)

        jobs = get_manifest_from_s3_mock(s3)
        self.assertEqual(len(jobs), 4)

    @patch('pipeline.infra.pipeline_init.app.query_cmr')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_all_sources_accepted(self, mock_daily_files, mock_cmr):
        """Every configured source can be passed to handler without error"""
        mock_daily_files.return_value = {}
        mock_cmr.return_value = {}

        for source in get_available_sources():
            s3.put_object.reset_mock()
            event = {
                "bucket": "test-bucket",
                "source": source,
                "force_update": True,
                "start": "2025-01-01",
                "end": "2025-01-01",
            }
            result = handler(event, None)
            self.assertIn("jobs_key", result)
            self.assertIn("unify", result)

    @patch('pipeline.infra.pipeline_init.app.query_cmr')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_manifest_key_format(self, mock_daily_files, mock_cmr):
        """Manifest key follows expected pattern"""
        event = {
            "bucket": "test-bucket",
            "source": "S6",
            "force_update": True,
            "start": "2024-01-21",
            "end": "2024-01-21",
        }
        result = handler(event, None)

        self.assertTrue(result["jobs_key"].startswith("pipeline_runs/S6/"))
        self.assertTrue(result["jobs_key"].endswith("/jobs.json"))

    @patch('pipeline.infra.pipeline_init.app.query_cmr')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_returns_unify_flag(self, mock_daily_files, mock_cmr):
        """Handler returns unify flag from source config"""
        event = {
            "bucket": "test-bucket",
            "source": "GSFC",
            "force_update": True,
            "start": "1992-10-25",
            "end": "1992-10-25",
        }
        result = handler(event, None)
        self.assertTrue(result["unify"])

        s3.put_object.reset_mock()
        event["source"] = "S6B"
        event["start"] = "2025-11-26"
        event["end"] = "2025-11-26"
        result = handler(event, None)
        self.assertFalse(result["unify"])


if __name__ == '__main__':
    unittest.main(verbosity=2)
