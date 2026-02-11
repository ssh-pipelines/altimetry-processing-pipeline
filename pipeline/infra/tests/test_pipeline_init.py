import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, date
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
)
from pipeline.infra.pipeline_init.config.source_config import (
    get_source_config,
    get_available_sources,
    PipelineInitSourceConfig,
    CollectionConfig,
)


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

    def test_collection_config_fields(self):
        cfg = get_source_config("S6")
        for col in cfg.collections:
            self.assertIsInstance(col, CollectionConfig)
            self.assertTrue(col.concept_id)
            self.assertIsInstance(col.priority, int)


class TestHandler(unittest.TestCase):
    """Test the main Lambda handler function"""

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
            "start": "2024-01-20",
            "end": "2024-01-22",
        }
        result = handler(event, None)

        mock_daily_files.assert_not_called()
        mock_cmr.assert_not_called()

        jobs = result["jobs"]
        self.assertEqual(len(jobs), 3)
        self.assertEqual(jobs[0]["date"], "2024-01-20")
        self.assertEqual(jobs[1]["date"], "2024-01-21")
        self.assertEqual(jobs[2]["date"], "2024-01-22")
        for job in jobs:
            self.assertEqual(job["source"], "S6")
            self.assertEqual(job["satellite"], "S6")
            self.assertEqual(job["bucket"], "test-bucket")

    @patch('pipeline.infra.pipeline_init.app.query_cmr')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_creates_jobs_for_missing_files(self, mock_daily_files, mock_cmr):
        mock_daily_files.return_value = {}
        mock_cmr.return_value = {
            datetime(2024, 1, 20).date(): datetime(2024, 1, 20, 12, 0, 0),
        }

        event = {
            "bucket": "test-bucket",
            "source": "S6",
            "start": "2024-01-20",
            "end": "2024-01-20",
        }
        result = handler(event, None)

        jobs = result["jobs"]
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["date"], "2024-01-20")
        self.assertEqual(jobs[0]["source"], "S6")

    @patch('pipeline.infra.pipeline_init.app.query_cmr')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_creates_jobs_for_updated_granules(self, mock_daily_files, mock_cmr):
        mock_daily_files.return_value = {
            datetime(2024, 1, 20).date(): datetime(2024, 1, 20, 10, 0, 0),
        }
        mock_cmr.return_value = {
            datetime(2024, 1, 20).date(): datetime(2024, 1, 20, 14, 0, 0),
        }

        event = {
            "bucket": "test-bucket",
            "source": "S6",
            "start": "2024-01-20",
            "end": "2024-01-20",
        }
        result = handler(event, None)

        jobs = result["jobs"]
        self.assertEqual(len(jobs), 1)

    @patch('pipeline.infra.pipeline_init.app.query_cmr')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_skips_up_to_date_files(self, mock_daily_files, mock_cmr):
        mock_daily_files.return_value = {
            datetime(2024, 1, 20).date(): datetime(2024, 1, 20, 14, 0, 0),
        }
        mock_cmr.return_value = {
            datetime(2024, 1, 20).date(): datetime(2024, 1, 20, 10, 0, 0),
        }

        event = {
            "bucket": "test-bucket",
            "source": "S6",
            "start": "2024-01-20",
            "end": "2024-01-20",
        }
        result = handler(event, None)

        jobs = result["jobs"]
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

        jobs = result["jobs"]
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

        # Should produce jobs starting from S6 config start_date (2024-01-20)
        if result["jobs"]:
            earliest = min(job["date"] for job in result["jobs"])
            self.assertGreaterEqual(earliest, "2024-01-20")

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

        jobs = result["jobs"]
        self.assertEqual(len(jobs), 4)

    @patch('pipeline.infra.pipeline_init.app.query_cmr')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_all_sources_accepted(self, mock_daily_files, mock_cmr):
        """Every configured source can be passed to handler without error"""
        mock_daily_files.return_value = {}
        mock_cmr.return_value = {}

        for source in get_available_sources():
            event = {
                "bucket": "test-bucket",
                "source": source,
                "force_update": True,
                "start": "2025-01-01",
                "end": "2025-01-01",
            }
            result = handler(event, None)
            self.assertIn("jobs", result)


if __name__ == '__main__':
    unittest.main(verbosity=2)
