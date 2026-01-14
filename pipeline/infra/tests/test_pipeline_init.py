import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
import sys

sys.modules['cmr'] = MagicMock()
sys.modules['boto3'] = MagicMock()

from pipeline.infra.pipeline_init.app import (
    daily_file_end_date,
    chunk_dates_by_year,
    query_granules_with_source_logic,
    determine_source_for_date,
    handler,
    SWITCHOVER_DATE,
)


class TestDateUtilities(unittest.TestCase):
    """Test utility functions for date handling"""
    
    def test_daily_file_end_date_returns_past_monday(self):
        """Test that daily_file_end_date returns a date in the past"""
        result = daily_file_end_date()
        self.assertIsInstance(result, datetime)
        self.assertLess(result, datetime.today())
    
    def test_daily_file_end_date_is_friday(self):
        """Test that the returned date is a Friday (Monday + 4 days)"""
        result = daily_file_end_date()
        # Friday is weekday 4
        self.assertEqual(result.weekday(), 4)
    
    def test_chunk_dates_by_year_single_year(self):
        """Test chunking dates within a single year"""
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
        """Test chunking dates across multiple years"""
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
        """Test chunking an empty list of dates"""
        result = chunk_dates_by_year([])
        self.assertEqual(len(result), 0)


class TestSourceDetermination(unittest.TestCase):
    """Test source determination logic"""
    
    def test_determine_source_before_switchover(self):
        """Test source determination for dates before switchover"""
        date = datetime(2024, 1, 15)
        result = determine_source_for_date(date)
        self.assertEqual(result, "GSFC")
    
    def test_determine_source_on_switchover(self):
        """Test source determination on switchover date"""
        date = SWITCHOVER_DATE
        result = determine_source_for_date(date)
        self.assertEqual(result, "S6")
    
    def test_determine_source_after_switchover(self):
        """Test source determination for dates after switchover"""
        date = datetime(2024, 6, 1)
        result = determine_source_for_date(date)
        self.assertEqual(result, "S6")
    
    def test_determine_source_with_gsfc_override(self):
        """Test source determination with GSFC override"""
        date = datetime(2024, 6, 1)  # Would normally be S6
        result = determine_source_for_date(date, source_override="GSFC")
        self.assertEqual(result, "GSFC")
    
    def test_determine_source_with_s6_override(self):
        """Test source determination with S6 override"""
        date = datetime(2023, 1, 1)  # Would normally be GSFC
        result = determine_source_for_date(date, source_override="S6")
        self.assertEqual(result, "S6")


class TestGranuleQuerying(unittest.TestCase):
    """Test granule querying logic"""
    
    @patch('pipeline.infra.pipeline_init.app.query_gsfc')
    @patch('pipeline.infra.pipeline_init.app.query_s6')
    def test_query_granules_with_gsfc_override(self, mock_s6, mock_gsfc):
        """Test querying with manual GSFC source"""
        mock_gsfc.return_value = {
            datetime(2024, 1, 1).date(): datetime(2024, 1, 1, 12, 0, 0),
            datetime(2024, 1, 2).date(): datetime(2024, 1, 2, 12, 0, 0),
        }
        
        dates = [datetime(2024, 1, 1), datetime(2024, 1, 2)]
        result = query_granules_with_source_logic(dates, source_override="GSFC")
        
        mock_gsfc.assert_called_once()
        mock_s6.assert_not_called()
        self.assertEqual(len(result), 2)
    
    @patch('pipeline.infra.pipeline_init.app.query_gsfc')
    @patch('pipeline.infra.pipeline_init.app.query_s6')
    def test_query_granules_with_s6_override(self, mock_s6, mock_gsfc):
        """Test querying with manual S6 source"""
        mock_s6.return_value = {
            datetime(2024, 6, 1).date(): datetime(2024, 6, 1, 12, 0, 0),
            datetime(2024, 6, 2).date(): datetime(2024, 6, 2, 12, 0, 0),
        }
        
        dates = [datetime(2024, 6, 1), datetime(2024, 6, 2)]
        result = query_granules_with_source_logic(dates, source_override="S6")
        
        mock_s6.assert_called_once()
        mock_gsfc.assert_not_called()
        self.assertEqual(len(result), 2)
    
    @patch('pipeline.infra.pipeline_init.app.query_gsfc')
    @patch('pipeline.infra.pipeline_init.app.query_s6')
    def test_query_granules_default_switchover_logic(self, mock_s6, mock_gsfc):
        """Test querying with default switchover logic"""
        mock_gsfc.return_value = {
            datetime(2024, 1, 15).date(): datetime(2024, 1, 15, 12, 0, 0),
        }
        mock_s6.return_value = {
            datetime(2024, 6, 1).date(): datetime(2024, 6, 1, 12, 0, 0),
        }
        
        dates = [datetime(2024, 1, 15), datetime(2024, 6, 1)]
        result = query_granules_with_source_logic(dates, source_override=None)
        
        # Both should be called since dates span the switchover
        mock_gsfc.assert_called_once()
        mock_s6.assert_called_once()
        self.assertEqual(len(result), 2)
    
    @patch('pipeline.infra.pipeline_init.app.query_gsfc')
    @patch('pipeline.infra.pipeline_init.app.query_s6')
    def test_query_granules_multiple_years_with_override(self, mock_s6, mock_gsfc):
        """Test querying multiple years with source override"""
        mock_gsfc.return_value = {
            datetime(2022, 1, 1).date(): datetime(2022, 1, 1, 12, 0, 0),
            datetime(2023, 1, 1).date(): datetime(2023, 1, 1, 12, 0, 0),
        }
        
        dates = [datetime(2022, 1, 1), datetime(2023, 1, 1)]
        result = query_granules_with_source_logic(dates, source_override="GSFC")
        
        # Should be called twice (once per year)
        self.assertEqual(mock_gsfc.call_count, 2)
        mock_s6.assert_not_called()
    
    def test_query_granules_invalid_source(self):
        """Test that invalid source raises ValueError"""
        dates = [datetime(2024, 1, 1)]
        
        with self.assertRaises(ValueError) as context:
            query_granules_with_source_logic(dates, source_override="INVALID")
        
        self.assertIn("Invalid source", str(context.exception))


class TestHandler(unittest.TestCase):
    """Test the main Lambda handler function"""
    
    @patch('pipeline.infra.pipeline_init.app.query_granules_with_source_logic')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_missing_bucket(self, mock_daily_files, mock_granules):
        """Test handler raises error when bucket is missing"""
        event = {}
        context = None
        
        with self.assertRaises(ValueError) as context:
            handler(event, context)
        
        self.assertIn("bucket", str(context.exception))
    
    @patch('pipeline.infra.pipeline_init.app.query_granules_with_source_logic')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_force_update(self, mock_daily_files, mock_granules):
        """Test handler with force_update flag"""
        event = {
            "bucket": "test-bucket",
            "force_update": True,
            "start": "2024-01-01",
            "end": "2024-01-03",
        }
        context = None
        
        result = handler(event, context)
        
        # Should not query files or granules when force_update is True
        mock_daily_files.assert_not_called()
        mock_granules.assert_not_called()
        
        # Should return 3 jobs (one per day)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]["date"], "2024-01-01")
        self.assertEqual(result[1]["date"], "2024-01-02")
        self.assertEqual(result[2]["date"], "2024-01-03")
    
    @patch('pipeline.infra.pipeline_init.app.query_granules_with_source_logic')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_with_manual_source(self, mock_daily_files, mock_granules):
        """Test handler with manual source specification"""
        mock_daily_files.return_value = {}
        mock_granules.return_value = {}
        
        event = {
            "bucket": "test-bucket",
            "source": "GSFC",
            "start": "2024-01-01",
            "end": "2024-01-02",
        }
        context = None
        
        result = handler(event, context)
        
        # Verify granules were queried with GSFC source
        mock_granules.assert_called_once()
        call_args = mock_granules.call_args

        self.assertEqual(call_args[0][1], "GSFC") 
        
        # All jobs should use GSFC source
        for job in result:
            self.assertEqual(job["source"], "GSFC")
    
    @patch('pipeline.infra.pipeline_init.app.query_granules_with_source_logic')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_default_date_range(self, mock_daily_files, mock_granules):
        """Test handler uses default date range when not specified"""
        mock_daily_files.return_value = {}
        mock_granules.return_value = {}
        
        event = {
            "bucket": "test-bucket",
        }
        context = None
        
        result = handler(event, context)
        
        # Should query files and granules
        self.assertTrue(mock_daily_files.called)
        self.assertTrue(mock_granules.called)
    
    @patch('pipeline.infra.pipeline_init.app.query_granules_with_source_logic')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_full_lookback(self, mock_daily_files, mock_granules):
        """Test handler with full lookback option"""
        mock_daily_files.return_value = {}
        mock_granules.return_value = {}
        
        event = {
            "bucket": "test-bucket",
            "lookback": "full",
        }
        context = None
        
        result = handler(event, context)
        
        # Should query multiple years
        self.assertGreater(mock_daily_files.call_count, 1)
    
    @patch('pipeline.infra.pipeline_init.app.query_granules_with_source_logic')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_creates_jobs_for_new_files(self, mock_daily_files, mock_granules):
        """Test handler creates jobs when daily file doesn't exist"""
        # No daily files exist
        mock_daily_files.return_value = {}
        
        # But granules exist
        mock_granules.return_value = {
            datetime(2024, 1, 1).date(): datetime(2024, 1, 1, 12, 0, 0),
        }
        
        event = {
            "bucket": "test-bucket",
            "start": "2024-01-01",
            "end": "2024-01-01",
        }
        context = None
        
        result = handler(event, context)
        
        # Should create a job since daily file is missing
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["date"], "2024-01-01")
    
    @patch('pipeline.infra.pipeline_init.app.query_granules_with_source_logic')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_creates_jobs_for_updated_granules(self, mock_daily_files, mock_granules):
        """Test handler creates jobs when granule is newer than daily file"""
        # Daily file exists with old timestamp
        mock_daily_files.return_value = {
            datetime(2024, 1, 1).date(): datetime(2024, 1, 1, 10, 0, 0),
        }
        
        # Granule has newer timestamp
        mock_granules.return_value = {
            datetime(2024, 1, 1).date(): datetime(2024, 1, 1, 14, 0, 0),
        }
        
        event = {
            "bucket": "test-bucket",
            "start": "2024-01-01",
            "end": "2024-01-01",
        }
        context = None
        
        result = handler(event, context)
        
        # Should create a job since granule is newer
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["date"], "2024-01-01")
    
    @patch('pipeline.infra.pipeline_init.app.query_granules_with_source_logic')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_skips_up_to_date_files(self, mock_daily_files, mock_granules):
        """Test handler skips jobs when daily file is up to date"""
        # Daily file exists with newer timestamp than granule
        mock_daily_files.return_value = {
            datetime(2024, 1, 1).date(): datetime(2024, 1, 1, 14, 0, 0),
        }
        
        # Granule has older timestamp
        mock_granules.return_value = {
            datetime(2024, 1, 1).date(): datetime(2024, 1, 1, 10, 0, 0),
        }
        
        event = {
            "bucket": "test-bucket",
            "start": "2024-01-01",
            "end": "2024-01-01",
        }
        context = None
        
        result = handler(event, context)
        
        # Should not create a job since daily file is up to date
        self.assertEqual(len(result), 0)
    
    @patch('pipeline.infra.pipeline_init.app.query_granules_with_source_logic')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_invalid_source(self, mock_daily_files, mock_granules):
        """Test handler raises error for invalid source"""
        event = {
            "bucket": "test-bucket",
            "source": "INVALID",
            "start": "2024-01-01",
            "end": "2024-01-01",
        }
        context = None
        
        with self.assertRaises(ValueError) as context:
            handler(event, context)
        
        self.assertIn("Invalid source", str(context.exception))
    
    @patch('pipeline.infra.pipeline_init.app.query_granules_with_source_logic')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_respects_source_across_switchover(self, mock_daily_files, mock_granules):
        """Test handler respects manual source even across switchover date"""
        mock_daily_files.return_value = {}
        mock_granules.return_value = {}
        
        event = {
            "bucket": "test-bucket",
            "source": "S6",
            "start": "2024-01-15",  # Before switchover
            "end": "2024-01-25",    # After switchover
        }
        context = None
        
        result = handler(event, context)
        
        # All jobs should use S6, even dates before switchover
        for job in result:
            self.assertEqual(job["source"], "S6")
    
    @patch('pipeline.infra.pipeline_init.app.query_granules_with_source_logic')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_switchover_logic_without_manual_source(self, mock_daily_files, mock_granules):
        """Test handler uses switchover logic when source not specified"""
        mock_daily_files.return_value = {}
        mock_granules.return_value = {}
        
        event = {
            "bucket": "test-bucket",
            "start": "2024-01-15",  # Before switchover
            "end": "2024-01-25",    # After switchover
        }
        context = None
        
        result = handler(event, context)
        
        # Jobs should have different sources based on switchover date
        gsfc_jobs = [j for j in result if j["source"] == "GSFC"]
        s6_jobs = [j for j in result if j["source"] == "S6"]
        
        self.assertGreater(len(gsfc_jobs), 0)
        self.assertGreater(len(s6_jobs), 0)
        
        # All GSFC dates should be before switchover
        for job in gsfc_jobs:
            job_date = datetime.fromisoformat(job["date"])
            self.assertLess(job_date, SWITCHOVER_DATE)
        
        # All S6 dates should be on or after switchover
        for job in s6_jobs:
            job_date = datetime.fromisoformat(job["date"])
            self.assertGreaterEqual(job_date, SWITCHOVER_DATE)


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and boundary conditions"""
    
    @patch('pipeline.infra.pipeline_init.app.query_granules_with_source_logic')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_single_day_range(self, mock_daily_files, mock_granules):
        """Test handler with single day date range"""
        mock_daily_files.return_value = {}
        mock_granules.return_value = {}
        
        event = {
            "bucket": "test-bucket",
            "start": "2024-01-01",
            "end": "2024-01-01",
        }
        context = None
        
        result = handler(event, context)
        
        # Should handle single day
        self.assertEqual(len(result), 1)
    
    @patch('pipeline.infra.pipeline_init.app.query_granules_with_source_logic')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_year_boundary(self, mock_daily_files, mock_granules):
        """Test handler across year boundary"""
        mock_daily_files.return_value = {}
        mock_granules.return_value = {}
        
        event = {
            "bucket": "test-bucket",
            "start": "2023-12-30",
            "end": "2024-01-02",
        }
        context = None
        
        result = handler(event, context)
        
        # Should handle year boundary correctly
        self.assertEqual(len(result), 4)
        
        # Should call daily files query for both years
        self.assertEqual(mock_daily_files.call_count, 2)
    
    @patch('pipeline.infra.pipeline_init.app.query_granules_with_source_logic')
    @patch('pipeline.infra.pipeline_init.app.query_daily_files_for_year')
    def test_handler_start_date_before_1992(self, mock_daily_files, mock_granules):
        """Test handler clamps start date to 1992-10-25"""
        mock_daily_files.return_value = {}
        mock_granules.return_value = {}
        
        event = {
            "bucket": "test-bucket",
            "start": "1990-01-01",
            "end": "1992-10-26",
        }
        context = None
        
        result = handler(event, context)
        
        # Should only include dates from 1992-10-25 onwards
        earliest_date = min(datetime.fromisoformat(job["date"]) for job in result)
        self.assertEqual(earliest_date.date(), datetime(1992, 10, 25).date())


if __name__ == '__main__':
    unittest.main(verbosity=2)