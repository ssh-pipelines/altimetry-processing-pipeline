import unittest
from unittest.mock import MagicMock
from datetime import datetime

from daily_files.config.source_config import get_source_config, get_available_sources
from daily_files.config.dataset_schema import validate_dataset
from daily_files.daily_file_job import DailyFileJob, make_empty


class TestEmptyTemplates(unittest.TestCase):
    """Verify that empty template datasets pass schema validation
    after make_empty() applies the required overrides."""

    def test_gsfc_empty_template(self):
        job = DailyFileJob("2023-01-01", "GSFC", "TOPEX")
        ds = make_empty(job)
        errors = validate_dataset(ds)
        self.assertEqual(errors, [], f"GSFC empty template schema errors: {errors}")
        ds.close()

    def test_s6_empty_template(self):
        job = DailyFileJob("2023-01-01", "S6", "S6A")
        ds = make_empty(job)
        errors = validate_dataset(ds)
        self.assertEqual(errors, [], f"S6 empty template schema errors: {errors}")
        ds.close()

    def test_empty_has_required_global_attrs(self):
        """Empty templates should have time_coverage_start/end set to the job date."""
        job = DailyFileJob("2023-06-15", "GSFC", "TOPEX")
        ds = make_empty(job)
        self.assertEqual(ds.attrs["time_coverage_start"], "2023-06-15T00:00:00Z")
        self.assertEqual(ds.attrs["time_coverage_end"], "2023-06-15T23:59:59Z")
        self.assertIn("date_created", ds.attrs)
        self.assertEqual(ds.attrs["comment"], "No data available from source")
        ds.close()
