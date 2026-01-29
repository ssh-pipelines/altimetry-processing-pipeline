import unittest

from daily_files.config.dataset_schema import validate_dataset
from daily_files.daily_file_job import DailyFileJob, make_empty


class TestEmptyTemplates(unittest.TestCase):
    """Verify that empty template datasets pass schema validation
    after make_empty() applies the required overrides."""

    def test_gsfc_empty_template(self):
        job = DailyFileJob("2023-01-01", "GSFC")
        ds = make_empty(job)
        errors = validate_dataset(ds)
        self.assertEqual(errors, [], f"GSFC empty template schema errors: {errors}")
        ds.close()

    def test_s6_empty_template(self):
        job = DailyFileJob("2023-01-01", "S6")
        ds = make_empty(job)
        errors = validate_dataset(ds)
        self.assertEqual(errors, [], f"S6 empty template schema errors: {errors}")
        ds.close()

    def test_empty_has_required_global_attrs(self):
        """Empty templates should have time_coverage_start/end set to the job date."""
        job = DailyFileJob("2023-06-15", "GSFC")
        ds = make_empty(job)
        self.assertEqual(ds.attrs["time_coverage_start"], "2023-06-15T00:00:00Z")
        self.assertEqual(ds.attrs["time_coverage_end"], "2023-06-15T23:59:59Z")
        self.assertIn("date_created", ds.attrs)
        self.assertEqual(ds.attrs["comment"], "No data available from source")
        ds.close()

    def test_empty_overrides_id_and_generation_step(self):
        """make_empty must set the DOI-based id and product_generation_step,
        overriding the template's defaults."""
        job = DailyFileJob("2023-06-15", "GSFC")
        ds = make_empty(job)
        self.assertEqual(ds.attrs["id"], "10.5067/NSREF-AT0V1")
        self.assertEqual(ds.attrs["product_generation_step"], "1")
        ds.close()
