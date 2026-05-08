import unittest
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

from config.source_config import get_source_config
from enumeration.base import GranuleRef
from planning import plan_jobs


class TestPlanJobs(unittest.TestCase):
    def _patch_enumerator(self, refs: list[GranuleRef]):
        enumerator = MagicMock()
        enumerator.enumerate.return_value = refs
        return patch("planning.build_enumerator", return_value=enumerator)

    @patch("planning.scan_existing_p3_mod_times", return_value={})
    def test_no_existing_p3_produces_job(self, _scan):
        cfg = get_source_config("GSFC")
        refs = [
            GranuleRef(
                date=date(2023, 12, 17),
                uri="s3://b/g1.nc",
                mod_time=datetime(2023, 12, 17, 1, 0, 0, tzinfo=timezone.utc),
            ),
        ]
        with self._patch_enumerator(refs):
            jobs = plan_jobs(cfg, "test-bucket", date(2023, 12, 17), date(2023, 12, 17), False)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["date"], "2023-12-17")
        self.assertEqual(jobs[0]["granules"], ["s3://b/g1.nc"])
        self.assertEqual(jobs[0]["bucket"], "test-bucket")

    @patch(
        "planning.scan_existing_p3_mod_times",
        return_value={date(2023, 12, 17): datetime(2023, 12, 18, tzinfo=timezone.utc)},
    )
    def test_stale_upstream_skips_job(self, _scan):
        cfg = get_source_config("GSFC")
        refs = [
            GranuleRef(
                date=date(2023, 12, 17),
                uri="s3://b/g1.nc",
                mod_time=datetime(2023, 12, 17, 1, 0, 0, tzinfo=timezone.utc),
            ),
        ]
        with self._patch_enumerator(refs):
            jobs = plan_jobs(cfg, "test-bucket", date(2023, 12, 17), date(2023, 12, 17), False)
        self.assertEqual(jobs, [])

    @patch(
        "planning.scan_existing_p3_mod_times",
        return_value={date(2023, 12, 17): datetime(2023, 12, 18, tzinfo=timezone.utc)},
    )
    def test_force_update_overrides_diff(self, _scan):
        cfg = get_source_config("GSFC")
        refs = [
            GranuleRef(
                date=date(2023, 12, 17),
                uri="s3://b/g1.nc",
                mod_time=datetime(2023, 12, 17, 1, 0, 0, tzinfo=timezone.utc),
            ),
        ]
        with self._patch_enumerator(refs):
            jobs = plan_jobs(cfg, "test-bucket", date(2023, 12, 17), date(2023, 12, 17), True)
        self.assertEqual(len(jobs), 1)

    @patch("planning.scan_existing_p3_mod_times", return_value={})
    def test_granules_grouped_by_date_and_sorted(self, _scan):
        cfg = get_source_config("S6")
        refs = [
            GranuleRef(
                date=date(2023, 12, 17),
                uri="s3://b/b.nc",
                mod_time=datetime(2023, 12, 17, 1, 0, 0, tzinfo=timezone.utc),
                sort_key=(1, 20),
            ),
            GranuleRef(
                date=date(2023, 12, 17),
                uri="s3://b/a.nc",
                mod_time=datetime(2023, 12, 17, 2, 0, 0, tzinfo=timezone.utc),
                sort_key=(1, 10),
            ),
            GranuleRef(
                date=date(2023, 12, 18),
                uri="s3://b/c.nc",
                mod_time=datetime(2023, 12, 18, 1, 0, 0, tzinfo=timezone.utc),
                sort_key=(1, 5),
            ),
        ]
        with self._patch_enumerator(refs):
            jobs = plan_jobs(cfg, "test-bucket", date(2023, 12, 17), date(2023, 12, 18), False)
        self.assertEqual(len(jobs), 2)
        self.assertEqual(jobs[0]["date"], "2023-12-17")
        self.assertEqual(jobs[0]["granules"], ["s3://b/a.nc", "s3://b/b.nc"])
        self.assertEqual(jobs[1]["date"], "2023-12-18")
        self.assertEqual(jobs[1]["granules"], ["s3://b/c.nc"])
