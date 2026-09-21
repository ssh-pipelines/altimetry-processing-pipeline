import unittest
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from config.source_config import get_source_config
from enumeration.base import GranuleRef
from planning import dates_to_plan, plan_jobs


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


def _ref(d: date, uri: str, hour: int = 1) -> GranuleRef:
    return GranuleRef(date=d, uri=uri, mod_time=datetime(d.year, d.month, d.day, hour, tzinfo=timezone.utc))


class TestInteriorGaps(unittest.TestCase):
    """Dates with no upstream granules still need a job so that `daily_files`
    writes an empty daily file — but only when upstream coverage resumes after
    them (see issue #40, GSFC 6.1's 2019-02-24..2019-03-06 gap)."""

    def _patch_enumerator(self, refs: list[GranuleRef]):
        enumerator = MagicMock()
        enumerator.enumerate.return_value = refs
        return patch("planning.build_enumerator", return_value=enumerator)

    # Granules on 02-23 and 03-07 bracket a 12-day gap, mirroring GSFC 6.1.
    _GAP_REFS = [
        _ref(date(2019, 2, 23), "s3://b/before.nc"),
        _ref(date(2019, 3, 7), "s3://b/after.nc"),
    ]
    _GAP_DATES = [(date(2019, 2, 24) + timedelta(days=i)).isoformat() for i in range(11)]

    @patch("planning.scan_existing_p3_mod_times", return_value={})
    def test_interior_gap_planned_with_no_granules(self, _scan):
        cfg = get_source_config("GSFC")
        with self._patch_enumerator(self._GAP_REFS):
            jobs = plan_jobs(cfg, "test-bucket", date(2019, 2, 23), date(2019, 3, 7), False)

        by_date = {job["date"]: job for job in jobs}
        self.assertEqual(sorted(by_date), ["2019-02-23"] + self._GAP_DATES + ["2019-03-07"])
        for gap_date in self._GAP_DATES:
            self.assertEqual(by_date[gap_date]["granules"], [])
            self.assertEqual(by_date[gap_date]["source"], "GSFC")
            self.assertEqual(by_date[gap_date]["bucket"], "test-bucket")

    @patch(
        "planning.scan_existing_p3_mod_times",
        return_value={date(2019, 2, 28): datetime(2025, 1, 1, tzinfo=timezone.utc)},
    )
    def test_interior_gap_with_existing_empty_file_is_skipped(self, _scan):
        cfg = get_source_config("GSFC")
        with self._patch_enumerator(self._GAP_REFS):
            jobs = plan_jobs(cfg, "test-bucket", date(2019, 2, 23), date(2019, 3, 7), False)
        self.assertNotIn("2019-02-28", [job["date"] for job in jobs])

    @patch(
        "planning.scan_existing_p3_mod_times",
        return_value={date(2019, 2, 28): datetime(2025, 1, 1, tzinfo=timezone.utc)},
    )
    def test_force_update_replans_interior_gap(self, _scan):
        cfg = get_source_config("GSFC")
        with self._patch_enumerator(self._GAP_REFS):
            jobs = plan_jobs(cfg, "test-bucket", date(2019, 2, 23), date(2019, 3, 7), True)
        gap_job = next(job for job in jobs if job["date"] == "2019-02-28")
        self.assertEqual(gap_job["granules"], [])

    @patch("planning.scan_existing_p3_mod_times", return_value={})
    def test_trailing_dates_without_granules_are_not_planned(self, _scan):
        """Upstream latency, not a real gap — writing empties here would churn grids."""
        cfg = get_source_config("GSFC")
        refs = [_ref(date(2025, 6, 30), "s3://b/last.nc")]
        with self._patch_enumerator(refs):
            jobs = plan_jobs(cfg, "test-bucket", date(2025, 6, 30), date(2025, 12, 31), False)
        self.assertEqual([job["date"] for job in jobs], ["2025-06-30"])

    @patch("planning.scan_existing_p3_mod_times", return_value={})
    def test_no_granules_at_all_plans_nothing(self, _scan):
        cfg = get_source_config("GSFC")
        with self._patch_enumerator([]):
            jobs = plan_jobs(cfg, "test-bucket", date(2019, 2, 24), date(2019, 3, 6), False)
        self.assertEqual(jobs, [])


class TestDatesToPlan(unittest.TestCase):
    def test_spans_start_through_last_granule_date(self):
        by_date = {date(2019, 2, 23): [], date(2019, 3, 7): []}
        planned = dates_to_plan(date(2019, 2, 21), date(2019, 3, 31), by_date)
        self.assertEqual(planned[0], date(2019, 2, 21))
        self.assertEqual(planned[-1], date(2019, 3, 7))
        self.assertEqual(len(planned), 15)

    def test_empty_by_date_plans_nothing(self):
        self.assertEqual(dates_to_plan(date(2019, 1, 1), date(2019, 12, 31), {}), [])

    def test_granule_dates_outside_span_are_retained(self):
        """A granule date must never be dropped, whatever the requested range."""
        by_date = {date(2018, 12, 31): [], date(2019, 1, 2): []}
        planned = dates_to_plan(date(2019, 1, 1), date(2019, 1, 2), by_date)
        self.assertEqual(planned, [date(2018, 12, 31), date(2019, 1, 1), date(2019, 1, 2)])
