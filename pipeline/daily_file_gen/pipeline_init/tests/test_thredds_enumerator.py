import unittest
from datetime import date, datetime, timezone
from unittest.mock import patch

from enumeration.thredds import _Granule, _granule_dates, _parse_filename


class TestParseFilename(unittest.TestCase):
    def test_parses_cycle_pass_and_dates(self):
        name = "global_sla_l2p_ntc_e1_C0050_P0003_19960115T000000_19960115T003519_20240305T133008.nc.gz"
        parsed = _parse_filename(name)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["cycle"], 50)
        self.assertEqual(parsed["pass_number"], 3)
        self.assertEqual(parsed["data_start"], datetime(1996, 1, 15, 0, 0, 0, tzinfo=timezone.utc))
        self.assertEqual(parsed["data_end"], datetime(1996, 1, 15, 0, 35, 19, tzinfo=timezone.utc))
        self.assertEqual(parsed["processed_at"], datetime(2024, 3, 5, 13, 30, 8, tzinfo=timezone.utc))

    def test_returns_none_for_non_matching_filename(self):
        self.assertIsNone(_parse_filename("not_a_granule.nc"))


class TestGranuleDates(unittest.TestCase):
    def _granule(self, start: datetime, end: datetime) -> _Granule:
        return _Granule(
            granule_id="x",
            cycle=1,
            pass_number=1,
            data_start=start,
            data_end=end,
            processed_at=start,
            download_url="https://x",
        )

    def test_single_day_granule(self):
        g = self._granule(
            datetime(2023, 12, 17, 0, 0, tzinfo=timezone.utc),
            datetime(2023, 12, 17, 23, 30, tzinfo=timezone.utc),
        )
        dates = _granule_dates(g, date(2023, 12, 1), date(2023, 12, 31))
        self.assertEqual(dates, [date(2023, 12, 17)])

    def test_clipped_to_range(self):
        g = self._granule(
            datetime(2023, 12, 16, 23, 0, tzinfo=timezone.utc),
            datetime(2023, 12, 18, 1, 0, tzinfo=timezone.utc),
        )
        dates = _granule_dates(g, date(2023, 12, 17), date(2023, 12, 17))
        self.assertEqual(dates, [date(2023, 12, 17)])

    def test_multi_day_within_range(self):
        g = self._granule(
            datetime(2023, 12, 16, 23, 0, tzinfo=timezone.utc),
            datetime(2023, 12, 18, 1, 0, tzinfo=timezone.utc),
        )
        dates = _granule_dates(g, date(2023, 12, 1), date(2023, 12, 31))
        self.assertEqual(dates, [date(2023, 12, 16), date(2023, 12, 17), date(2023, 12, 18)])
