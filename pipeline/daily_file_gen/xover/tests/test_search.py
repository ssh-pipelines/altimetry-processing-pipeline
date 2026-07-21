"""Synthetic tests for the pure reference-crossover search.

Exercises ``find_reference_crossovers`` (and its bracketing/interpolation) with
hand-built ``TrackWindow`` fixtures — no S3, no xarray, no real granules — so the
group-by-pass, tightest-bracket, time-interpolation, same-origin, and
single-sided-skip behaviours (decisions 4/7/8) are pinned deterministically.

Geometry: a high-lat seed pass runs diagonally up-right (8,64)->(12,66); each
reference pass runs down-right (8,66)->(12,64), so they cross at (10, 65). An
even sample count keeps the crossing strictly between samples (xover_ssh's
sign-change detector needs that).
"""
import unittest

import numpy as np
from crossover.config.source_config import get_source_config
from crossover.search import ReferenceCrossover, find_reference_crossovers

N = 20  # even: no sample sits exactly on the crossover
DAY = np.datetime64("2025-02-07")
BASE = np.datetime64("2025-02-07T00:00:00")
STEP = np.timedelta64(60, "s")

# Import here so a missing TrackWindow surfaces as an import error, not a skip.
from crossover.track_window import TrackWindow  # noqa: E402


def _hl_window(cycle=103, pass_=89, start=BASE, ssh=0.5):
    """One high-lat seed pass crossing (10, 65)."""
    lon = np.linspace(8, 12, N)
    lat = np.linspace(64, 66, N)
    time = start + np.arange(N) * STEP
    return TrackWindow.from_arrays(
        time, lon, lat, np.full(N, ssh), np.full(N, cycle), np.full(N, pass_)
    )


def _ref_pass(cycle, pass_, offset_days, ssh):
    """Arrays for one reference pass crossing (10, 65), offset from DAY."""
    lon = np.linspace(8, 12, N)
    lat = np.linspace(66, 64, N)
    time = BASE + np.timedelta64(offset_days, "D") + np.arange(N) * STEP
    return (
        time,
        lon,
        lat,
        np.full(N, ssh),
        np.full(N, cycle),
        np.full(N, pass_),
    )


def _ref_window(*passes):
    """Concatenate several ``_ref_pass`` tuples into one TrackWindow."""
    time, lon, lat, ssh, cyc, pas = (np.concatenate(a) for a in zip(*passes))
    return TrackWindow.from_arrays(time, lon, lat, ssh, cyc, pas)


class BracketInterpTestCase(unittest.TestCase):
    """A before/after pair of the same reference pass -> one interpolated record."""

    @classmethod
    def setUpClass(cls):
        cls.cfg = get_source_config("S3B")
        ref = _ref_window(
            _ref_pass(1192, 140, offset_days=-5, ssh=0.2),  # before
            _ref_pass(1193, 140, offset_days=5, ssh=0.4),  # after
        )
        cls.records = list(find_reference_crossovers(_hl_window(), ref, cls.cfg, DAY))

    def test_one_record(self):
        self.assertEqual(len(self.records), 1)

    def test_record_type(self):
        self.assertIsInstance(self.records[0], ReferenceCrossover)

    def test_crossover_location(self):
        r = self.records[0]
        self.assertAlmostEqual(r.lon, 10.0, places=6)
        self.assertAlmostEqual(r.lat, 65.0, places=6)

    def test_high_lat_side(self):
        r = self.records[0]
        self.assertEqual(r.cycle1, 103)
        self.assertEqual(r.pass1, 89)
        self.assertEqual(r.ssh1, 0.5)

    def test_reference_pass_and_bracket(self):
        r = self.records[0]
        self.assertEqual(r.pass2, 140)
        self.assertEqual(r.ref_cycle_before, 1192)
        self.assertEqual(r.ref_cycle_after, 1193)
        self.assertEqual(r.ref_ssha_before, 0.2)
        self.assertEqual(r.ref_ssha_after, 0.4)

    def test_ssh2_is_time_interpolated(self):
        # t_hl is equidistant between the two reference crossings -> midpoint.
        self.assertAlmostEqual(self.records[0].ssh2, 0.3, places=6)

    def test_bracket_straddles_high_lat_time(self):
        r = self.records[0]
        self.assertLess(r.ref_time_before, r.time1)
        self.assertGreaterEqual(r.ref_time_after, r.time1)


class TightestBracketTestCase(unittest.TestCase):
    """>2 cycles of one pass -> only the nearest before + nearest after are used."""

    @classmethod
    def setUpClass(cls):
        cls.cfg = get_source_config("S3B")
        ref = _ref_window(
            _ref_pass(1191, 140, offset_days=-10, ssh=0.0),  # far before (ignored)
            _ref_pass(1192, 140, offset_days=-2, ssh=0.2),  # near before
            _ref_pass(1193, 140, offset_days=2, ssh=0.4),  # near after
            _ref_pass(1194, 140, offset_days=10, ssh=9.9),  # far after (ignored)
        )
        cls.records = list(find_reference_crossovers(_hl_window(), ref, cls.cfg, DAY))

    def test_one_record(self):
        self.assertEqual(len(self.records), 1)

    def test_uses_nearest_bracket(self):
        r = self.records[0]
        self.assertEqual(r.ref_cycle_before, 1192)
        self.assertEqual(r.ref_cycle_after, 1193)

    def test_ssh2_from_nearest_only(self):
        # Equidistant nearest bracket (±2d) -> midpoint of 0.2 and 0.4.
        self.assertAlmostEqual(self.records[0].ssh2, 0.3, places=6)


class SingleSidedSkipTestCase(unittest.TestCase):
    """All crossings of a pass on one side of t_hl -> skipped (no extrapolation)."""

    @classmethod
    def setUpClass(cls):
        cls.cfg = get_source_config("S3B")

    def test_only_before_skipped(self):
        ref = _ref_window(
            _ref_pass(1191, 140, offset_days=-8, ssh=0.1),
            _ref_pass(1192, 140, offset_days=-2, ssh=0.2),
        )
        records = list(find_reference_crossovers(_hl_window(), ref, self.cfg, DAY))
        self.assertEqual(records, [])

    def test_only_after_skipped(self):
        ref = _ref_window(
            _ref_pass(1193, 140, offset_days=2, ssh=0.3),
            _ref_pass(1194, 140, offset_days=8, ssh=0.4),
        )
        records = list(find_reference_crossovers(_hl_window(), ref, self.cfg, DAY))
        self.assertEqual(records, [])


class SameOriginTestCase(unittest.TestCase):
    """A pass number shared across two contributing missions never brackets across
    the origin gap (decision 8) — cycle numbers thousands apart are split."""

    @classmethod
    def setUpClass(cls):
        cls.cfg = get_source_config("S3B")

    def test_cross_origin_pair_not_formed(self):
        # Same pass 140, but one crossing from a GSFC-range cycle and one from an
        # S6-range cycle: the huge cycle gap splits them, so neither side has a
        # same-origin partner -> no record.
        ref = _ref_window(
            _ref_pass(300, 140, offset_days=-3, ssh=0.2),  # GSFC-range origin
            _ref_pass(9300, 140, offset_days=3, ssh=0.4),  # S6-range origin
        )
        records = list(find_reference_crossovers(_hl_window(), ref, self.cfg, DAY))
        self.assertEqual(records, [])

    def test_same_origin_pair_still_brackets(self):
        # Two adjacent same-origin cycles bracket normally even with an unrelated
        # far-origin crossing present.
        ref = _ref_window(
            _ref_pass(1192, 140, offset_days=-2, ssh=0.2),
            _ref_pass(1193, 140, offset_days=2, ssh=0.4),
            _ref_pass(9300, 140, offset_days=1, ssh=5.0),  # other origin, single-sided
        )
        records = list(find_reference_crossovers(_hl_window(), ref, self.cfg, DAY))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].ref_cycle_before, 1192)
        self.assertEqual(records[0].ref_cycle_after, 1193)


class MultiPassTestCase(unittest.TestCase):
    """Distinct reference pass numbers each yield their own record."""

    @classmethod
    def setUpClass(cls):
        cls.cfg = get_source_config("S3B")
        ref = _ref_window(
            _ref_pass(1192, 140, offset_days=-2, ssh=0.2),
            _ref_pass(1193, 140, offset_days=2, ssh=0.4),
            _ref_pass(1192, 141, offset_days=-2, ssh=1.2),
            _ref_pass(1193, 141, offset_days=2, ssh=1.4),
        )
        cls.records = list(find_reference_crossovers(_hl_window(), ref, cls.cfg, DAY))

    def test_two_records(self):
        self.assertEqual(len(self.records), 2)

    def test_emitted_in_ascending_pass_order(self):
        self.assertEqual([r.pass2 for r in self.records], [140, 141])


class EmptyReferenceTestCase(unittest.TestCase):
    """No reference passes -> no records."""

    def test_empty(self):
        cfg = get_source_config("S3B")
        empty = np.array([])
        ref = TrackWindow.from_arrays(
            empty.astype("datetime64[ns]"), empty, empty, empty, empty, empty
        )
        records = list(find_reference_crossovers(_hl_window(), ref, cfg, DAY))
        self.assertEqual(records, [])


if __name__ == "__main__":
    unittest.main()
