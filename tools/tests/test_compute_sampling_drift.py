"""Unit tests for tools/compute_sampling_drift.py.

Build days from known step sequences (constant, linearly drifting, two
alternating rates, gaps of whole and fractional periods) and assert the
per-pass statistics, the samples near an ambiguous gap, the two checks against
the count-based smoother, and the verdict. Assert that passes split across
daily files are reassembled, and that duplicate timestamps, all-gap passes, and
a constant step do not raise.
"""
import os
import sys
import unittest

import numpy as np

# tools/ is the parent of this tests/ dir.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from compute_sampling_drift import (  # noqa: E402
    AMBIGUOUS_SAMPLE_CYCLE_TOLERANCE,
    AMBIGUOUS_SAMPLE_TOLERANCE,
    FILTER_WIDTH_TOLERANCE,
    PassStats,
    ambiguous_window_samples,
    day_arrays,
    evaluate,
    iqr_trend_label,
    iter_passes,
    max_grid_misalignment,
    pass_stats,
    summarize,
    trend_per_year,
)

T0_NS = np.datetime64("2015-03-01T00:00:00", "ns").astype("int64")
ERS1_PERIOD_S = 0.980468


def _times(steps_s, t0_ns=T0_NS):
    """datetime64[ns] timestamps from a sequence of steps in seconds, built in
    integer nanoseconds so the steps survive at full precision."""
    offsets = np.concatenate([[0], np.cumsum(np.round(np.asarray(steps_s) * 1e9).astype("int64"))])
    return (t0_ns + offsets).astype("datetime64[ns]")


def _pass(steps_s, sample_period, cycle=1, pass_=1):
    arrays = day_arrays(_times(steps_s), sample_period)
    arrays.pop("order")
    return pass_stats(cycle, pass_, arrays, sample_period)


def _stats(cycle=1, pass_=1, start_day=0, median=1.0, iqr=1e-6, corr=0.0, width=0.0, n_amb=0, n_points=2001):
    return PassStats(
        cycle=cycle,
        pass_=pass_,
        start=np.datetime64("2015-01-01", "ns") + np.timedelta64(start_day, "D"),
        n_points=n_points,
        n_steps=n_points - 1,
        median_dt=median,
        iqr_dt=iqr,
        corr_position_dt=corr,
        local_period_p1=median * (1 - width),
        local_period_p99=median * (1 + width),
        n_window_gaps=10,
        n_ambiguous_gaps=1 if n_amb else 0,
        n_ambiguous_samples=n_amb,
        grid_misalignment=0.0,
    )


class AmbiguousWindowSamplesTestCase(unittest.TestCase):
    def test_marks_windows_that_span_the_gap(self):
        # 40 samples, 3 omitted after sample 19: slots 0..19, then 23..42.
        offsets = np.concatenate([np.arange(20), 23 + np.arange(20)])
        ambiguous = np.zeros(39, dtype=bool)
        ambiguous[19] = True
        marked = ambiguous_window_samples(offsets, ambiguous)
        # A window at slot o spans the gap when 23 - 9 <= o <= 19 + 9.
        expected = (offsets >= 14) & (offsets <= 28)
        np.testing.assert_array_equal(marked, expected)

    def test_none_ambiguous(self):
        offsets = np.arange(30)
        self.assertFalse(ambiguous_window_samples(offsets, np.zeros(29, dtype=bool)).any())


class PassStatsTestCase(unittest.TestCase):
    def test_constant_period(self):
        s = _pass(np.full(2500, 0.943437), 0.943437, cycle=193, pass_=14)
        self.assertAlmostEqual(s.median_dt, 0.943437, places=9)
        self.assertLess(s.iqr_dt, 1e-8)
        self.assertTrue(np.isnan(s.corr_position_dt))  # constant step: undefined
        self.assertAlmostEqual(s.local_period_p1, 0.943437, places=9)
        self.assertAlmostEqual(s.local_period_p99, 0.943437, places=9)
        self.assertEqual(s.n_ambiguous_samples, 0)
        self.assertEqual((s.cycle, s.pass_, s.n_points, s.n_steps), (193, 14, 2501, 2500))

    def test_whole_period_gaps_are_not_ambiguous(self):
        steps = np.full(1000, ERS1_PERIOD_S)
        steps[[200, 600]] = [ERS1_PERIOD_S * 3, ERS1_PERIOD_S * 8]
        s = _pass(steps, ERS1_PERIOD_S)
        self.assertEqual(s.n_steps, 998)
        self.assertEqual(s.n_window_gaps, 2)
        self.assertEqual(s.n_ambiguous_gaps, 0)
        self.assertEqual(s.n_ambiguous_samples, 0)

    def test_fractional_gap_is_ambiguous(self):
        """ERS-1 C0015 P0001's 8.202 s gap is 8.366 local periods (ADR 0011)."""
        steps = np.full(1000, ERS1_PERIOD_S)
        steps[500] = 8.202148
        s = _pass(steps, ERS1_PERIOD_S)
        self.assertEqual((s.n_window_gaps, s.n_ambiguous_gaps), (1, 1))
        # The far side is 8 slots on (7 omitted), so a +-9 window reaches across
        # only from the last 2 samples before the gap and the first 2 after it.
        self.assertEqual(s.n_ambiguous_samples, 4)

    def test_wide_gap_is_outside_the_window(self):
        steps = np.full(1000, 1.0)
        steps[500] = 132.4
        s = _pass(steps, 1.0)
        self.assertEqual((s.n_window_gaps, s.n_ambiguous_gaps, s.n_ambiguous_samples), (0, 0, 0))

    def test_linear_drift_shows_in_local_period_and_correlation(self):
        # Step rising 1.03 -> 1.05 s along the pass, about +-1% of its median.
        s = _pass(np.linspace(1.03, 1.05, 2500), 1.04)
        self.assertAlmostEqual(s.median_dt, 1.04, places=4)
        self.assertAlmostEqual(s.iqr_dt, 0.01, places=4)
        self.assertGreater(s.corr_position_dt, 0.99)
        self.assertAlmostEqual(s.local_period_p1 / s.median_dt - 1, -0.0095, places=3)
        self.assertAlmostEqual(s.local_period_p99 / s.median_dt - 1, 0.0095, places=3)

    def test_alternating_rates_stay_near_the_median(self):
        # ERS-1's two clock rates, 2.4e-4 s apart: the local median sits on one
        # or the other, both within 0.03% of the configured period.
        steps = np.tile(np.concatenate([np.full(200, 0.980468), np.full(200, 0.980224)]), 6)
        s = _pass(steps, ERS1_PERIOD_S)
        self.assertLess(s.iqr_dt, 3e-4)
        self.assertLess(abs(s.local_period_p1 / ERS1_PERIOD_S - 1), 3e-4)
        self.assertLess(abs(s.local_period_p99 / ERS1_PERIOD_S - 1), 3e-4)

    def test_unsorted_input(self):
        steps = np.linspace(1.03, 1.05, 500)
        arrays = day_arrays(_times(steps), 1.04)
        order = arrays.pop("order")
        expected = pass_stats(1, 1, arrays, 1.04)
        t = _times(steps)
        perm = np.random.RandomState(0).permutation(t.size)
        shuffled = day_arrays(t[perm], 1.04)
        self.assertTrue(np.array_equal(perm[shuffled.pop("order")], np.arange(t.size)))
        self.assertEqual(pass_stats(1, 1, shuffled, 1.04), expected)
        self.assertEqual(order.tolist(), list(range(t.size)))

    def test_duplicate_timestamps_only(self):
        arrays = day_arrays(np.full(5, T0_NS).astype("datetime64[ns]"), 1.0)
        arrays.pop("order")
        self.assertIsNone(pass_stats(1, 1, arrays, 1.0))

    def test_all_gaps(self):
        self.assertIsNone(_pass(np.full(10, 30.0), 1.0))

    def test_single_point(self):
        self.assertIsNone(_pass([], 1.0))


class GridMisalignmentTestCase(unittest.TestCase):
    """Kept for reference only; not part of the verdict."""

    def test_uniform_samples_are_aligned(self):
        t = _times(np.full(2000, 0.943437)).astype("int64")
        self.assertLess(max_grid_misalignment(t, 0.943437), 1e-6)

    def test_grid_offset_is_chosen_not_anchored_on_first_sample(self):
        # Phases 0.0 and 0.2: anchoring on the first sample gives 0.2, a grid
        # offset by 0.1 gives 0.1.
        self.assertAlmostEqual(max_grid_misalignment(np.array([0, int(1.2e9)]), 1.0), 0.1, places=9)

    def test_phases_around_the_circle(self):
        t = (np.arange(10) * 1.1e9).astype("int64")
        self.assertAlmostEqual(max_grid_misalignment(t, 1.0), 0.45, places=6)


class IterPassesTestCase(unittest.TestCase):
    def _chunk(self, parts):
        """One daily file from [(cycle, pass, time), ...]."""
        time = np.concatenate([t for _, _, t in parts])
        return (
            np.concatenate([np.full(t.size, c, dtype="int32") for c, _, t in parts]),
            np.concatenate([np.full(t.size, p, dtype="int32") for _, p, t in parts]),
            {"time": time, "index": np.arange(time.size)},
        )

    def test_pass_crossing_midnight_is_reassembled(self):
        whole = _times(np.full(999, 1.0))
        a = _times(np.full(99, 1.0), T0_NS - 200 * 10**9)
        c = _times(np.full(99, 1.0), T0_NS + 2000 * 10**9)
        day1 = self._chunk([(5, 1, a), (5, 2, whole[:400])])
        day2 = self._chunk([(5, 2, whole[400:]), (5, 3, c)])
        out = {(cy, ps): arr for cy, ps, arr in iter_passes([day1, day2])}
        self.assertEqual(sorted(out), [(5, 1), (5, 2), (5, 3)])
        np.testing.assert_array_equal(out[(5, 2)]["time"], whole)
        self.assertEqual(out[(5, 1)]["time"].size, 100)

    def test_closed_pass_is_released_before_next_file(self):
        day1 = self._chunk([(5, 1, _times(np.full(9, 1.0)))])
        day2 = self._chunk([(5, 2, _times(np.full(9, 1.0)))])
        seen = set()

        def files():
            yield day1
            yield day2
            self.assertIn((5, 1), seen)

        for cy, ps, _ in iter_passes(files()):
            seen.add((cy, ps))
        self.assertEqual(seen, {(5, 1), (5, 2)})

    def test_no_files(self):
        self.assertEqual(list(iter_passes([])), [])


class ChecksTestCase(unittest.TestCase):
    def test_filter_width_limit(self):
        ok = evaluate([_stats(width=FILTER_WIDTH_TOLERANCE * 0.99), _stats(width=0.0)])
        self.assertTrue(ok.filter_width_ok)
        bad = evaluate([_stats(width=FILTER_WIDTH_TOLERANCE * 1.01), _stats(width=0.0)])
        self.assertFalse(bad.filter_width_ok)

    def test_record_ambiguous_share_limit(self):
        n = 10000
        under = int(AMBIGUOUS_SAMPLE_TOLERANCE * n * 0.9)
        over = int(AMBIGUOUS_SAMPLE_TOLERANCE * n * 1.1)
        cycles = range(1, 20)  # spread so no single cycle breaches its own limit
        self.assertTrue(evaluate([_stats(cycle=c, n_amb=under, n_points=n) for c in cycles]).gap_counts_ok)
        self.assertFalse(evaluate([_stats(cycle=c, n_amb=over, n_points=n) for c in cycles]).gap_counts_ok)

    def test_worst_cycle_limit(self):
        # One bad cycle among clean ones: the record share stays low.
        n = 10000
        stats = [_stats(cycle=c, n_points=n) for c in range(1, 40)]
        stats.append(_stats(cycle=99, n_amb=int(AMBIGUOUS_SAMPLE_CYCLE_TOLERANCE * n * 1.1), n_points=n))
        checks = evaluate(stats)
        self.assertLess(checks.ambiguous_share, AMBIGUOUS_SAMPLE_TOLERANCE)
        self.assertEqual(checks.worst_cycle, 99)
        self.assertFalse(checks.gap_counts_ok)


class SummaryTestCase(unittest.TestCase):
    def test_trend_recovers_slope(self):
        stats = [_stats(start_day=d, median=1.04 + 1e-4 * d / 365.25) for d in range(0, 3650, 35)]
        start = np.array([s.start for s in stats])
        self.assertAlmostEqual(trend_per_year(start, np.array([s.median_dt for s in stats])), 1e-4, places=8)

    def test_trend_undefined_under_a_day(self):
        start = np.array([np.datetime64("2015-01-01T00", "ns"), np.datetime64("2015-01-01T06", "ns")])
        self.assertTrue(np.isnan(trend_per_year(start, np.array([1.0, 1.1]))))

    def test_iqr_trend_labels(self):
        self.assertEqual(iqr_trend_label(np.array([1e-2] * 9))[0], "stable")
        self.assertEqual(iqr_trend_label(np.array([1e-2] * 6 + [2e-2] * 3))[0], "growing")
        self.assertEqual(iqr_trend_label(np.array([2e-2] * 3 + [1e-2] * 6))[0], "shrinking")
        self.assertEqual(iqr_trend_label(np.array([1e-2, 1e-2]))[0], "insufficient")
        self.assertEqual(iqr_trend_label(np.zeros(9))[0], "insufficient")

    def test_passing_source(self):
        stats = [_stats(cycle=1 + d // 2, pass_=d, start_day=d, corr=float("nan")) for d in range(6)]
        text = "\n".join(summarize(stats, configured_period=1.0))
        self.assertIn("undefined: the step is constant in every pass", text)
        self.assertIn("filter width  PASS", text)
        self.assertIn("gap counts    PASS", text)
        self.assertNotIn("WARNING", text)
        self.assertTrue(text.endswith("Verdict: the count-based smoother holds; one sample_period is adequate"))

    def test_failing_source_and_configured_mismatch(self):
        stats = [_stats(start_day=0, median=1.04, width=0.03), _stats(start_day=1, median=1.04)]
        lines = summarize(stats, configured_period=1.0)
        text = "\n".join(lines)
        self.assertIn("WARNING: they differ", text)
        self.assertIn("filter width  FAIL", text)
        self.assertTrue(lines[-1].endswith("take it to science"))


if __name__ == "__main__":
    unittest.main()
