"""Pure crossover search: TrackWindow(s) -> typed crossover records.

No I/O and no xarray. The self search reproduces the original
``search_day_for_crossovers`` loop exactly (seed tracks starting on the day,
pair each against different-cycle/opposite-direction candidates within one
orbital cycle, run ``xover_ssh``). The reference search crosses each high-lat
seed pass against *every* reference pass (brute-force n×m, no candidate
filters), groups the resulting crossings by reference pass number across
cycles, and time-interpolates the reference ssha through the tightest
before/after bracket to the high-lat crossover time (decisions 4/7/8).

Day-filtering and time-sorting are *not* done here — they are output-shaping
and belong to the accumulator.
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterator

import numpy as np
from crossover.config.source_config import SourceConfig
from crossover.track_window import EPOCH, TRACKID_CYCLE_STRIDE, TrackWindow
from crossover.xover_ssh import xover_ssh

ZERO_DIFF: np.timedelta64 = np.timedelta64(0, "ns")


def _to_datetime64(float_ns: float) -> np.datetime64:
    """Convert an ``xover_ssh`` float-ns time (since EPOCH) to datetime64[ns]."""
    return EPOCH + np.timedelta64(int(float_ns), "ns")


@dataclass
class SelfCrossover:
    """One self-crossover between two passes of the same source."""

    time1: np.datetime64
    time2: np.datetime64
    lon: float
    lat: float
    ssh1: float
    ssh2: float
    cycle1: int
    pass1: int
    cycle2: int
    pass2: int

    @classmethod
    def from_xover(cls, xcoords, xssh, xtime, trackid_1: int, trackid_2: int) -> "SelfCrossover":
        """Build a record from a non-empty ``xover_ssh`` result and the two trackids."""
        return cls(
            time1=EPOCH + np.timedelta64(int(xtime[0]), "ns"),
            time2=EPOCH + np.timedelta64(int(xtime[1]), "ns"),
            lon=xcoords[0],
            lat=xcoords[1],
            ssh1=xssh[0],
            ssh2=xssh[1],
            cycle1=trackid_1 // TRACKID_CYCLE_STRIDE,
            pass1=trackid_1 % TRACKID_CYCLE_STRIDE,
            cycle2=trackid_2 // TRACKID_CYCLE_STRIDE,
            pass2=trackid_2 % TRACKID_CYCLE_STRIDE,
        )


def _candidate_trackids(
    window: TrackWindow,
    trackid_1: int,
    start_1: np.datetime64,
    max_diff: np.timedelta64,
) -> np.ndarray:
    """Trackids that could cross ``trackid_1``: different cycle, opposite pass
    direction, starting after it but within one orbital cycle. Returned in
    ascending-trackid order (the order of ``window.unique_trackids``)."""
    unique = window.unique_trackids
    different_cycles = np.abs(trackid_1 - unique) > 1
    opposite_passes = (trackid_1 % 2) != (unique % 2)
    starts_diff = window.starts - start_1
    within_window = (starts_diff <= max_diff) & (starts_diff > ZERO_DIFF)
    return unique[different_cycles & opposite_passes & within_window]


def find_self_crossovers(window: TrackWindow, cfg: SourceConfig, day: np.datetime64) -> Iterator[SelfCrossover]:
    """Yield self-crossovers for tracks starting on ``day``.

    Preserves the original emission order: seed tracks ascending by trackid, each
    paired against ascending candidate trackids.
    """
    max_diff = np.timedelta64(int(cfg.cycle_length * 86400000000000), "ns")

    for track_1 in window.tracks_on(day):
        time_1, lonlat_1, ssh_1 = track_1.data
        if time_1.size <= 1:
            continue

        for trackid_2 in _candidate_trackids(window, track_1.trackid, track_1.start_time, max_diff):
            time_2, lonlat_2, ssh_2 = window.track_data(trackid_2)
            if time_2.size <= 1:
                continue

            xcoords, xssh, xtime = xover_ssh(lonlat_1, lonlat_2, ssh_1, ssh_2, time_1, time_2)
            if np.size(xcoords) == 0:
                continue

            yield SelfCrossover.from_xover(xcoords, xssh, xtime, track_1.trackid, trackid_2)


@dataclass
class ReferenceCrossover:
    """One high-lat pass crossed against a reference-mission pass.

    ``ssh2`` is the reference ssha time-interpolated through the before/after
    bracket to the high-lat crossover time; the ``ref_*_before``/``ref_*_after``
    fields record that same-pass, two-cycle bracket explicitly (decision 5).
    ``dssh = ssh1 - ssh2`` is the derivable comparison quantity.
    """

    time1: np.datetime64  # high-lat crossover time
    lon: float
    lat: float
    ssh1: float  # high-lat ssha at the crossover
    cycle1: int
    pass1: int
    ssh2: float  # time-interpolated reference ssha at the high-lat crossover time
    pass2: int  # reference pass number (same for before & after)
    ref_cycle_before: int
    ref_time_before: np.datetime64
    ref_ssha_before: float
    ref_cycle_after: int
    ref_time_after: np.datetime64
    ref_ssha_after: float


@dataclass
class _RefCrossing:
    """One non-empty high-lat × reference-pass crossing, before bracketing."""

    ref_cycle: int
    ref_pass: int
    cycle1: int  # high-lat cycle
    pass1: int  # high-lat pass
    time1: np.datetime64  # high-lat crossover time (xover_ssh side 1)
    lon: float
    lat: float
    ssh1: float  # high-lat ssha at the crossover
    ref_time: np.datetime64  # reference crossover time (xover_ssh side 2)
    ref_ssha: float  # reference ssha at the crossover


def _split_by_origin(crossings: list, window_cycles: int) -> Iterator[list]:
    """Split a same-pass-number group into same-origin sub-groups (decision 8).

    NASA-SSH P3 is byte-copied from whichever mission contributed (GSFC before
    2026-01-01, S6 after), and those missions do not share cycle numbering. A
    centered window straddling the transition can therefore group two
    *non-co-located* observations under one pass number. Contributing missions
    sit at cycle numbers thousands apart, so a contiguity split with a generous
    gap threshold (the window span in cycles, +1 slack) cleanly separates
    origins while never splitting a genuine within-mission before/after pair
    (adjacent reference cycles differ by 1).
    """
    ordered = sorted(crossings, key=lambda c: c.ref_cycle)
    gap_threshold = window_cycles + 1
    group: list = [ordered[0]]
    for prev, cur in zip(ordered, ordered[1:]):
        if cur.ref_cycle - prev.ref_cycle > gap_threshold:
            yield group
            group = [cur]
        else:
            group.append(cur)
    yield group


def _bracket_and_interp(origin_group: list) -> ReferenceCrossover | None:
    """Tightest before/after bracket for one same-origin group, interpolated.

    Uses the latest crossing with ``ref_time < t_hl`` and the earliest with
    ``ref_time >= t_hl`` (decision 4, 2026-07-16), then linearly interpolates
    the reference ssha in time to the high-lat crossover time. Returns ``None``
    if all crossings fall on one side of ``t_hl`` (no extrapolation).

    ``t_hl`` (and the recorded high-lat side) is taken from the "after"
    crossing: because the reference mission repeats its ground track, every
    cycle crosses the fixed high-lat pass at essentially the same instant
    (< ~30 ms spread observed), so the anchor choice is numerically immaterial.
    """
    before = [c for c in origin_group if c.ref_time < c.time1]
    after = [c for c in origin_group if c.ref_time >= c.time1]
    if not before or not after:
        return None

    b = max(before, key=lambda c: c.ref_time)
    a = min(after, key=lambda c: c.ref_time)
    t_hl = a.time1

    tb = (b.ref_time - EPOCH) / np.timedelta64(1, "ns")
    ta = (a.ref_time - EPOCH) / np.timedelta64(1, "ns")
    tq = (t_hl - EPOCH) / np.timedelta64(1, "ns")
    ssh2 = float(np.interp(tq, [tb, ta], [b.ref_ssha, a.ref_ssha]))

    return ReferenceCrossover(
        time1=a.time1,
        lon=a.lon,
        lat=a.lat,
        ssh1=a.ssh1,
        cycle1=a.cycle1,
        pass1=a.pass1,
        ssh2=ssh2,
        pass2=a.ref_pass,
        ref_cycle_before=b.ref_cycle,
        ref_time_before=b.ref_time,
        ref_ssha_before=b.ref_ssha,
        ref_cycle_after=a.ref_cycle,
        ref_time_after=a.ref_time,
        ref_ssha_after=a.ref_ssha,
    )


def find_reference_crossovers(
    highlat_window: TrackWindow,
    reference_window: TrackWindow,
    cfg: SourceConfig,
    day: np.datetime64,
) -> Iterator[ReferenceCrossover]:
    """Yield reference-mission crossovers for high-lat tracks starting on ``day``.

    For each high-lat seed pass (``tracks_on(day)``, plus overhanging passes
    reassembled by the loader), run ``xover_ssh`` against *every* reference pass
    in the window — no candidate filters (decision 7). Group the non-empty
    crossings by reference pass number, split each group by origin (decision 8),
    and emit one record per same-origin before/after bracket (decisions 4/5),
    with the reference ssha time-interpolated to the high-lat crossover time.

    Seed tracks are iterated in ascending-trackid order; within a seed, groups
    are emitted in ascending reference-pass-number order for deterministic
    output before the accumulator's final time-sort.
    """
    # Number of distinct reference cycles a single pass can appear in across the
    # centered window — the gap threshold that separates contributing missions.
    window_cycles = max(1, math.ceil(cfg.window_size / cfg.cycle_length))

    for track_1 in highlat_window.tracks_on(day):
        time_1, lonlat_1, ssh_1 = track_1.data
        if time_1.size <= 1:
            continue

        crossings_by_pass: dict[int, list] = defaultdict(list)
        for ref_trackid in reference_window.unique_trackids:
            time_2, lonlat_2, ssh_2 = reference_window.track_data(ref_trackid)
            if time_2.size <= 1:
                continue

            xcoords, xssh, xtime = xover_ssh(lonlat_1, lonlat_2, ssh_1, ssh_2, time_1, time_2)
            if np.size(xcoords) == 0:
                continue

            ref_cycle = int(ref_trackid) // TRACKID_CYCLE_STRIDE
            ref_pass = int(ref_trackid) % TRACKID_CYCLE_STRIDE
            crossings_by_pass[ref_pass].append(
                _RefCrossing(
                    ref_cycle=ref_cycle,
                    ref_pass=ref_pass,
                    cycle1=track_1.cycle,
                    pass1=track_1.pass_,
                    time1=_to_datetime64(xtime[0]),
                    lon=xcoords[0],
                    lat=xcoords[1],
                    ssh1=xssh[0],
                    ref_time=_to_datetime64(xtime[1]),
                    ref_ssha=xssh[1],
                )
            )

        for ref_pass in sorted(crossings_by_pass):
            for origin_group in _split_by_origin(crossings_by_pass[ref_pass], window_cycles):
                record = _bracket_and_interp(origin_group)
                if record is not None:
                    yield record
