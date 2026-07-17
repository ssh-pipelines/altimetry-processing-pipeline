"""Pure crossover search: TrackWindow(s) -> typed crossover records.

No I/O and no xarray. The self search reproduces the original
``search_day_for_crossovers`` loop exactly (seed tracks starting on the day,
pair each against different-cycle/opposite-direction candidates within one
orbital cycle, run ``xover_ssh``). Day-filtering and time-sorting are *not* done
here — they are output-shaping and belong to the accumulator.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator

import numpy as np
from crossover.config.source_config import SourceConfig
from crossover.track_window import TRACKID_CYCLE_STRIDE, EPOCH, TrackWindow
from crossover.xover_ssh import xover_ssh

ZERO_DIFF: np.timedelta64 = np.timedelta64(0, "ns")


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
