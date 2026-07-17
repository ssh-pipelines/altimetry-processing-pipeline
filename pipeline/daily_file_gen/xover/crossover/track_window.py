"""Pure, in-memory model of a time-windowed set of tracks.

A ``TrackWindow`` owns the trackid grouping and the ``datetime64 <-> float-ns``
conversion that ``xover_ssh`` requires. It is built from plain arrays by the I/O
adapter (``loader.load_track_window``); it touches no S3 and no xarray, so the
crossover search can be exercised with synthetic arrays.

A ``Track`` is a lightweight view onto one (cycle, pass) half-orbit: it exposes
the ``xover_ssh``-ready coordinates/ssh/time for its rows without copying the
parent arrays until asked.
"""

from __future__ import annotations

from typing import Iterator, Tuple

import numpy as np

# Reference epoch for the float-ns time representation xover_ssh operates on.
EPOCH: np.datetime64 = np.datetime64("1990-01-01T00:00:00.000000")

# trackid = cycle * TRACKID_CYCLE_STRIDE + pass
TRACKID_CYCLE_STRIDE: int = 10000


class Track:
    """A view onto one (cycle, pass) half-orbit within a ``TrackWindow``.

    Holds only its parent window and the (time-sorted) row indices into the
    window's arrays; the ``xover_ssh``-ready arrays are materialised lazily and
    cached on the window so repeated access is free.
    """

    def __init__(self, window: "TrackWindow", trackid: int, start_time: np.datetime64):
        self._window = window
        self.trackid = int(trackid)
        self.start_time = start_time

    @property
    def cycle(self) -> int:
        return self.trackid // TRACKID_CYCLE_STRIDE

    @property
    def pass_(self) -> int:
        return self.trackid % TRACKID_CYCLE_STRIDE

    @property
    def data(self) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """``(time_float_ns, lonlat, ssh)`` for this track, cached on the window."""
        return self._window.track_data(self.trackid)


class TrackWindow:
    """A time-windowed set of Tracks, grouped by trackid.

    Built via :meth:`from_arrays`. Owns the unique-trackid ordering (ascending,
    from a lexsort on ``(time, trackid)``), each track's start time, the per-track
    row index (time-sorted within a track), and the float-ns conversion cache.
    """

    def __init__(
        self,
        time: np.ndarray,
        longitude: np.ndarray,
        latitude: np.ndarray,
        ssh: np.ndarray,
        trackids: np.ndarray,
        unique_trackids: np.ndarray,
        starts: np.ndarray,
        track_index: dict,
    ):
        self.time = time
        self.longitude = longitude
        self.latitude = latitude
        self.ssh = ssh
        self.trackids = trackids
        self.unique_trackids = unique_trackids
        self.starts = starts
        self.track_index = track_index
        self._track_data_cache: dict = {}

    @classmethod
    def from_arrays(
        cls,
        time: np.ndarray,
        longitude: np.ndarray,
        latitude: np.ndarray,
        ssh: np.ndarray,
        cycle: np.ndarray,
        pass_: np.ndarray,
    ) -> "TrackWindow":
        """Build a window from parallel per-observation arrays.

        Callers are responsible for NaN-masking upstream; this method takes the
        already-valid rows. Encodes trackids as ``cycle * 10000 + pass``, then
        computes the ascending unique-trackid order, per-track start times, and
        the time-sorted per-track row index.
        """
        longitude = longitude.astype(np.float64)
        latitude = latitude.astype(np.float64)
        ssh = ssh.astype(np.float64)
        trackids = cycle.astype("int32") * TRACKID_CYCLE_STRIDE + pass_

        if len(time) == 0:
            return cls(
                time=time,
                longitude=longitude,
                latitude=latitude,
                ssh=ssh,
                trackids=trackids,
                unique_trackids=np.array([], dtype="int32"),
                starts=np.array([], dtype="datetime64[ns]"),
                track_index={},
            )

        # Unique trackids (ascending) and their start times, via a lexsort keyed
        # first on time then trackid so each group's earliest time is its start.
        sort_idx = np.lexsort((time.view("i8"), trackids))
        sorted_trackids = trackids[sort_idx]
        sorted_time = time[sort_idx]

        boundaries = np.diff(sorted_trackids) != 0
        start_indices = np.concatenate([[0], np.where(boundaries)[0] + 1])

        unique_trackids = sorted_trackids[start_indices]
        starts = sorted_time[start_indices]

        track_index = cls._build_track_index(trackids, time)

        return cls(
            time=time,
            longitude=longitude,
            latitude=latitude,
            ssh=ssh,
            trackids=trackids,
            unique_trackids=unique_trackids,
            starts=starts,
            track_index=track_index,
        )

    @staticmethod
    def _build_track_index(trackids: np.ndarray, time: np.ndarray) -> dict:
        """Map each trackid to its row indices, sorted within the track by time.

        Time-ordering each group means ``track_data`` returns time-ordered data
        and xover_ssh's internal argsorts are near-free.
        """
        track_index: dict = {}
        sort_idx = np.argsort(trackids)
        sorted_trackids = trackids[sort_idx]

        changes = np.where(np.diff(sorted_trackids) != 0)[0] + 1
        starts = np.concatenate([[0], changes])
        ends = np.concatenate([changes, [len(sorted_trackids)]])

        for i, tid in enumerate(sorted_trackids[starts]):
            group_idx = sort_idx[starts[i] : ends[i]]
            time_order = np.argsort(time[group_idx])
            track_index[tid] = group_idx[time_order]
        return track_index

    def track_data(self, trackid: int) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """``(time_float_ns, lonlat, ssh)`` for a track, cached after first call."""
        cached = self._track_data_cache.get(trackid)
        if cached is not None:
            return cached
        idx = self.track_index[trackid]
        masked_time = (self.time[idx] - EPOCH).astype("timedelta64[ns]").astype("float64")
        masked_lonlat = np.column_stack((self.longitude[idx], self.latitude[idx]))
        masked_ssh = self.ssh[idx]
        result = (masked_time, masked_lonlat, masked_ssh)
        self._track_data_cache[trackid] = result
        return result

    def tracks(self) -> Iterator[Track]:
        """Yield every track in ascending-trackid order."""
        for tid, start in zip(self.unique_trackids, self.starts):
            yield Track(self, tid, start)

    def tracks_on(self, day: np.datetime64) -> Iterator[Track]:
        """Yield tracks that start on the processing ``day`` (start < day+1)."""
        next_day = day + np.timedelta64(1, "D")
        mask = self.starts < next_day
        for tid, start in zip(self.unique_trackids[mask], self.starts[mask]):
            yield Track(self, tid, start)
