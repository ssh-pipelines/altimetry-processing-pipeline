"""Crossover orchestration by composition.

One ``CrossoverProcessor`` owns ``run()``; each ``crossover_type`` supplies a
``CrossoverSpec`` that plugs in the type-specific load/search/to_dataset steps.
The ``SPECS`` registry maps a ``crossover_type`` to its spec, and ``app.py``
picks by the source's config.

Data flow (self): ``spec.load`` (stream + load_track_window) -> ``spec.search``
(find_self_crossovers) -> accumulate (pack + filter_and_sort) -> ``spec.to_dataset``
-> save/upload.
"""

from __future__ import annotations

import logging
import os
from typing import Protocol

import numpy as np
import xarray as xr
from crossover.config.source_config import SourceConfig, get_source_config
from crossover.loader import load_track_window, stream_files
from crossover.results import (
    build_reference_dataset,
    build_self_dataset,
    filter_and_sort,
    pack_records,
)
from crossover.search import (
    ReferenceCrossover,
    SelfCrossover,
    find_reference_crossovers,
    find_self_crossovers,
)

from utilities.aws_utils import aws_manager
from utilities.pipeline_layout import crossover_key, s3_uri
from utilities.source_profile import get_source_profile


class CrossoverSpec(Protocol):
    """The type-specific half of a crossover run.

    ``load`` returns the window(s) the search needs; ``search`` returns typed
    records; ``record_cls`` drives the columnar packing; ``to_dataset`` shapes
    the packed, day-filtered, time-sorted columns into an ``xr.Dataset``.
    ``two_sided_filter`` is True when the load window extends before the
    processing day (so the day-filter must drop ``time1 < day`` too).
    """

    record_cls: type
    two_sided_filter: bool

    def load(self, processor: "CrossoverProcessor", bucket: str) -> tuple: ...

    def search(self, processor: "CrossoverProcessor", windows: tuple) -> list: ...

    def to_dataset(self, processor: "CrossoverProcessor", columns: dict) -> xr.Dataset: ...


class SelfSpec:
    """Self-crossover spec: one source's window crossed against itself."""

    record_cls = SelfCrossover
    two_sided_filter = False

    def load(self, processor: "CrossoverProcessor", bucket: str) -> tuple:
        streams = stream_files(
            processor.config,
            processor.window_start,
            processor.window_end,
            processor.df_version,
            bucket,
        )
        return (load_track_window(streams),)

    def search(self, processor: "CrossoverProcessor", windows: tuple) -> list:
        (window,) = windows
        return list(find_self_crossovers(window, processor.config, processor.day))

    def to_dataset(self, processor: "CrossoverProcessor", columns: dict) -> xr.Dataset:
        return build_self_dataset(
            columns,
            processor.source,
            processor.day,
            processor.df_version,
            processor.window_start,
            processor.window_end,
            processor.config,
        )


class ReferenceSpec:
    """Reference-mission spec: a high-lat source crossed against NASA-SSH P3.

    Loads two windows: the high-lat source over ``[D-1, D+1]`` (neighbor days so
    the loader reassembles passes straddling midnight), and the reference mission
    over a window *centered* on the processing day (``window_size``/
    ``window_padding`` reinterpreted as ±), always at the finalized reference
    version regardless of the high-lat df_version (ADR-0006).
    """

    record_cls = ReferenceCrossover
    two_sided_filter = True

    def load(self, processor: "CrossoverProcessor", bucket: str) -> tuple:
        one_day = np.timedelta64(1, "D")
        hl_streams = stream_files(
            processor.config,
            processor.day - one_day,
            processor.day + one_day,
            processor.df_version,
            bucket,
        )
        half = np.timedelta64(
            processor.config.window_size // 2 + processor.config.window_padding, "D"
        )
        ref_streams = stream_files(
            processor.reference_profile,
            processor.day - half,
            processor.day + half,
            processor.config.reference_version,
            bucket,
        )
        return (load_track_window(hl_streams), load_track_window(ref_streams))

    def search(self, processor: "CrossoverProcessor", windows: tuple) -> list:
        highlat_window, reference_window = windows
        return list(
            find_reference_crossovers(
                highlat_window, reference_window, processor.config, processor.day
            )
        )

    def to_dataset(self, processor: "CrossoverProcessor", columns: dict) -> xr.Dataset:
        return build_reference_dataset(
            columns,
            processor.source,
            processor.day,
            processor.df_version,
            processor.window_start,
            processor.window_end,
            processor.config,
        )


SPECS: dict[str, CrossoverSpec] = {
    "self": SelfSpec(),
    "reference": ReferenceSpec(),
}


class CrossoverProcessor:
    """Orchestrates a crossover run for one (day, source, df_version).

    Owns the shared processing state (day, window bounds, config) and delegates
    the type-specific steps to its ``CrossoverSpec``.
    """

    def __init__(self, day: np.datetime64, source: str, df_version: str, spec: CrossoverSpec):
        self.day: np.datetime64 = day
        self.next_day: np.datetime64 = day + np.timedelta64(1, "D")
        self.source: str = source
        self.df_version: str = df_version
        self.spec: CrossoverSpec = spec
        self.config: SourceConfig = get_source_config(source)

        if self.config.crossover_type == "reference":
            # Reference window is centered on the day (±); load the reference
            # mission's identity profile for its daily-file keys.
            self.reference_profile = get_source_profile(self.config.reference_source)
            half = np.timedelta64(
                self.config.window_size // 2 + self.config.window_padding, "D"
            )
            self.window_start = day - half
            self.window_end = day + half
        else:
            self.reference_profile = None
            self.window_start = day
            self.window_end = day + np.timedelta64(
                self.config.window_size + self.config.window_padding, "D"
            )

    def save_to_netcdf(self, ds: xr.Dataset, out_dir: str = "/tmp") -> str:
        """Save the dataset as a local NetCDF and return its path."""
        day_dt = self.day.astype("datetime64[D]").astype(object)
        key = crossover_key(self.source, day_dt, self.df_version)
        filename = key.rsplit("/", 1)[-1]
        local_output_path = os.path.join(out_dir, filename)
        logging.info(f"Saving netcdf to {local_output_path}")
        ds.to_netcdf(local_output_path, engine="h5netcdf")
        return local_output_path

    def upload_xover(self, local_path: str, bucket: str):
        """Upload the crossover NetCDF to the bucket."""
        day_dt = self.day.astype("datetime64[D]").astype(object)
        s3_output_path = s3_uri(bucket, crossover_key(self.source, day_dt, self.df_version))
        aws_manager.upload_obj(local_path, s3_output_path)

    def run(self, bucket: str):
        """Load window(s), search, accumulate, build the dataset, save and upload."""
        logging.info(f"Looking for {self.source} {self.day} {self.spec.record_cls.__name__}s...")

        windows = self.spec.load(self, bucket)
        records = self.spec.search(self, windows)

        columns = pack_records(records, self.spec.record_cls)
        day_bound = self.day if self.spec.two_sided_filter else None
        columns = filter_and_sort(columns, self.next_day, day_bound)

        ds = self.spec.to_dataset(self, columns)
        local_path = self.save_to_netcdf(ds)
        self.upload_xover(local_path, bucket)
        logging.info(f"Processing {self.source} {self.day} complete")
