"""Accumulate typed crossover records into a day-filtered, time-sorted dataset.

Shared, schema-driven packer: records -> columnar arrays -> filter to the
processing day -> sort by ``time1``. A per-type dataset-builder then turns those
arrays into an ``xr.Dataset`` with the type's schema and attributes.

Day-filter + sort live here (not in ``search``) because they are output-shaping,
not part of finding crossovers.
"""

from __future__ import annotations

from dataclasses import fields
from datetime import UTC, datetime

import numpy as np
import xarray as xr
from crossover.config.source_config import SourceConfig
from crossover.track_window import EPOCH


def pack_records(records: list, record_cls) -> dict:
    """Pack a list of dataclass records into a dict of numpy arrays by field name.

    Uses ``record_cls`` for the field order/schema so an empty list still yields
    the full set of (empty) columns.
    """
    field_names = [f.name for f in fields(record_cls)]
    columns = {name: [] for name in field_names}
    for record in records:
        for name in field_names:
            columns[name].append(getattr(record, name))
    return {name: np.array(values) for name, values in columns.items()}


def filter_and_sort(
    columns: dict, next_day: np.datetime64, day: np.datetime64 | None = None
) -> dict:
    """Keep rows whose ``time1`` falls in the processing day, then sort by ``time1``.

    Always drops ``time1 >= next_day``. When ``day`` is given, also drops
    ``time1 < day`` — a two-sided ``[day, next_day)`` filter needed when the
    load window extends *before* the processing day (the reference path loads
    ``D-1`` high-lat overhang to reassemble straddling passes). Omitting ``day``
    reproduces the original upper-bound-only behaviour; it is a no-op for the
    self path, whose forward-only window never yields ``time1 < day``.

    No-op on empty columns.
    """
    time1 = columns["time1"]
    if time1.size == 0:
        return columns

    mask = time1 < next_day
    if day is not None:
        mask &= time1 >= day
    columns = {name: values[mask] for name, values in columns.items()}

    sorted_indices = np.argsort(columns["time1"])
    return {name: values[sorted_indices] for name, values in columns.items()}


def build_self_dataset(
    columns: dict,
    source: str,
    day: np.datetime64,
    df_version: str,
    window_start: np.datetime64,
    window_end: np.datetime64,
    config: SourceConfig,
) -> xr.Dataset:
    """Create the self-crossover ``xr.Dataset`` from packed, sorted columns.

    Reproduces the original ``Crossover.create_dataset`` schema, attrs, and
    time encoding exactly.
    """
    ds = xr.Dataset(
        data_vars={k: ("time1", v) for k, v in columns.items() if k != "time1"},
        coords={"time1": ("time1", columns["time1"])},
        attrs={
            "title": f"{source} self-crossovers {day}",
            "window_length": f"{(window_end - window_start).astype('int32')} days "
            f"(nominal: {config.window_size} days + {config.window_padding} days padding)",
            "created_on": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S"),
            "input_product_generation_steps": df_version[-1],
            "satellite_names": source,
        },
    )
    ds["time2"].attrs = {"long_name": "Time of crossover in later pass"}
    ds["lon"].attrs = {"units": "degrees", "long_name": "Crossover longitude"}
    ds["lat"].attrs = {"units": "degrees", "long_name": "Crossover latitude"}
    ds["ssh1"].attrs = {"units": "m", "long_name": "SSH at crossover in earlier pass"}
    ds["ssh2"].attrs = {"units": "m", "long_name": "SSH at crossover in later pass"}
    ds["cycle1"].attrs = {"units": "N/A", "long_name": "Cycle number of earlier pass"}
    ds["cycle2"].attrs = {"units": "N/A", "long_name": "Cycle number of later pass"}
    ds["pass1"].attrs = {"units": "N/A", "long_name": "Pass number of earlier pass"}
    ds["pass2"].attrs = {"units": "N/A", "long_name": "Pass number of later pass"}

    ds["time1"].encoding["units"] = f"seconds since {EPOCH}"
    ds["time2"].encoding["units"] = f"seconds since {EPOCH}"
    return ds


def build_reference_dataset(
    columns: dict,
    source: str,
    day: np.datetime64,
    df_version: str,
    window_start: np.datetime64,
    window_end: np.datetime64,
    config: SourceConfig,
) -> xr.Dataset:
    """Create the reference-mission-crossover ``xr.Dataset`` from packed columns.

    Schema per decision 5: the high-lat side (``time1``/``lon``/``lat``/``ssh1``/
    ``cycle1``/``pass1``), the time-interpolated reference ``ssh2`` and its pass
    number ``pass2``, and the explicit same-pass before/after reference bracket.
    ``dssh = ssh1 - ssh2`` is derivable and not stored.
    """
    ds = xr.Dataset(
        data_vars={k: ("time1", v) for k, v in columns.items() if k != "time1"},
        coords={"time1": ("time1", columns["time1"])},
        attrs={
            "title": f"{source} reference-mission crossovers {day}",
            "reference_source": config.reference_source,
            "reference_version": config.reference_version,
            "window_length": f"{(window_end - window_start).astype('int32')} days "
            f"(nominal: {config.window_size} days centered "
            f"+ {config.window_padding} days padding)",
            "created_on": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S"),
            "input_product_generation_steps": df_version[-1],
            "satellite_names": f"{source}, {config.reference_source}",
        },
    )
    ds["lon"].attrs = {"units": "degrees", "long_name": "Crossover longitude"}
    ds["lat"].attrs = {"units": "degrees", "long_name": "Crossover latitude"}
    ds["ssh1"].attrs = {"units": "m", "long_name": "High-latitude SSH at crossover"}
    ds["cycle1"].attrs = {"units": "N/A", "long_name": "High-latitude cycle number"}
    ds["pass1"].attrs = {"units": "N/A", "long_name": "High-latitude pass number"}
    ds["ssh2"].attrs = {
        "units": "m",
        "long_name": "Reference SSH time-interpolated to the high-latitude crossover time",
    }
    ds["pass2"].attrs = {"units": "N/A", "long_name": "Reference pass number"}
    ds["ref_cycle_before"].attrs = {
        "units": "N/A",
        "long_name": "Reference cycle of the before-bracket crossover",
    }
    ds["ref_ssha_before"].attrs = {
        "units": "m",
        "long_name": "Reference SSH at the before-bracket crossover",
    }
    ds["ref_cycle_after"].attrs = {
        "units": "N/A",
        "long_name": "Reference cycle of the after-bracket crossover",
    }
    ds["ref_ssha_after"].attrs = {
        "units": "m",
        "long_name": "Reference SSH at the after-bracket crossover",
    }
    ds["ref_time_before"].attrs = {"long_name": "Time of the before-bracket reference crossover"}
    ds["ref_time_after"].attrs = {"long_name": "Time of the after-bracket reference crossover"}

    ds["time1"].encoding["units"] = f"seconds since {EPOCH}"
    ds["ref_time_before"].encoding["units"] = f"seconds since {EPOCH}"
    ds["ref_time_after"].encoding["units"] = f"seconds since {EPOCH}"
    return ds
