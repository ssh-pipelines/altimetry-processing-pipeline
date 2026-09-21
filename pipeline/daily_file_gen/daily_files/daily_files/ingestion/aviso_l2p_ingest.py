import logging
from contextlib import ExitStack
from typing import Iterable, TextIO

import numpy as np
import xarray as xr

from daily_files.ingestion.ingest import IngestedData, Ingestor


class AvisoL2PIngestor(Ingestor):
    """Ingests AVISO L2P NetCDF pass files into the normalized IngestedData
    schema. cycle/pass come from per-file global attributes; inv_bar_cor is
    not present in L2P and is filled with NaN (written as fill) since L2P
    carries no separable inverse barometer to swap in for dac."""

    def ingest(
        self,
        file_objs: Iterable[TextIO],
        **kwargs,
    ) -> IngestedData:

        with ExitStack() as stack:
            opened: list[xr.Dataset] = []
            cycle_arrays: list[np.ndarray] = []
            pass_arrays: list[np.ndarray] = []

            for i, file_obj in enumerate(file_objs):
                try:
                    ds = stack.enter_context(xr.open_dataset(file_obj, engine="h5netcdf"))
                except Exception as e:
                    logging.warning(f"Unable to open AVISO L2P file {i}: {e}")
                    continue

                n = ds.sizes["time"]
                cycle_num = int(ds.attrs.get("cycle_number", -1))
                pass_num = int(ds.attrs.get("pass_number", -1))
                cycle_arrays.append(np.full(n, cycle_num, dtype=np.int32))
                pass_arrays.append(np.full(n, pass_num, dtype=np.int32))
                opened.append(ds)

            if not opened:
                raise RuntimeError("No AVISO L2P files could be opened")

            ds = xr.concat(opened, dim="time")
            order = np.argsort(ds["time"].values)
            ds = ds.isel(time=order)

            cycles = np.concatenate(cycle_arrays)[order]
            passes = np.concatenate(pass_arrays)[order]

            ssha = ds["sea_level_anomaly"].values.astype(np.float64)
            dac = ds["dynamic_atmospheric_correction"].values.astype(np.float64)
            inv_bar_cor = np.full_like(ssha, np.nan, dtype=np.float64)
            lats = ds["latitude"].values
            lons = ds["longitude"].values
            times = ds["time"].values
            mean_sea_surface = ds["mean_sea_surface"].values
            inter_mission_bias = ds["inter_mission_bias"].values
            validation_flag = ds["validation_flag"].values

        return IngestedData(
            ssha=ssha,
            lat=lats,
            lon=lons,
            time=times,
            cycles=cycles,
            passes=passes,
            dac=dac,
            inv_bar_cor=inv_bar_cor,
            source_specific={
                "mean_sea_surface": mean_sea_surface,
                "inter_mission_bias": inter_mission_bias,
                "validation_flag": validation_flag,
            },
        )
