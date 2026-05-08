import logging
from typing import Iterable, TextIO

import numpy as np
import xarray as xr

from daily_files.ingestion.ingest import IngestedData, Ingestor


class AvisoL2PIngestor(Ingestor):
    """Ingests AVISO L2P NetCDF pass files into the normalized IngestedData
    schema. cycle/pass come from per-file global attributes; inv_bar_cor is
    not present in L2P and is filled with zeros."""

    def ingest(
        self,
        file_objs: Iterable[TextIO],
        **kwargs,
    ) -> IngestedData:
        opened: list[xr.Dataset] = []
        cycle_arrays: list[np.ndarray] = []
        pass_arrays: list[np.ndarray] = []

        for i, file_obj in enumerate(file_objs):
            try:
                ds = xr.open_dataset(file_obj, engine="h5netcdf")
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
        inv_bar_cor = np.zeros_like(ssha, dtype=np.float64)

        return IngestedData(
            ssha=ssha,
            lat=ds["latitude"].values,
            lon=ds["longitude"].values,
            time=ds["time"].values,
            cycles=cycles,
            passes=passes,
            dac=dac,
            inv_bar_cor=inv_bar_cor,
            source_specific={
                "original_ds": ds,
                "mean_sea_surface": ds["mean_sea_surface"].values,
                "inter_mission_bias": ds["inter_mission_bias"].values,
                "validation_flag": ds["validation_flag"].values,
            },
        )
