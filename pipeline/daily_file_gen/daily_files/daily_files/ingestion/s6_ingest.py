import logging
import os
from typing import Iterable, TextIO

import netCDF4 as nc
import numpy as np
import xarray as xr

from daily_files.fetching.orbit_fetcher import OrbitFetcher
from daily_files.ingestion.ingest import IngestedData, Ingestor
from daily_files.ingestion.orbit_swap import run_orbit_swap


class S6Ingestor(Ingestor):
    def ingest(
        self,
        file_objs: Iterable[TextIO],
        filenames: list[str] | None = None,
        **kwargs,
    ) -> IngestedData:
        file_objs = list(file_objs)
        logging.info(f"Opening {len(file_objs)} files")

        orbit_fetcher = OrbitFetcher() if filenames else None
        names = filenames if filenames else [None] * len(file_objs)

        opened_files = []
        for i, (file_obj, filename) in enumerate(zip(file_objs, names)):
            try:
                ds = self._open_with_orbit_swap(file_obj, filename, orbit_fetcher)
                opened_files.append(ds)
            except Exception as e:
                logging.warning(f"Unable to open file object {i}: {e}")

        ds = xr.concat(opened_files, dim="time")

        return IngestedData(
            ssha=ds["ssha_nr"].values,
            lat=ds["latitude"].values,
            lon=ds["longitude"].values,
            time=ds["time"].values,
            cycles=ds["cycle"].values,
            passes=ds["passes"].values,
            dac=ds["dac"].values,
            inv_bar_cor=ds["inv_bar_cor"].values,
            source_specific={
                "original_ds": ds,
                "mean_sea_surface_sol1": ds["mean_sea_surface_sol1"].values,
                "mean_sea_surface_sol2": ds["mean_sea_surface_sol2"].values,
            },
        )

    def _open_with_orbit_swap(
        self,
        file_obj: TextIO,
        filename: str | None,
        orbit_fetcher: OrbitFetcher | None,
    ) -> xr.Dataset:
        """Read a pass file and apply orbit swap if filename + fetcher are provided.

        The raw bytes are read once and used both for the netCDF4 in-memory parse
        and (if orbit swap is requested) written to /tmp/ for the C executable.
        Falls back to the original ssha_nr if any step of the orbit swap fails.
        """
        data = file_obj.read()
        ds = self._extract_grouped_data(data)

        if filename is None or orbit_fetcher is None:
            return ds

        tmp_nc = f"/tmp/{filename}"
        try:
            with open(tmp_nc, "wb") as f:
                f.write(data)

            orbit_path = orbit_fetcher.fetch(filename)
            if orbit_path is None:
                logging.warning(f"No orbit file for {filename}, using original ssha_nr")
                return ds

            swapped = run_orbit_swap(tmp_nc, orbit_path)
            if swapped is None:
                logging.warning(
                    f"Orbit swap returned no data for {filename}, using original ssha_nr"
                )
                return ds

            if len(swapped) != len(ds["ssha_nr"]):
                logging.warning(
                    f"Orbit swap length mismatch for {filename} "
                    f"(expected {len(ds['ssha_nr'])}, got {len(swapped)}), "
                    "using original ssha_nr"
                )
                return ds

            ds["ssha_nr"] = xr.DataArray(swapped, dims="time", attrs=ds["ssha_nr"].attrs)
            logging.info(f"Orbit swap applied for {filename}")
        except Exception as e:
            logging.warning(
                f"Orbit swap error for {filename}: {e}. Using original ssha_nr."
            )
        finally:
            if os.path.exists(tmp_nc):
                os.remove(tmp_nc)

        return ds

    def _extract_grouped_data(self, data: bytes) -> xr.Dataset:
        """
        Use the netCDF4 library to efficiently open and extract grouped variables
        from in-memory bytes.
        """
        ds = nc.Dataset("file_like", "r", memory=data)

        das = []

        for var in [
            "latitude",
            "longitude",
            "surface_classification_flag",
            "rain_flag_nr",
            "rad_water_vapor_qual",
            "dac",
            "inv_bar_cor",
            "mean_sea_surface_sol1",
            "mean_sea_surface_sol2",
        ]:
            nc_var = ds.groups["data_01"].variables[var]
            nc_var_data = nc_var[:]
            nc_var_attrs = {k: v for k, v in nc_var.__dict__.items() if k != "scale_factor"}
            da = xr.DataArray(nc_var_data, dims="time", attrs=nc_var_attrs, name=var)
            das.append(da)

        for var in ["sig0_ocean_nr", "range_ocean_nr_qual", "swh_ocean_nr", "ssha_nr"]:
            nc_var = ds.groups["data_01"].groups["ku"].variables[var]
            nc_var_data = nc_var[:]
            nc_var_attrs = {k: v for k, v in nc_var.__dict__.items() if k != "scale_factor"}
            da = xr.DataArray(nc_var_data, dims="time", attrs=nc_var_attrs, name=var)
            das.append(da)

        merged_ds = xr.merge(das)
        merged_ds = merged_ds.set_coords(["latitude", "longitude"])
        merged_ds["time"] = ds.groups["data_01"].variables["time"][:]
        merged_ds["time"].attrs = {
            k: v
            for k, v in ds.groups["data_01"].variables["time"].__dict__.items()
            if k != "scale_factor" and k != "add_offset"
        }
        merged_ds.attrs = {k: v for k, v in ds.__dict__.items() if k != "scale_factor" and k != "add_offset"}
        merged_ds["cycle"] = (
            ("time"),
            np.full(merged_ds["time"].values.shape, ds.cycle_number),
        )
        merged_ds["passes"] = (
            ("time"),
            np.full(merged_ds["time"].values.shape, ds.pass_number),
        )
        return xr.decode_cf(merged_ds)
