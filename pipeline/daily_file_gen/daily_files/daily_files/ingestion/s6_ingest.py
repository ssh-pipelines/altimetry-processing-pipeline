import logging
from typing import Iterable, TextIO

import netCDF4 as nc
import numpy as np
import xarray as xr

from daily_files.ingestion.ingest import IngestedData, Ingestor


class S6Ingestor(Ingestor):
    def ingest(self, file_objs: Iterable[TextIO], **kwargs) -> IngestedData:
        logging.info(f"Opening {len(file_objs)} files")

        opened_files = []
        for i, file_obj in enumerate(file_objs):
            try:
                ds = self._extract_grouped_data(file_obj)
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
            source_specific={
                "original_ds": ds,
                "mean_sea_surface_sol1": ds["mean_sea_surface_sol1"].values,
                "mean_sea_surface_sol2": ds["mean_sea_surface_sol2"].values,
            },
        )

    def _extract_grouped_data(self, file_obj: TextIO) -> xr.Dataset:
        """
        Use the netCDF4 library to efficiently open and extract grouped variables
        """
        ds = nc.Dataset("file_like", "r", memory=file_obj.read())

        das = []

        for var in [
            "latitude",
            "longitude",
            "surface_classification_flag",
            "rain_flag_nr",
            "rad_water_vapor_qual",
            "dac",
            "mean_sea_surface_sol1",
            "mean_sea_surface_sol2",
        ]:
            nc_var = ds.groups["data_01"].variables[var]
            nc_var_data = nc_var[:]
            nc_var_attrs = {
                k: v for k, v in nc_var.__dict__.items() if k != "scale_factor"
            }
            da = xr.DataArray(nc_var_data, dims="time", attrs=nc_var_attrs, name=var)
            das.append(da)

        for var in ["sig0_ocean_nr", "range_ocean_nr_qual", "swh_ocean_nr", "ssha_nr"]:
            nc_var = ds.groups["data_01"].groups["ku"].variables[var]
            nc_var_data = nc_var[:]
            nc_var_attrs = {
                k: v for k, v in nc_var.__dict__.items() if k != "scale_factor"
            }
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
        merged_ds.attrs = {
            k: v
            for k, v in ds.__dict__.items()
            if k != "scale_factor" and k != "add_offset"
        }
        merged_ds["cycle"] = (
            ("time"),
            np.full(merged_ds["time"].values.shape, ds.cycle_number),
        )
        merged_ds["passes"] = (
            ("time"),
            np.full(merged_ds["time"].values.shape, ds.pass_number),
        )
        return xr.decode_cf(merged_ds)
