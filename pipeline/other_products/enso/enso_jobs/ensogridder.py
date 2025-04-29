import logging
import xarray as xr
import numpy as np
from datetime import datetime
from netCDF4 import default_fillvals
import warnings

from enso_jobs.smoother import new_smoother

warnings.filterwarnings("ignore")


class ENSOGridder:
    def __init__(self):
        self.seas_ds: xr.Dataset = self.init_season()
        self.padded_seas_ds: xr.Dataset = self.init_padded_season()
        self.mask: np.ndarray = self.init_mask()

    @staticmethod
    def get_decimal_year(dt: datetime) -> float:
        year_start = datetime(dt.year, 1, 1)
        year_end = datetime(dt.year + 1, 1, 1)
        seconds_so_far = (dt - year_start).total_seconds()
        seconds_in_year = float((year_end - year_start).total_seconds())
        return dt.year + (seconds_so_far / seconds_in_year)

    @staticmethod
    def init_season() -> xr.Dataset:
        seas_ds = xr.open_dataset("enso_jobs/ref_files/trnd_seas_simple_grid.nc")
        seas_ds.coords["Longitude"] = seas_ds.coords["Longitude"] % 360
        seas_ds = seas_ds.sortby(seas_ds.Longitude)
        seas_ds = seas_ds.rename({"Latitude": "latitude", "Longitude": "longitude"})
        return seas_ds

    def init_padded_season(self) -> xr.Dataset:
        front_seas_ds = self.seas_ds.isel(Month_grid=0)
        back_seas_ds = self.seas_ds.isel(Month_grid=-1)
        front_seas_ds = front_seas_ds.assign_coords(
            {"Month_grid": front_seas_ds.Month_grid.values + (12 / 12)}
        )
        back_seas_ds = back_seas_ds.assign_coords(
            {"Month_grid": back_seas_ds.Month_grid.values - (12 / 12)}
        )
        padded_seas_ds = xr.concat(
            [back_seas_ds, self.seas_ds, front_seas_ds], dim="Month_grid"
        )
        return padded_seas_ds

    @staticmethod
    def init_mask() -> np.ndarray:
        mask_ds = xr.open_dataset("enso_jobs/ref_files/new_basin_mask_quartdeg.nc")
        mask = (mask_ds.basinmask.values > 0) & (mask_ds.basinmask.values < 1000)
        return mask

    @staticmethod
    def save_grid(ds: xr.Dataset, outpath: str):
        var_encoding = {
            "zlib": True,
            "complevel": 5,
            "dtype": "float32",
            "shuffle": True,
            "_FillValue": default_fillvals["f8"],
        }
        encoding = {var: var_encoding for var in ds.data_vars}
        encoding["time"] = {"units": "days since 1985-01-01"}
        ds.to_netcdf(outpath, encoding=encoding)

    def remove_cycle_trend(self, da: xr.DataArray, date: datetime) -> xr.DataArray:
        decimal_year = self.get_decimal_year(date)
        cycle_ds = self.padded_seas_ds.interp({"Month_grid": decimal_year - date.year})

        removed_cycle_data = da * 1000 - cycle_ds.Seasonal_SSH * 10
        trend = (self.seas_ds.SSH_Slope * 10 * decimal_year) + (
            self.seas_ds.SSH_Offset * 10
        )
        removed_cycle_trend_data = removed_cycle_data - trend
        removed_cycle_trend_data.name = "ssha"
        return removed_cycle_trend_data

    @staticmethod
    def pad_longitudes(da: xr.DataArray) -> xr.Dataset:
        front = da.sel(longitude=slice(0, 1))
        back = da.sel(longitude=slice(359, 360))
        front = front.assign_coords({"longitude": front.longitude.values + 360})
        back = back.assign_coords({"longitude": back.longitude.values - 360})
        padded_ds = xr.merge([back, da, front])
        return padded_ds

    def interp_deg(self, ds: xr.Dataset, degree: float) -> xr.Dataset:
        new_lats = np.arange(-89.875, 90.125, degree)
        new_lons = np.arange(0.125, 360, degree)
        return (
            ds.interp(longitude=new_lons, latitude=new_lats)
            .interpolate_na("longitude", limit=1)
            .interpolate_na("latitude", limit=1)
        )

    def process_grid(self, ds: xr.Dataset, date: datetime) -> xr.Dataset:
        """
        1. Smooth
        2. Remove seasonal cycle and trend
        3. Pad longitudes
        4. Interpolate to 1/4 degree grid
        """

        ds = ds.drop_vars(
            [
                var
                for var in ds.data_vars
                if var not in ["latitude", "longitude", "ssha", "time"]
            ]
        )

        smoothed_da = new_smoother(
            ds["ssha"].values, ds["latitude"].values, ds["longitude"].values
        )
        logging.info("Smoothed")
        smoothed_removed_da = self.remove_cycle_trend(smoothed_da, date)
        logging.info("Removed cycle and trend")
        padded_ds = self.pad_longitudes(smoothed_removed_da)
        logging.info("Padded longitudes")
        enso_ds = self.interp_deg(padded_ds, 0.25)
        logging.info("Interpolated to quarter degree")

        enso_ds = enso_ds.sel({"longitude": slice(0, 360)})
        enso_ds["ssha"] = enso_ds["ssha"].where(np.abs(enso_ds["ssha"].latitude) <= 66)
        enso_ds["ssha"] = enso_ds["ssha"].where(self.mask)

        enso_ds["ssha"].attrs = ds["ssha"].attrs
        enso_ds["ssha"].attrs.update(
            {
                "units": "mm",
                "valid_min": np.nanmin(enso_ds["ssha"].values),
                "valid_max": np.nanmax(enso_ds["ssha"].values),
                "summary": "Data gridded to 0.25 degree grid with seasonal cycle and trend removed",
            }
        )

        enso_ds["time"] = ds["time"]
        enso_ds.time.attrs = {"long_name": "time", "standard_name": "time"}
        enso_ds.latitude.attrs = {"long_name": "latitude", "standard_name": "latitude"}
        enso_ds.longitude.attrs = {
            "long_name": "longitude",
            "standard_name": "longitude",
        }

        filename = f'ENSO_{datetime.strftime(date, "%Y%m%d")}.nc'
        outpath = f"/tmp/{filename}"
        self.save_grid(enso_ds, outpath)
        return enso_ds
