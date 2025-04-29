import warnings
import xarray as xr
import numpy as np

with warnings.catch_warnings():
    warnings.simplefilter("ignore", UserWarning)
    from pyresample.utils import check_and_wrap


class Pattern:
    def __init__(self, pattern: str) -> None:
        self.name = pattern
        self.pattern_ds = xr.open_dataset(f"ref_files/{pattern}_pattern_and_index.nc")
        self.pattern_ds = self.pattern_ds.rename(
            {"Latitude": "latitude", "Longitude": "longitude"}
        )
        self.pattern_field = self.pattern_ds[f"{self.name}_pattern"].values
        self.pattern_lons, self.pattern_lats = check_and_wrap(
            self.pattern_ds["longitude"].values, self.pattern_ds["latitude"].values
        )
        self.pattern_nns = ~np.isnan(self.pattern_ds[f"{self.name}_pattern"])

    def _get_ann_cyc(self) -> xr.Dataset:
        """
        Subsets annual cycle to pattern area
        """
        ann_ds = xr.open_dataset("ref_files/ann_pattern.nc")
        ann_ds = ann_ds.rename({"Latitude": "latitude", "Longitude": "longitude"})
        pattern_lats = self.pattern_ds["latitude"].values
        pattern_lons = self.pattern_ds["longitude"].values
        return ann_ds.sel(
            latitude=slice(pattern_lats[0], pattern_lats[-1]),
            longitude=slice(pattern_lons[0], pattern_lons[-1]),
        )
