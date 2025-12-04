from dataclasses import dataclass
from datetime import datetime, timedelta
from io import TextIOWrapper
import logging
from typing import Iterable, Optional, Tuple
import warnings
import xarray as xr
import geopandas as gpd
import numpy as np

with warnings.catch_warnings():
    warnings.simplefilter("ignore", UserWarning)
    import pyresample as pr


class InsufficientData(Exception):
    pass


class Target:
    def __init__(self, resolution: Optional[str]) -> None:
        if resolution == "quart":
            mask_ds = xr.open_dataset("simple_gridder/ref_files/new_basin_mask_quartdeg.nc")
        else:
            mask_ds = xr.open_dataset("simple_gridder/ref_files/new_basin_mask_halfdeg.nc")
        self.basin_mask: np.ndarray = mask_ds["basinmask"].values
        self.lats: np.ndarray = mask_ds["lat"].values
        self.og_lons: np.ndarray = mask_ds["lon"].values
        self.lon_mesh, self.lat_mesh = np.meshgrid(*pr.utils.check_and_wrap(self.og_lons, self.lats))


class Source:
    def __init__(self, ds: xr.Dataset) -> None:
        if "ssha_smoothed" in ds:
            self.smssh = ds["ssha_smoothed"].values
        elif "ssh_smoothed" in ds:
            self.smssh = ds["ssh_smoothed"].values

        self.bflag = ds["basin_flag"].values
        self.lon, self.lat = pr.utils.check_and_wrap(ds["longitude"].values, ds["latitude"].values)


@dataclass
class basin_connection:
    id: int
    valid_basins: Iterable[int]


class Gridder:
    ROI: int = 6e5
    SIGMA: int = 175e3
    NEIGHBOURS: int = 500

    def __init__(
        self,
        center_date: datetime,
        start_date: datetime,
        end_date: datetime,
        filenames: Iterable[str],
        streamed_files: Iterable[TextIOWrapper],
        resolution: Optional[str],
    ) -> None:
        self.streamed_files = streamed_files
        self.center_date = center_date
        self.start_date = start_date
        self.end_date = end_date
        self.filenames = filenames
        self.basin_connections = self.load_basin_connections()
        self.resolution = resolution
        self.nnan_count = 0

    def load_basin_connections(self) -> Iterable[basin_connection]:
        basin_connections = []
        with open("simple_gridder/ref_files/basin_connection_table.txt", "r") as f:
            for line in f:
                i, valid_is = line.split(":")
                basin_connections.append(basin_connection(int(i), np.int16(valid_is.split(","))))
        return basin_connections

    def make_grid(self, filename: str) -> xr.Dataset:
        self.target = Target(self.resolution)

        try:
            merged_ds: xr.Dataset = self.merge_granules()
            self.source = Source(merged_ds)
            resampled_data, counts = self.gridding()
        except Exception as e:
            print(e)
            resampled_data = np.full(self.target.lon_mesh.shape, np.nan)
            counts = np.full(self.target.lon_mesh.shape, np.nan)

        grid_ds = self.make_ds(resampled_data, counts, filename)
        return grid_ds

    def merge_granules(self) -> xr.Dataset:
        threshold = 150000
        logging.info(f"Opening and merging {len(self.streamed_files)} files.")

        def preprocess(x: xr.Dataset):
            x = x.drop_dims("src_flag_dim")
            if "ssh_smoothed" in x.data_vars:
                x = x.rename_vars({"ssh_smoothed": "ssha_smoothed"})
                x = x.rename_vars({"ssh": "ssha"})
            return x

        cycle_ds = xr.open_mfdataset(self.streamed_files, concat_dim="time", preprocess=preprocess, combine="nested")
        cycle_ds = cycle_ds.sortby("time")

        nnan_count = cycle_ds["ssha_smoothed"].notnull().sum().compute().item()
        self.nnan_count = nnan_count

        if len(cycle_ds["time"].values) == 0:
            raise InsufficientData("All daily files are empty")

        if nnan_count < threshold:
            raise InsufficientData(
                f"Window contains {nnan_count} valid data points which is below {threshold}. Making empty grid."
            )

        return cycle_ds

    def gridding(self) -> Tuple[np.ndarray, np.ndarray]:
        """
        Performs gridding using pyresample's resample_gauss function.
        """
        logging.info("Performing Gaussian sampling")
        if np.isnan(self.source.smssh).all():
            logging.exception(f"No valid SSHA values for {str(self.center_date)}")
            raise ValueError(f"No valid SSHA values for {str(self.center_date)}")

        resampled_ssh = np.full_like(self.target.lon_mesh, np.nan)
        counts = np.full_like(self.target.lon_mesh, np.nan)

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore")
            for connection in self.basin_connections:
                if connection.id >= 1000:
                    continue
                mask_subset: np.ndarray = self.target.basin_mask == connection.id
                if ~mask_subset.any():
                    continue
                valid_indices = np.flatnonzero(
                    np.isin(self.source.bflag, connection.valid_basins) & (~np.isnan(self.source.smssh))
                )
                # if valid_indices.size <= 40:
                if valid_indices.size == 0:
                    continue
                target_grid = pr.geometry.SwathDefinition(
                    lons=self.target.lon_mesh[mask_subset],
                    lats=self.target.lat_mesh[mask_subset],
                )
                source_grid = pr.geometry.SwathDefinition(
                    lons=self.source.lon[valid_indices],
                    lats=self.source.lat[valid_indices],
                )
                resampled_ssh[mask_subset], _, counts[mask_subset] = pr.kd_tree.resample_gauss(
                    source_grid,
                    self.source.smssh[valid_indices],
                    target_grid,
                    self.ROI,
                    sigmas=self.SIGMA,
                    neighbours=self.NEIGHBOURS,
                    fill_value=np.NaN,
                    with_uncert=True,
                )
        return resampled_ssh, counts

    def make_ds(self, resampled_data: np.ndarray, counts: np.ndarray, filename: str) -> xr.Dataset:
        logging.info("Building netcdf from resampled arrays")
        ssha_dataarray = xr.DataArray(
            resampled_data,
            dims=("latitude", "longitude"),
            coords={"latitude": self.target.lats, "longitude": self.target.og_lons},
            attrs={
                "long_name": "sea surface height anomaly",
                "standard_name": "sea_surface_height_above_sea_level",
                "units": "m",
                "valid_min": -1e100,
                "valid_max": 1e100,
                "coverage_content_type": "physicalMeasurement",
                "comment": "Sea level determined from satellite altitude - range - all altimetric corrections",
                "summary": "Data gridded to 0.5 degree lat lon grid",
            },
        )
        if self.resolution == "quart":
            ssha_dataarray.attrs["summary"] = "Data gridded to 0.25 degree lat lon grid"

        counts_dataarray = xr.DataArray(
            counts,
            dims=("latitude", "longitude"),
            coords={"latitude": self.target.lats, "longitude": self.target.og_lons},
            attrs={
                "long_name": "number of data values used in weighting each element in SSHA",
                "valid_min": np.int32(0),
                "valid_max": np.int32(500),
                "overage_content_type": "auxiliaryInformation",
                "source": "Returned from pyresample resample_gauss function.",
            },
        )

        # Create mask DataArray
        mask_dataarray = xr.DataArray(
            self.target.basin_mask,
            dims=("latitude", "longitude"),
            coords={"latitude": self.target.lats, "longitude": self.target.og_lons},
            attrs={
                "reference": "Adapted from Natural Earth. Free vector and raster map data @ naturalearthdata.com",
                "long_name": "Basin ID number mapping to a geographic basin",
                "comment": "See basin_names_table for basin descriptions",
            },
        )

        # Create time DataArray
        time_dataarray = xr.DataArray(
            self.center_date,
            attrs={"long_name": "time", "standard_name": "time", "coverage_content_type": "coordinate"},
        )

        # Create dataset and assign variables and attributes
        ds = xr.Dataset(
            {"ssha": ssha_dataarray, "basin_flag": mask_dataarray, "counts": counts_dataarray, "time": time_dataarray}
        )

        poly_df = gpd.read_file("simple_gridder/ref_files/basin/new_basin_lake_polygons.shp")

        # Format basin ids and names for basin_names_table
        names = poly_df["name"].apply(lambda x: x.replace("'", " ").replace(",", " -")).values
        basin_ids = poly_df["feature_id"].astype(str).values
        basin_table = np.array([f"{basin},{name}" for basin, name in zip(basin_ids, names)])
        basin_table = np.insert(basin_table, 0, "0,Land", axis=0)
        ds["basin_names_table"] = (("basins"), np.array(basin_table).astype("unicode"))
        ds["basin_names_table"].attrs = {
            "long_name": "Table mapping basin ID numbers to basin names",
            "description": "Values are comma separated string of the form feature id,feature name",
            "note": "Some basins without widely known basin names are named with their basin number as Feature ID: XX, where XX is the basin number from basin_flag",
            "reference": "Adapted from Natural Earth. Free vector and raster map data @ naturalearthdata.com",
            "coverage_content_type": "auxiliaryInformation",
        }

        ds["basin_flag"].attrs["flag_values"] = np.array(basin_ids, dtype=np.int32)
        ds["basin_flag"].attrs["flag_meanings"] = " ".join(
            [name.replace(": ", ":").replace(" ", "_").replace(":", "_") for name in names]
        )

        # Set attributes for latitude and longitude
        ds["latitude"].attrs = {
            "long_name": "latitude",
            "standard_name": "latitude",
            "units": "degrees_north",
            "comment": "Positive latitude is North latitude, negative latitude is South latitude.",
            "coverage_content_type": "coordinate",
            "valid_min": -90.0,
            "valid_max": 90.0,
        }

        ds["longitude"].attrs = {
            "long_name": "longitude",
            "standard_name": "longitude",
            "units": "degrees_east",
            "comment": "East longitude relative to Greenwich meridian.",
            "coverage_content_type": "coordinate",
            "valid_min": 0.0,
            "valid_max": 360.0,
        }

        creation_time = datetime.now().isoformat(timespec="seconds")

        # Set global attributes
        ds.attrs["Conventions"] = "CF-1.7"
        ds.attrs["title"] = (
            "NASA-SSH Simple Gridded Sea Surface Height from Standardized Reference Missions Only Version 1"
        )
        ds.attrs["summary"] = (
            "This data set contains satellite based measurements of sea surface height, computed relative to the mean sea surface specified in mean_sea_surface. Data have been collected from multiple satellites, and processed to maximize compatibility and minimize bias between satellites. They are intended for use in studies and applications requiring climate-quality observations without additional adjustments or filtering."
        )
        ds.attrs["acknowledgement"] = "This data is provided by NASAs PO.DAAC."
        ds.attrs["license"] = "https://creativecommons.org/licenses/by/4.0/"
        ds.attrs["geospatial_lat_max"] = 90.0
        ds.attrs["geospatial_lat_min"] = -90.0
        ds.attrs["geospatial_lon_max"] = 360.0
        ds.attrs["geospatial_lon_min"] = 0.0
        ds.attrs["date_created"] = creation_time
        ds.attrs["history"] = f"Created on {creation_time}"
        ds.attrs["id"] = "10.5067/NSREF-SG0V1"
        ds.attrs["institution"] = "NASA/Jet Propulsion Laboratory"
        ds.attrs["instrument"] = "Altimeter"
        ds.attrs["keywords"] = "Earth Science, Oceans, Ocean Topography, Sea Surface Height, Sea Level"
        ds.attrs["keywords_vocabulary"] = "NASA Global Change Master Directory (GCMD) Science Keywords"
        ds.attrs["naming_authority"] = "gov.nasa.jpl.podaac"
        ds.attrs["platform"] = "Satellite"
        ds.attrs["processing_level"] = "Level 3"
        ds.attrs["product_short_name"] = "NASA_SSH_REF_SIMPLE_GRID_V1"
        ds.attrs["product_version"] = "V1"
        ds.attrs["project"] = "NASA-SSH"
        ds.attrs["publisher_name"] = "PO.DAAC"
        ds.attrs["publisher_url"] = "https://podaac.jpl.nasa.gov/"
        ds.attrs["publisher_email"] = "podaac@podaac.jpl.nasa.gov"
        ds.attrs["creator_name"] = "Josh Willis"
        ds.attrs["creator_url"] = "https://podaac.jpl.nasa.gov/NASA-SSH/"
        ds.attrs["creator_email"] = "podaac@podaac.jpl.nasa.gov"
        ds.attrs["references"] = "https://doi.org/10.5067/S6AP4-2LRST"
        ds.attrs["source_url"] = "https://podaac.jpl.nasa.gov/dataset/nasa_ssh_ref_alongtrack_v1"
        ds.attrs["standard_name_vocabulary"] = "CF Standard Name Table v86"
        ds.attrs["mean_sea_surface"] = "DTU21"
        ds.attrs["gridding_method"] = (
            f"Gridded using pyresample resample_gauss with roi={self.ROI}, sigma={self.SIGMA}, neighbours={self.NEIGHBOURS}, respecting basin boundaries as defined by the basin mask ID numbers and their connections."
        )
        ds.attrs["time_coverage_start"] = self.start_date.isoformat(timespec="seconds")
        ds.attrs["time_coverage_end"] = (self.end_date + timedelta(days=1)).isoformat(timespec="seconds")
        ds.attrs["source_files"] = ", ".join(self.filenames)
        ds.attrs["source_valid_points"] = self.nnan_count
        return ds
