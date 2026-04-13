from abc import ABC, abstractmethod
import logging
import os
import xarray as xr
import numpy as np
import geopandas as gpd
import shapely

from datetime import datetime, timedelta

from daily_files.config.paths import REF_FILES_DIR
from daily_files.config.source_config import SourceConfig
from daily_files.ingestion.ingest import IngestedData
from daily_files.processing.smoothing import ssha_smoothing


def get_base_global_attrs(source_files: str = "") -> dict:
    """Return the global attrs shared between DailyFile.set_global_attrs() and make_empty()."""
    creation_time = datetime.now().isoformat(timespec="seconds")
    return {
        "Conventions": "CF-1.9",
        "title": "NASA-SSH Along-Track Sea Surface Height from Standardized Reference Missions Version 1.1",
        "summary": (
            "This data set contains satellite based measurements of sea surface height, "
            "computed relative to the mean sea surface specified in mean_sea_surface. "
            "Data have been collected from multiple satellites, and processed to maximize "
            "compatibility and minimize bias between satellites. They are intended for use "
            "in studies and applications requiring climate-quality observations without "
            "additional adjustments or filtering."
        ),
        "institution": "NASA/Jet Propulsion Laboratory",
        "source": "",
        "source_url": "",
        "source_files": source_files,
        "date_created": creation_time,
        "history": f"Created on {creation_time}",
        "references": "",
        "standard_name_vocabulary": "CF Standard Name Table v86",
        "id": "10.5067/NSREF-AT0V11",
        "naming_authority": "gov.nasa.jpl.podaac",
        "project": "NASA-SSH",
        "processing_level": "Level 2",
        "product_generation_step": "1",
        "product_short_name": "NASA_SSH_REF_ALONGTRACK_V11",
        "acknowledgement": "This data is provided by NASAs PO.DAAC.",
        "license": "https://creativecommons.org/licenses/by/4.0/",
        "product_version": "V1.1",
        "keywords": "Earth Science, Oceans, Ocean Topography, Sea Surface Height, Sea Level",
        "keywords_vocabulary": "NASA Global Change Master Directory (GCMD) Science Keywords",
        "cdm_data_type": "Point",
        "featureType": "trajectory",
        "platform": "Satellite",
        "instrument": "Altimeter",
        "publisher_name": "PO.DAAC",
        "publisher_url": "https://podaac.jpl.nasa.gov/",
        "publisher_email": "podaac@podaac.jpl.nasa.gov",
        "creator_name": "Josh K. Willis",
        "creator_url": "https://podaac.jpl.nasa.gov/NASA-SSH/",
        "creator_email": "podaac@podaac.jpl.nasa.gov",
        "geospatial_lat_min": -90.0,
        "geospatial_lat_max": 90.0,
        "geospatial_lon_min": 0.0,
        "geospatial_lon_max": 360.0,
    }


def get_var_attrs(target_mss: str) -> dict:
    """Return variable attribute definitions. Used by DailyFile.set_var_attrs() and empty template generation."""
    return {
        "latitude": {
            "long_name": "latitude",
            "standard_name": "latitude",
            "units": "degrees_north",
            "coverage_content_type": "coordinate",
            "valid_min": np.float32(-90.0),
            "valid_max": np.float32(90.0),
        },
        "longitude": {
            "long_name": "longitude",
            "standard_name": "longitude",
            "units": "degrees_east",
            "coverage_content_type": "coordinate",
            "valid_min": np.float32(0.0),
            "valid_max": np.float32(360.0),
        },
        "time": {
            "long_name": "time",
            "standard_name": "time",
            "REFTime": "1990-01-01 00:00:00",
            "REFTime_comment": (
                "This string contains a time in the format yyyy-mm-dd HH:MM:SS "
                "to which all times in the time variable are referenced."
            ),
            "coverage_content_type": "coordinate",
        },
        "cycle": {
            "long_name": "Satellite cycle number",
            "coverage_content_type": "auxiliaryInformation",
        },
        "pass": {
            "long_name": "Satellite pass number",
            "coverage_content_type": "auxiliaryInformation",
        },
        "ssha": {
            "long_name": "Sea surface height anomaly relative to mean_sea_surface",
            "standard_name": "sea_surface_height_above_mean_sea_level",
            "mean_sea_surface": target_mss,
            "description": "Use nasa_flag = 0 to select valid data points from this variable",
            "units": "m",
            "coordinates": "latitude longitude",
            "coverage_content_type": "physicalMeasurement",
            "valid_min": -1e100,
            "valid_max": 1e100,
        },
        "ssha_smoothed": {
            "long_name": "Smoothed sea surface height anomaly relative to mean_sea_surface",
            "standard_name": "sea_surface_height_above_mean_sea_level",
            "mean_sea_surface": target_mss,
            "description": (
                "Smoothed sea surface height anomaly values computed using a 19 point filter. "
                "nasa_flag is applied prior to filter and should not be used to remove points from this field."
            ),
            "units": "m",
            "coordinates": "latitude longitude",
            "coverage_content_type": "physicalMeasurement",
            "valid_min": -1e100,
            "valid_max": 1e100,
        },
        "dac": {
            "long_name": "dynamic atmospheric correction",
            "comment": "Additive correction applied to ssha to remove atmospheric effects.  Subtract this field from ssha or ssha_smoothed to un-apply this correction.",
            "units": "m",
            "coordinates": "latitude longitude",
            "coverage_content_type": "auxiliaryInformation",
            "valid_min": -1e100,
            "valid_max": 1e100,
        },
        "inv_bar_cor": {
            "long_name": "inverse barometric correction",
            "comment": "Additive correction applied to ssha to remove inverse barometric effects.  Subtract this field from ssha or ssha_smoothed to un-apply this correction.",
            "units": "m",
            "coordinates": "latitude longitude",
            "coverage_content_type": "auxiliaryInformation",
            "valid_min": -1e100,
            "valid_max": 1e100,
        },
        "basin_flag": {
            "long_name": "Basin ID number mapping each observation to a geographic basin",
            "comment": "Also see basin_names_table for basin ID to basin name mapping",
            "reference": "Adapted from Natural Earth. Free vector and raster map data @ naturalearthdata.com",
            "coverage_content_type": "auxiliaryInformation",
        },
        "basin_names_table": {
            "long_name": "Table mapping basin ID numbers to basin names",
            "description": "Values are comma separated string of the form feature id,feature name",
            "note": "Some basins without widely known basin names are named with their basin number as Feature ID: XX, where XX is the basin number from basin_flag",
            "reference": "Adapted from Natural Earth. Free vector and raster map data @ naturalearthdata.com",
            "coverage_content_type": "auxiliaryInformation",
        },
        "nasa_flag": {
            "long_name": "NASA SSHA quality flag",
            "standard_name": "quality_flag",
            "flag_values": np.array([0, 1], dtype=np.int8),
            "flag_meanings": "good bad",
            "description": "Quality flag to be used for ssha, not for ssha_smoothed.",
            "coverage_content_type": "auxiliaryInformation",
        },
    }


class DailyFile(ABC):
    """
    Parent class for individual altimeter source data. Receives pre-extracted
    IngestedData and a SourceConfig for source-specific parameters.

    Individual subclasses will implement:
        make_daily_file_ds (defines sequence of processing)
        make_nasa_flag (creates boolean flag from source data flags)
        mss_swap (performs MSS swap on ssha)
        set_source_attrs (sets source-specific metadata)
    """

    def __init__(
        self,
        ingested_data: IngestedData,
        date: datetime,
        source_config: SourceConfig,
        collection_ids: list[str],
        source_files: str = "",
    ):
        self.date = date
        self.source_config = source_config
        self.collection_ids = collection_ids
        self.source_files = source_files
        self.source_mss = source_config.source_mss
        self.target_mss = source_config.target_mss
        self.mss_name = source_config.mss_diff_file

        self.time = ingested_data.time
        self.data = {
            "ssha": xr.DataArray(ingested_data.ssha, dims=["time"]),
            "dac": xr.DataArray(ingested_data.dac, dims=["time"]),
            "inv_bar_cor": xr.DataArray(ingested_data.inv_bar_cor, dims=["time"]),
            "latitude": xr.DataArray(ingested_data.lat, dims=["time"]),
            "longitude": xr.DataArray(ingested_data.lon, dims=["time"]),
            "cycle": xr.DataArray(ingested_data.cycles, dims=["time"]),
            "pass": xr.DataArray(ingested_data.passes, dims=["time"]),
        }

        self.ds = self.make_ds()

        self._pre_process_setup()
        self.make_daily_file_ds()

    def _pre_process_setup(self):
        """Hook for subclass-specific setup before processing begins.
        Override to add source-specific data to self.ds, etc."""
        pass

    def make_daily_file_ds(self):
        """Standard processing sequence for creating a daily file dataset."""
        self.map_points_to_basin()
        self.make_nasa_flag()
        self.clean_date(self.date)
        self.mss_swap()
        self.apply_basin_to_nasa()
        self.make_ssha_smoothed(self.date)
        self.set_metadata()
        self.set_source_attrs()

    @abstractmethod
    def make_nasa_flag(self):
        """Define the NASA flag variable. Source-specific."""
        raise NotImplementedError

    def mss_swap(self):
        """Performs MSS swap on ssha values using the configured MSS diff grid."""
        logging.info("Applying mss swap to ssha values...")
        if len(self.ds["time"]) == 0:
            logging.debug("Empty data arrays, skipping mss swapping")
            return
        mss_path = os.path.join(REF_FILES_DIR, "mss_diffs", self.mss_name)
        mss_correction = self.get_mss_values(mss_path)
        self.ds["ssha"].values = self.ds["ssha"].values + self._source_mss_correction() + mss_correction
        self._post_mss_swap()

    def _source_mss_correction(self) -> np.ndarray:
        """Override to add source-specific MSS correction terms. Default returns 0."""
        return 0.0

    def _post_mss_swap(self):
        """Hook called after MSS swap. Override for cleanup (e.g., dropping temp vars)."""
        pass

    def make_ds(self) -> xr.Dataset:
        ds = xr.Dataset(data_vars=self.data, coords=dict(time=self.time))
        ds["time"].encoding["units"] = "seconds since 1990-01-01"
        ds = ds.sortby("time")
        return ds

    def date_subset(self, ds: xr.Dataset, date: datetime) -> xr.Dataset:
        """
        Drop times outside of date
        """
        today = str(date)[:10]
        # For reasons still to be discovered, the where() function is required
        # before smoothing.

        basin_table = ds["basin_names_table"]
        ds = ds.drop_vars("basin_names_table")
        ds = ds.where(~np.isnat(ds["time"]), drop=True)
        ds = ds.sel(time=today)
        ds["basin_names_table"] = basin_table
        return ds

    def drop_dupe_times(self, ds: xr.Dataset) -> xr.Dataset:
        logging.debug("Dropping duplicate times")
        return ds.drop_duplicates(dim="time")

    def clean_date(self, date: datetime):
        """
        Subsets data to date, drops duplicate times and filters outliers
        """
        logging.info("Performing subsetting by date and filtering outlier values")
        self.ds = self.date_subset(self.ds, date)
        self.ds = self.drop_dupe_times(self.ds)

    def mss_interp(
        self,
        mss_lat: np.ndarray,
        mss_lon: np.ndarray,
        mss_diff: np.ndarray,
        lat: np.ndarray,
        lon: np.ndarray,
    ) -> np.ndarray:
        """
        perform bilinear interpolation of a 2-D gridded input field to a list input positions
        Function assumes the following:
            1) x & y are regularly spaced, monotonically increasing
            2) z has shape = (len(x), len(y))
            3) all values of xi are within the range of x
            4) all values of yi are within the range of y
            5) xi and yi are vectors of the same length
            6) all input arrays are numpy arrays
        """
        # get spacing for x and y
        delx = mss_lat[1] - mss_lat[0]
        dely = mss_lon[1] - mss_lon[0]

        # compute indices surrounding xi and yi
        xind1 = np.floor((lat - mss_lat[0]) / delx).astype(int)
        xind2 = xind1 + 1
        yind1 = np.floor((lon - mss_lon[0]) / dely).astype(int)
        yind2 = yind1 + 1

        # save z values at 4 locations surrounding each input point
        z1 = mss_diff[xind1, yind1]
        z2 = mss_diff[xind2, yind1]
        z3 = mss_diff[xind2, yind2]
        z4 = mss_diff[xind1, yind2]
        # compute weights for each of the z values
        w1 = (mss_lat[xind2] - lat) / delx * (mss_lon[yind2] - lon) / dely
        w2 = (lat - mss_lat[xind1]) / delx * (mss_lon[yind2] - lon) / dely
        w3 = (lat - mss_lat[xind1]) / delx * (lon - mss_lon[yind1]) / dely
        w4 = (mss_lat[xind2] - lat) / delx * (lon - mss_lon[yind1]) / dely

        # compute zi
        zi = w1 * z1 + w2 * z2 + w3 * z3 + w4 * z4
        return zi

    def get_mss_values(self, mss_path: str) -> np.ndarray:
        with xr.open_dataset(mss_path) as mss_ds:
            # Load arrays into memory
            mss_lat = mss_ds["lat"].values
            mss_lon = mss_ds["lon"].values
            mss_diff = mss_ds["mssdiff"].values
            mss_swapped_values = self.mss_interp(
                mss_lat,
                mss_lon,
                mss_diff,
                self.ds["latitude"].values,
                self.ds["longitude"].values,
            )
        return mss_swapped_values

    def make_ssha_smoothed(self, date: datetime):
        self.ds = ssha_smoothing(self.ds, date, self.source_config.source)

    def make_lonlat_points(self, lats: np.ndarray, lons: np.ndarray) -> gpd.GeoDataFrame:
        """
        Convert lat lon values to shapely Point objects and wrap
        as georeferenced GeoDataFrame.
        """
        lons = (lons + 180) % 360 - 180
        lonlats = list(zip(lons, lats))
        lonlat_points = [shapely.Point(lonlat) for lonlat in lonlats]
        points_df = gpd.GeoDataFrame(lonlat_points, geometry=0, crs="4326")
        return points_df

    def map_points_to_basin(self):
        """ """
        logging.info("Mapping data points to their respective basin")

        poly_df = gpd.read_file(os.path.join(REF_FILES_DIR, "basin", "new_basin_lake_polygons.shp"))

        # Format basin ids and names for basin_names_table
        names = poly_df["name"].apply(lambda x: x.replace("'", " ").replace(",", " -")).values
        basin_ids = poly_df["feature_id"].astype(str).values
        basin_table = np.array([f"{basin},{name}" for basin, name in zip(basin_ids, names)])
        basin_table = np.insert(basin_table, 0, "0,Land", axis=0)
        self.ds["basin_names_table"] = (
            ("basins"),
            np.array(basin_table).astype("unicode"),
        )

        if len(self.ds["time"]) == 0:
            self.ds["ssha_smoothed"] = (("time"), np.array([], dtype="float64"))
            self.ds["basin_flag"] = (("time"), np.array([], dtype="int32"))
            self.ds["basin_flag"].attrs["flag_values"] = np.array(basin_ids, dtype=np.int32)
            self.ds["basin_flag"].attrs["flag_meanings"] = " ".join(
                [name.replace(": ", ":").replace(" ", "_").replace(":", "_") for name in names]
            )
            return

        points_df = self.make_lonlat_points(self.ds["latitude"].values, self.ds["longitude"].values)
        join_df = gpd.sjoin(points_df, poly_df, how="left", predicate="within")
        self.ds["basin_flag"] = (
            ("time"),
            np.nan_to_num(join_df.feature_id.values).astype("int32"),
        )
        self.ds["basin_flag"].attrs["flag_values"] = np.array(basin_ids, dtype=np.int32)
        self.ds["basin_flag"].attrs["flag_meanings"] = " ".join(
            [name.replace(": ", ":").replace(" ", "_").replace(":", "_") for name in names]
        )

    def apply_basin_to_nasa(self):
        self.ds["nasa_flag"].values[
            ((self.ds["basin_flag"] == 0) | (self.ds["basin_flag"] == 1003) | (self.ds["basin_flag"] == 190))
        ] = 1

    def set_var_attrs(self):
        for var, attrs in get_var_attrs(self.target_mss).items():
            for attr, value in attrs.items():
                self.ds[var].attrs[attr] = value

    def set_global_attrs(self):
        """
        Sets the global attrs that are common across all sources. Individual processors
        set source specific global attrs via the abstract set_source_attrs().
        """
        global_attrs = get_base_global_attrs(source_files=self.source_files)
        global_attrs["time_coverage_start"] = (
            str(self.ds["time"].values[0])[:19] + "Z" if len(self.ds["time"]) > 0 else "N/A"
        )
        global_attrs["time_coverage_end"] = (
            str(self.ds["time"].values[-1])[:19] + "Z" if len(self.ds["time"]) > 0 else "N/A"
        )

        for k, v in global_attrs.items():
            self.ds.attrs[k] = v

    def set_metadata(self):
        self.set_var_attrs()
        self.set_global_attrs()

        if len(self.ds["time"]) == 0:
            for var in self.ds.variables:
                if "time" in self.ds[var].coords:
                    self.ds[var].attrs["comment"] = "No data for this date"
            self.ds.attrs["comment"] = "Data is missing from source for this date"

    def set_source_attrs(self):
        """Sets source-specific global attributes from collection metadata.
        Subclasses can override to add extra attributes (call super first)."""
        if self.source_config.collections:
            sources = set()
            source_urls = set()
            references = set()

            collections_by_id = {c.concept_id: c for c in self.source_config.collections}
            for collection_id in self.collection_ids:
                col = collections_by_id[collection_id]
                sources.add(col.source_label)
                source_urls.add(col.source_url)
                references.add(col.reference)

            self.ds.attrs["source"] = ", and ".join(sorted(sources))
            self.ds.attrs["source_url"] = ", and ".join(sorted(source_urls))
            self.ds.attrs["references"] = ", and ".join(sorted(references))
        else:
            self.ds.attrs["source"] = self.source_config.source_label
            self.ds.attrs["source_url"] = self.source_config.source_url
            self.ds.attrs["references"] = self.source_config.reference
        self.ds.attrs["mean_sea_surface"] = self.target_mss

        if self.ds.attrs["time_coverage_start"] == "N/A":
            self.ds.attrs["time_coverage_start"] = self.date.strftime("%Y-%m-%dT%H:%M:%S") + "Z"
        if self.ds.attrs["time_coverage_end"] == "N/A":
            self.ds.attrs["time_coverage_end"] = (self.date + timedelta(days=1) - timedelta(seconds=1)).strftime(
                "%Y-%m-%dT%H:%M:%S"
            ) + "Z"
