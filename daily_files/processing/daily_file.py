from abc import ABC, abstractmethod
import logging
import xarray as xr
import numpy as np
import geopandas as gpd
import shapely

from datetime import datetime

from daily_files.processing.smoothing import ssh_smoothing


class DailyFile(ABC):
    '''
    Parent class for individual altimeter source data. Required data arrays:
    - SSH in meters
    - Latitude
    - Longitude
    - Cycle
    - Pass
    - Time
    
    Individual subclasses will implement:
        make_daily_file_ds (defines sequence of processing):
        make_nasa_flag()
        clean_date()
        make_ssh_smoothed()
        map_points_to_basin()
        set_metadata()
    make_nasa_flag (creates boolean flag from source data flags)
    '''
    
    def __init__(self, ssh: np.ndarray, lat: np.ndarray, lon: np.ndarray, time: np.ndarray, sat_cycle: np.ndarray, sat_pass: np.ndarray, dac: np.ndarray):
        self.time: np.ndarray = time
        self.data = {
            'ssh': xr.DataArray(ssh, dims=['time']),
            'dac': xr.DataArray(dac, dims=['time']),
            'latitude': xr.DataArray(lat, dims=['time']),
            'longitude': xr.DataArray(lon, dims=['time']),
            'cycle': xr.DataArray(sat_cycle, dims=['time']),
            'pass': xr.DataArray(sat_pass, dims=['time'])
        }
        
        self.ds = self.make_ds()
        

    @abstractmethod
    def make_daily_file_ds(self):
        '''
        Abstract method for the steps required to create daily file ds object.
        Defined per source dataset
        '''
        raise NotImplementedError
    
    @abstractmethod
    def make_nasa_flag(self):
        '''
        Abstract method for defining the NASA flag variable.
        Defined per source dataset
        '''
        raise NotImplementedError
    
    @abstractmethod
    def mss_swap(self):
        '''
        Abstract method for performing an MSS swap on ssh.
        Defined per source dataset
        '''
        raise NotImplementedError
    
    @abstractmethod
    def set_source_attrs(self):
        '''
        Abstract method for defining source specific metadata
        '''
        raise NotImplementedError
    
    def make_ds(self) -> xr.Dataset:
        ds = xr.Dataset(
            data_vars=self.data,
            coords=dict(time=self.time)
        )
        ds.time.encoding['units'] = 'seconds since 1990-01-01'
        ds = ds.sortby('time')
        return ds
    
    def date_subset(self, ds: xr.Dataset, date: datetime) -> xr.Dataset:
        '''
        Drop times outside of date
        '''
        ds = ds.where(~np.isnat(ds.time), drop=True)        
        today = str(date)[:10]
        logging.debug(f'Subsetting data to {today}')
        try:
            ds = ds.sel(time=today)
        except KeyError as e:
            ds = ds.where(ds.time==today, drop=True)
        return ds
    
    def drop_dupe_times(self, ds: xr.Dataset) -> xr.Dataset:
        logging.debug('Dropping duplicate times')
        ds = ds.drop_duplicates(dim='time')
        return ds
    
    def filter_outliers(self, ds: xr.Dataset, limit: float = 2) -> xr.Dataset:
        '''
        Removes values that exceed limit. Not currently used.
        '''
        ds = ds.where(np.abs(ds.ssh) < limit, drop=True)
        return ds
    
    def clean_date(self, date: datetime):
        '''
        Subsets data to date, drops duplicate times and filters outliers
        '''
        logging.info('Performing subsetting by date and filtering outlier values')
        self.ds = self.date_subset(self.ds, date)
        self.ds = self.drop_dupe_times(self.ds)
        
    def mss_interp(self, mss_lat: np.ndarray, mss_lon: np.ndarray, mss_diff: np.ndarray, lat: np.ndarray, lon: np.ndarray) -> np.ndarray:
        '''
        perform bilinear interpolation of a 2-D gridded input field to a list input positions
        Function assumes the following:
            1) x & y are regularly spaced, monotonically increasing 
            2) z has shape = (len(x), len(y))
            3) all values of xi are within the range of x
            4) all values of yi are within the range of y
            5) xi and yi are vectors of the same length
            6) all input arrays are numpy arrays
        '''
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
        w1 = ( (mss_lat[xind2] - lat) / delx * (mss_lon[yind2] - lon) / dely )
        w2 = ( (lat - mss_lat[xind1]) / delx * (mss_lon[yind2] - lon) / dely )
        w3 = ( (lat - mss_lat[xind1]) / delx * (lon - mss_lon[yind1]) / dely )
        w4 = ( (mss_lat[xind2] - lat) / delx * (lon - mss_lon[yind1]) / dely )

        # compute zi
        zi = w1 * z1 + w2 * z2 + w3 * z3 + w4 * z4
        return zi
    
    def get_mss_values(self, mss_path: str) -> np.ndarray:
        with xr.open_dataset(mss_path) as mss_ds:
            # Load arrays into memory
            mss_lat = mss_ds.lat.values
            mss_lon = mss_ds.lon.values
            mss_diff = mss_ds.mssdiff.values
            mss_swapped_values = self.mss_interp(mss_lat, mss_lon, mss_diff, self.ds.latitude.values, self.ds.longitude.values)
        return mss_swapped_values
    
    def make_ssh_smoothed(self, date: datetime):
        self.ds = ssh_smoothing(self.ds, date)
    
    def make_lonlat_points(self, lats: np.ndarray, lons: np.ndarray) -> gpd.GeoDataFrame:
        '''
        Convert lat lon values to shapely Point objects and wrap
        as georeferenced GeoDataFrame.
        '''
        lons = (lons + 180) % 360 - 180
        lonlats = list(zip(lons, lats))
        lonlat_points = [shapely.Point(lonlat) for lonlat in lonlats]
        points_df = gpd.GeoDataFrame(lonlat_points, geometry=0, crs='4326')
        return points_df
    
    def map_points_to_basin(self):
        '''
        
        '''
        logging.info('Mapping data points to their respective basin')
        
        poly_df = gpd.read_file('daily_files/ref_files/basin/new_basin_lake_polygons.shp')
        
        if len(self.ds.time) == 0:
            self.ds['ssh_smoothed'] = (('time'), np.array([], dtype='float64'))
            self.ds['basin_flag'] = (('time'), np.array([], dtype='int32'))
            poly_df['feature_id'] = poly_df['feature_id'].astype(str).str.ljust(4, ' ')
            basin_table = poly_df[['feature_id', 'name']].agg(','.join, axis=1).values
            basin_table = np.insert(basin_table, 0, '0   ,Land', axis=0)
            self.ds['basin_names_table'] = (('basin_name_dim'), basin_table.astype('S33'))
            return
        
        points_df = self.make_lonlat_points(self.ds.latitude.values, self.ds.longitude.values)
        join_df = gpd.sjoin(points_df, poly_df, how='left',predicate="within")
        self.ds['basin_flag'] = (('time'), np.nan_to_num(join_df.feature_id.values).astype('int32'))
        poly_df['feature_id'] = poly_df['feature_id'].astype(str).str.ljust(4, ' ')
        basin_table = poly_df[['feature_id', 'name']].agg(','.join, axis=1).values
        basin_table = np.insert(basin_table, 0, '0   ,Land', axis=0)
        self.ds['basin_names_table'] = (('basin_name_dim'), basin_table.astype('S33'))
        
    def apply_basin_to_nasa(self):
        self.ds.nasa_flag.values[self.ds.basin_flag == 0] = 1
    
    def set_var_attrs(self):     
        attributes = {
            'latitude': {
                'long_name': 'latitude', 
                'standard_name': 'latitude', 
                'units': 'degrees_north'
            },
            'longitude': {
                'long_name': 'longitude', 
                'standard_name': 'longitude', 
                'units': 'degrees_east'
            },
            'time': {
                'long_name': 'time', 
                'standard_name': 'time',
                'REFTime': '1990-01-01 00:00:00',
                'REFTime_comment': f'This string contains a time in the format yyyy-mm-dd HH:MM:SS ' \
                                   f'to which all times in the time variable are referenced.'
            },
            'cycle': {
                'long_name': 'cycle number', 'standard_name': 'cycle'
            },
            'pass': {
                'long_name': 'pass number', 'standard_name': 'pass'
            },
            'ssh': {
                'long_name': 'sea surface height relative to mean_sea_surface',
                'standard_name': 'ssh',
                'mean_sea_surface': self.target_mss,
                'description': 'Use nasa_flag = 0 to select valid data points from this variable',
                'units': 'm', 
                'coordinates': 'latitude longitude'
            },
            'ssh_smoothed': {
                'long_name': 'smoothed sea surface height relative to mean_sea_surface',
                'standard_name': 'ssh_smoothed',
                'mean_sea_surface': self.target_mss,
                'description': f'smoothed sea surface height values computed using a 19 point filter. ' \
                               f'nasa_flag is applied prior to filter and should not be used to remove points from this field.',
                'units': 'm', 
                'coordinates': 'latitude longitude'
            },
            'dac': {
                'long_name': 'dynamic atmospheric correction',
                'standard_name': 'dac',
                'comment': 'Additive correction applied to ssh to remove atmospheric effects.  Subtract this field from ssh or ssh_smoothed to un-apply this correction.',
                'units': 'm', 
                'coordinates': 'latitude longitude'
            },
            'basin_flag': {
                'long_name': 'basin ID number mapping each observation to a geographic basin', 
                'standard_name': 'basin_flag', 
                'reference': 'Adapted from Natural Earth. Free vector and raster map data @ naturalearthdata.com'
            },
            'basin_names_table': {
                'long_name': 'Table mapping basin ID numbers to basin names', 
                'standard_name': 'basin_names_table',
                'description': 'Values are comma separated string of the form feature id,feature name',
                'note': 'Some basins without widely known basin names are named with their basin number as Feature ID: XX, where XX is the basin number from basin_flag',
                'reference': 'Adapted from Natural Earth. Free vector and raster map data @ naturalearthdata.com'
            },
            'nasa_flag': {
                'long_name': 'nasa ssh quality flag', 
                'standard_name': 'nasa_flag', 
                'flag_meanings': 'good bad',
                'description': 'Quality flag to be used for ssh, not for ssh_smoothed. nasa_flag is set to 0 for data that should be retained, and 1 for data that should be removed.'
            }
        }
        
        if len(self.ds.time) > 0:
            for ssh_var in ['ssh', 'ssh_smoothed', 'dac']:
                attributes[ssh_var]['valid_min'] = np.nanmin(self.ds[ssh_var])
                attributes[ssh_var]['valid_max'] = np.nanmax(self.ds[ssh_var])
                
        for var, attrs in attributes.items():
            for attr, value in attrs.items():
                self.ds[var].attrs[attr] = value
        
    def set_global_attrs(self):
        '''
        Sets the global attrs that are common across all sources. Individual processors
        set source specific global attrs via the abstract set_source_attrs().
        '''
        global_attrs = {
            'Conventions': "CF-1.7",
            'title': "Standardized Along-Track Sea Surface Height",
            'summary': "This data set contains satellite based measurements of sea surface height, computed relative to the mean sea surface specified in mean_sea_surface. Data have been collected from multiple satellites, and processed to maximize compatibility and minimize bias between satellites. They are intended for use in studies and applications requiring climate-quality observations without additional adjustments or filtering.",
            'institution': "NASA/Jet Propulsion Laboratory",
            'source': "", # Source specific
            'source_url': "", # Source specific
            'history': f"Created on {datetime.now().isoformat(timespec='seconds')}",
            'references': "", # Source specific
            'mean_sea_surface': "",
            # 'Metadata_Conventions': "Unidata Dataset Discovery v1.0",
            # 'standard_name_vocabulary': "CF Standard Name Table v29",
            'id': "PODAAC-EXAMPLE-DATA",
            # 'naming_authority': "org.nasa.podaac",
            'project': "NASA-SSH",
            'processing_level': "Level 2",
            'product_generation_step': "1",
            'acknowledgement': "This data is provided by NASAs PO.DAAC.",
            # 'license': "Public Domain",
            'product_version': "2401",
            'keywords': "Earth Science, Oceans, Ocean Topography, Sea Surface Height, Sea Level",
            'keywords_vocabulary': "NASA Global Change Master Directory (GCMD) Science Keywords",
            'platform': "Satellite",
            'instrument': "Altimeter",
            # 'publisher_name': "NASA PO.DAAC",
            # 'publisher_url': "https://podaac.jpl.nasa.gov/",
            # 'publisher_email': "podaac@podaac.jpl.nasa.gov",
            'creator_name': "NASA-SSH",
            'creator_url': "https://podaac.jpl.nasa.gov/NASA-SSH/",
            'creator_email': "",
            'geospatial_lat_min': '', # Source specific
            'geospatial_lat_max': '', # Source specific
            'geospatial_lon_min': '0LL',
            'geospatial_lon_max': '360LL',
            'time_coverage_start': str(self.ds.time.values[0])[:19] + 'Z' if len(self.ds.time) > 0 else 'N/A',
            'time_coverage_end': str(self.ds.time.values[-1])[:19] + 'Z' if len(self.ds.time) > 0 else 'N/A',
        }
        
        for k, v in global_attrs.items():
            self.ds.attrs[k] = v
            
    def set_metadata(self):
        self.set_var_attrs()
        self.set_global_attrs()
        
        if len(self.ds.time) == 0:
            for var in self.ds.variables:
                if 'time' in self.ds[var].coords:
                    self.ds[var].attrs['comment'] = 'No data for this date' 
            self.ds.attrs['comment'] = 'Data is missing from source for this date'