from abc import ABC, abstractmethod
import logging
import os
import pickle
import xarray as xr
import numpy as np
import geopandas as gpd
import shapely
import s3fs

from datetime import datetime, timedelta

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
    
    def __init__(self, ssh: np.ndarray, lat: np.ndarray, lon: np.ndarray, time: np.ndarray, sat_cycle: np.ndarray, sat_pass: np.ndarray, mss_path: str, dac: np.ndarray):
        self.time: np.ndarray = time
        self.data = {
            'ssh': xr.DataArray(ssh, dims=['time']),
            'dac': xr.DataArray(dac, dims=['time']),
            'latitude': xr.DataArray(lat, dims=['time']),
            'longitude': xr.DataArray(lon, dims=['time']),
            'cycle': xr.DataArray(sat_cycle, dims=['time']),
            'pass': xr.DataArray(sat_pass, dims=['time'])
        }
        self.mss = self.get_mss(mss_path)
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
        return ds
    
    def date_subset(self, ds: xr.Dataset, date: datetime) -> xr.Dataset:
        '''
        Drop times outside of date
        '''
        ds = ds.where(~np.isnat(ds.time), drop=True)
        
        today = str(date)[:10]
        logging.debug(f'Subsetting data to {today}')
        ds = ds.sel(time=today)
        return ds
    
    def drop_dupe_times(self, ds: xr.Dataset) -> xr.Dataset:
        ds = ds.drop_duplicates(dim='time')
        return ds
    
    def filter_outliers(self, ds: xr.Dataset, limit: float = 2) -> xr.Dataset:
        '''
        Removes values that exceed limit
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
        
    def get_mss(self, mss_path: str):
        s3 = s3fs.S3FileSystem(key=os.environ['AWS_ACCESS_KEY_ID'],
                            secret=os.environ['AWS_SECRET_ACCESS_KEY'], 
                            token=os.environ['AWS_SESSION_TOKEN'])
        mss_file_like = s3.open(mss_path)
        mss_interpolator = pickle.load(mss_file_like)
        return mss_interpolator
        
    def mss_swap(self):
        logging.info('Applying mss swap to ssh values...')
        mss_corr_interponrads = self.mss.ev(self.ds.latitude, self.ds.longitude)
        self.ds.ssh.values = self.ds.ssh.values + mss_corr_interponrads
        
    def make_ssh_smoothed(self):
        self.ds = ssh_smoothing(self.ds)
    
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
        poly_df = gpd.read_file('daily_files/ref_files/basin_shapefile/new_basin_lake_polygons.shp')
        points_df = self.make_lonlat_points(self.ds.latitude.values, self.ds.longitude.values)
        join_df = gpd.sjoin(points_df, poly_df, how='left',predicate="within")
        self.ds['basin_flag'] = (('time'), np.nan_to_num(join_df.feature_id.values))
        poly_df['feature_id'] = poly_df['feature_id'].astype(str).str.ljust(4, ' ')
        basin_table = poly_df[['feature_id', 'name']].agg(','.join, axis=1).values
        basin_table = np.insert(basin_table, 0, '0   ,Land', axis=0)
        self.ds['basin_names_table'] = (('basin_name_dim'), basin_table.astype('S33'))
    
    def set_var_attrs(self):
        # Lat/lon coordinates
        self.ds['latitude'].attrs = {'long_name': 'latitude', 
                                     'standard_name': 'latitude', 
                                     'units': 'degrees_north'}
        self.ds['longitude'].attrs = {'long_name': 'longitude', 
                                      'standard_name': 'longitude', 
                                      'units': 'degrees_east'}
        
        # Time
        self.ds['time'].attrs = {'long_name': 'time', 'standard_name': 'time'}
        
        # Cycle and pass
        self.ds['cycle'].attrs = {'long_name': 'cycle number', 'standard_name': 'cycle'}
        self.ds['pass'].attrs = {'long_name': 'pass number', 'standard_name': 'pass'}

        # SSH variables
        for ssh_var in ['ssh', 'ssh_smoothed', 'dac']:
            self.ds[ssh_var].attrs = {
                'valid_min': np.nanmin(self.ds[ssh_var]), 
                'valid_max': np.nanmax(self.ds[ssh_var]), 
                'units': 'm', 
                'coordinates': 'latitude longitude'
            }
        self.ds['ssh'].attrs['long_name'] = 'sea surface height'
        self.ds['ssh'].attrs['standard_name'] = 'ssh'
        self.ds['ssh_smoothed'].attrs['long_name'] = 'smoothed sea surface height'
        self.ds['ssh_smoothed'].attrs['standard_name'] = 'ssh_smoothed'
        self.ds['dac'].attrs['long_name'] = 'dynamic atmospheric correction'
        self.ds['dac'].attrs['standard_name'] = 'dac'
        
        # Basin flag
        self.ds['basin_flag'].attrs = {'long_name': 'basin flag mapping point to ocean basin', 
                                       'standard_name': 'basin_flag', 
                                       'reference': 'Adapted from Natural Earth. Free vector and raster map data @ naturalearthdata.com'}
        
        self.ds['basin_names_table'].attrs = {'long_name': 'Table mapping basin ids to basin names', 
                                       'standard_name': 'basin_names_table',
                                       'note': 'Some basins without widely known basin names are named with their basin number as Feature ID: XX, where XX is the basin number from basin_flag',
                                       'reference': 'Adapted from Natural Earth. Free vector and raster map data @ naturalearthdata.com'}
        
        # Nasa flag
        self.ds['nasa_flag'].attrs.update({'long_name': 'nasa ssh quality flag', 
                                       'standard_name': 'nasa_flag', 
                                       'flag_meanings': 'good bad'})
    
    def set_global_attrs(self):
        '''
        Sets the global attrs that are common across all sources. Individual processors
        set source specific global attrs via the abstract set_source_attrs().
        '''
        global_attrs = {
            'title': "Standardized Along-Track Sea Surface Height",
            'institution': "NASA/Jet Propulsion Laboratory",
            'source': "", # Source specific
            'source_url': "", # Source specific
            'history': f"Created on {datetime.now().isoformat(timespec='seconds')}",
            'references': "", # Source specific
            'comment': "Sea Surface Height data are computed relative to the mean sea surface specified in the global attribute: mean_sea_surface.",
            'mean_sea_surface': "",
            'Conventions': "CF-1.7",
            'Metadata_Conventions': "Unidata Dataset Discovery v1.0",
            'standard_name_vocabulary': "CF Standard Name Table v29",
            'id': "PODAAC-EXAMPLE-DATA",
            'naming_authority': "org.nasa.podaac",
            'project': "NASA-SSH",
            'processing_level': "Level 2",
            'product_generation_step': "1",
            'acknowledgement': "This data is provided by NASA\'s PO.DAAC.",
            'license': "Public Domain",
            'product_version': "2401",
            'summary': "This data set contains satellite based measurements of sea surface height, computed relative to the mean sea surface specified in mean_sea_surface. Data have been collected from multiple satellites, and processed to maximize compatibility and minimize bias between satellites. They are intended for use in climate-quality scientific studies without additional adjustments to account for inter-satellite biases.",
            'keywords': "Earth Science, Oceans, Ocean Topography, Sea Surface Height, Sea Level",
            'keywords_vocabulary': "NASA Global Change Master Directory (GCMD) Science Keywords",
            'platform': "Satellite",
            'instrument': "Altimeter",
            'publisher_name': "NASA PO.DAAC",
            'publisher_url': "https://podaac.jpl.nasa.gov/",
            'publisher_email': "podaac@podaac.jpl.nasa.gov",
            'creator_name': "NASA-SSH",
            'creator_url': "https://podaac.jpl.nasa.gov/NASA-SSH/",
            'geospatial_lat_min': '', # Source specific
            'geospatial_lat_max': '', # Source specific
            'geospatial_lon_min': '0LL',
            'geospatial_lon_max': '360LL',
            'time_coverage_start': str(self.ds.time.values[0])[:19] + 'Z',
            'time_coverage_end': str(self.ds.time.values[-1])[:19] + 'Z',
            'REFTime': '1990-01-01 00:00:00',
            'REFTime_Description': 'This string contains a time in the format yyyy-mm-dd HH:MM:SS to which all times in the time variable are referenced.'
        }
        
        for k, v in global_attrs.items():
            self.ds.attrs[k] = v
            
    def set_metadata(self):
        self.set_var_attrs()
        self.set_global_attrs()
        