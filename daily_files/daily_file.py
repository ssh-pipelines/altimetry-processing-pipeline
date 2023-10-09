from abc import ABC, abstractmethod
import logging
import xarray as xr
import numpy as np
from datetime import datetime, timedelta
import geopandas as gpd
import shapely

from daily_files.smoothing import ssh_smoothing

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
    
    def __init__(self, ssh: np.ndarray, lat: np.ndarray, lon: np.ndarray, time: np.ndarray, sat_cycle: np.ndarray, sat_pass: np.ndarray):
        self.time: np.ndarray = time
        self.data = {
            'ssh': xr.DataArray(ssh, dims=['time']),
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
        today = str(date)[:10]
        end_today = str(date + timedelta(1) - timedelta(seconds=1))[:10]
        logging.debug(f'Subsetting data to between {today} and {end_today}')
        ds = ds.sel(time=slice(today, end_today), drop=True)
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
        self.ds = self.date_subset(self.ds, date)
        self.ds = self.drop_dupe_times(self.ds)
        self.ds = self.filter_outliers(self.ds)
        
        


    
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
        points_df = gpd.GeoDataFrame(lonlat_points)
        points_df = points_df.set_geometry(0)
        points_df = points_df.set_crs('4326')
        return points_df
    
    def map_points_to_basin(self):
        '''
        
        '''
        poly_df = gpd.read_file('daily_files/ref_files/basin_shapefile/new_basin_polygons.shp')
        points_df = self.make_lonlat_points(self.ds.latitude.values, self.ds.longitude.values)
        join_df = gpd.sjoin(points_df, poly_df, how='left',predicate="within")
        self.ds['basin_flag'] = (('time'), join_df.feature_id.values)
    
    def set_latlon_attrs(self):
        self.ds.latitude.attrs = {'units': 'degrees_north'}
        self.ds.longitude.attrs = {'units': 'degrees_east'}
    
    
    def set_ssh_var_attrs(self):
        for ssh_var in ['ssh', 'ssh_smoothed']:
            self.ds[ssh_var].attrs = {
                'valid_min': np.nanmin(self.ds[ssh_var]), 
                'valid_max': np.nanmax(self.ds[ssh_var]), 
                'units': 'm', 
                'coordinates': 'latitude longitude'
            }
        
    
    def set_global_attrs(self):
        global_attrs = {
            'title': "Example Sea Surface Height Data",
            'institution': "NASA",
            'source': "Simulated data for example purposes",
            'history': f"Created on {datetime.now().isoformat(timespec='seconds')}",
            'references': "None",
            'comment': "This dataset is for illustrative purposes only.",
            'Conventions': "CF-1.7",
            'Metadata_Conventions': "Unidata Dataset Discovery v1.0",
            'standard_name_vocabulary': "CF Standard Name Table v29",
            'id': "PODAAC-EXAMPLE-DATA",
            'naming_authority': "org.nasa.podaac",
            'project': "Example Project",
            'processing_level': "Level 2",
            'acknowledgement': "This data is provided by NASA\'s PO.DAAC.",
            'license': "Public Domain",
            'product_version': "1.0",
            'summary': "Example dataset containing sea surface height data along a satellite ground track.",
            'keywords': "Earth Science, Oceans, Ocean Topography, Sea Surface Height",
            'keywords_vocabulary': "NASA Global Change Master Directory (GCMD) Science Keywords",
            'platform': "Example Satellite",
            'instrument': "Example Instrument",
            # 'cdm_data_type': "Point", # This causes issues when opening via Panoply
            'publisher_name': "NASA PO.DAAC",
            'publisher_url': "https://podaac.jpl.nasa.gov/",
            'publisher_email': "podaac@podaac.jpl.nasa.gov",
            'creator_name': "Your Name",
            'creator_email': "your.email@example.com",
            'creator_url': "https://www.example.com",
            'geospatial_lat_min': '-66LL',
            'geospatial_lat_max': '66LL',
            'geospatial_lon_min': '-180LL',
            'geospatial_lon_max': '180LL',
            'time_coverage_start': str(self.ds.time.values[0])[:19] + 'Z',
            'time_coverage_end': str(self.ds.time.values[-1])[:19] + 'Z',
            'REFTime': '1990-01-01 00:00:00',
            'REFTime_Description': 'This string contains a time in the format "yyyy-mm-dd HH:MM:SS" to which all times in the "time" variable are referenced.'
        }
        
        for k, v in global_attrs.items():
            self.ds.attrs[k] = v
            
    def set_metadata(self):
        self.set_latlon_attrs()
        self.set_ssh_var_attrs()
        self.set_global_attrs()
            
            
class GSFC_DailyFile(DailyFile):
    
    def __init__(self, ds:xr.Dataset, date: datetime):
        '''

        '''
        ssh: np.ndarray = ds.ssha.values / 1000 # Convert from mm
        lats: np.ndarray = ds.lat.values
        lons: np.ndarray = ds.lon.values
        times: np.ndarray = ds.time.values
        cycles: np.ndarray = np.full_like(ds.ssha.values, ds.attrs["merged_cycle"])
        passes: np.ndarray = ds["reference_orbit"].values
        
        super().__init__(ssh, lats, lons, times, cycles, passes)
        
        self.make_daily_file_ds(date, ds.flag, ds.Surface_Type.values)
    
    
    def make_daily_file_ds(self, date: datetime, flag: xr.DataArray, surface_type: np.ndarray):
        # Add in the gsfc flags we use in order to maintain consistant
        # temporal cropping of data. Will later be removed from ds during 
        # "nasa_flag" creation
        self.make_nasa_flag(flag, surface_type)
        self.clean_date(date)
        self.make_ssh_smoothed()
        self.map_points_to_basin()
        self.set_metadata()
    
    def gsfc_flag_splitting(self, gsfc_flag: xr.DataArray) -> dict:
        '''
        Breaks out individual GSFC flags from comprehensive flag
        '''
        split_flags = {}
        bin_strings = [f'{v:#017b}'[2:] for v in gsfc_flag.values]
        for i, flag_name in zip(range(-1, -16, -1), gsfc_flag.attrs['flag_meanings'].split()):
            name = flag_name.replace('/', '_per_')
            split_flags[name] = [int(v[i]) for v in bin_strings]
        return split_flags
    
    def make_nasa_flag(self, gsfc_flag: xr.DataArray, surface_type: np.ndarray):
        '''
        Surface_Type = 0 or 2
        AND:
        Radiometer_Observation_is_Suspect = 0
        Attitude_Out_of_Range = 0
        Sigma0_Ku_Band_Out_of_Range = 0
        Possible_Rain_Contamination = 0
        Sea_Ice_Detected = 0
        Significant_Wave_Height>8m = 0
        Any_Applied_SSH_Correction_Out_of_Limits = 0
        Contiguous_1Hz_Data = 0
        Sigma_H_of_fit>15cm = 0
        '''
        split_flags = self.gsfc_flag_splitting(gsfc_flag)
        valid_array = np.where((surface_type == 0) | (surface_type == 2), 0, 1)
        flag_list = ['Radiometer_Observation_is_Suspect', 'Sigma0_Ku_Band_Out_of_Range', 'Significant_Wave_Height>8m', 
                    'Possible_Rain_Contamination', 'Sea_Ice_Detected', 'Any_Applied_SSH_Correction_Out_of_Limits', 
                    'Sigma_H_of_fit>15cm', 'Contiguous_1Hz_Data', 'Attitude_Out_of_Range']
        for flag in flag_list:
            valid_array = np.logical_or(valid_array, split_flags[flag])
        self.ds['nasa_flag'] = (('time'), valid_array)
        self.ds.nasa_flag.attrs['flag_derivation'] = 'Logical AND of Surface_Type = 0 OR 2, Radiometer_Observation_is_Suspect = 0, \
            Attitude_Out_of_Range = 0, Sigma0_Ku_Band_Out_of_Range = 0, Possible_Rain_Contamination = 0, Sea_Ice_Detected = 0, \
            Significant_Wave_Height>8m = 0, Any_Applied_SSH_Correction_Out_of_Limits = 0, Contiguous_1Hz_Data = 0, Sigma_H_of_fit>15cm = 0'
        
        all_flags = [split_flags[flag] for flag in split_flags.keys()]                
        self.ds['gsfc_flag'] = (('time', 'flag_dim'), np.array(all_flags).T.astype('bool'))
        self.ds.gsfc_flag.attrs['Flag info'] = ', '.join(gsfc_flag.attrs['flag_meanings'].split())
    
    
class S6_DailyFile(DailyFile):
    pass