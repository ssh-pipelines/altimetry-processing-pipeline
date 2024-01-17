from typing import TextIO
import xarray as xr
import numpy as np
from datetime import datetime

from daily_files.processing.daily_file import DailyFile
from daily_files.collection_metadata import AllCollections

class S6DailyFile(DailyFile):
    
    def __init__(self, file_obj:TextIO, date: datetime, collection_id: str):
        ds = self.extract_grouped_data(file_obj)
        self.original_ds = ds
        self.collection_id = collection_id
        
        ssh: np.ndarray = ds.ssha.values
        lats: np.ndarray = ds.latitude.values
        lons: np.ndarray = ds.longitude.values
        times: np.ndarray = ds.time.values
        cycles: np.ndarray = np.full_like(ds.ssha.values, ds.attrs["cycle_number"])
        passes: np.ndarray = np.full_like(ds.ssha.values, ds.attrs["pass_number"])
        
        self.source_mss = 'DTU18'
        self.target_mss = 'DTU21'
        mss_name = f'{self.source_mss}_interp_to_{self.target_mss}'
        mss_path: str = f's3://example-bucket/ref_files/mss_interpolations/{mss_name}.pkl'
        
        super().__init__(ssh, lats, lons, times, cycles, passes, mss_path)
        
        self.make_daily_file_ds(date)
    
    def extract_grouped_data(file_obj:TextIO) -> xr.Dataset:
        ku_ds = xr.open_dataset(file_obj, group='data_01/ku')
        ku_sub_ds = ku_ds[['ssha', 'range_ocean_qual']]

        c_ds = xr.open_dataset(file_obj, group='data_01/c')
        c_sub_da = c_ds['range_ocean_qual']
        c_sub_da.name = 'range_ocean_qual_c'

        ds = xr.open_dataset(file_obj, group='data_01')
        sub_ds = ds[['latitude', 'longitude', 'time',
                    'surface_classification_flag', 'rain_flag', 
                    'rad_rain_flag', 'rad_sea_ice_flag']]

        ds = xr.merge([sub_ds, ku_sub_ds, c_sub_da])
        ds.attrs = xr.open_dataset(file_obj).attrs
        return ds
    
    
    def make_daily_file_ds(self, date: datetime):
        '''
        Ordering of steps to create daily file from GSFC granule
        '''
        self.make_nasa_flag()
        self.clean_date(date)
        if self.ds.time.size < 2:
            return
        self.mss_swap()
        self.make_ssh_smoothed()
        self.map_points_to_basin()
        self.set_metadata()
        self.set_source_attrs()
    
    
    def make_nasa_flag(self):
        '''
        surface_classification_flag = 0
        rain_flag = 0
        rad_rain_flag = 0
        rad_sea_ice_flag = 0
        range_ocean_qual = 0
        range_ocean_qual_c = 0
        '''
        flag_ds = self.original_ds.drop_vars('ssha')
        valid_array = np.logical_or.reduce([flag_ds[var] for var in flag_ds.data_vars])
        
        self.ds['nasa_flag'] = (('time'), valid_array)
        self.ds.nasa_flag.attrs['flag_derivation'] = 'Logical AND of surface_classification_flag = 0, \
            rain_flag = 0, rad_rain_flag = 0, rad_sea_ice_flag = 0, range_ocean_qual = 0, range_ocean_qual_c = 0'
        
        for var in flag_ds.data_vars:
            self.ds[var] = ('time', flag_ds[var].values.astype('bool'))
            self.ds[var].attrs['Flag info'] = flag_ds[var].attrs['comment']

    def set_source_attrs(self):
        '''
        Sets S6 specific global attributes
        '''
        collection_meta = AllCollections.collections[self.collection_id]
        self.ds.attrs['source'] = collection_meta.source
        self.ds.attrs['source_url'] = collection_meta.source_url
        self.ds.attrs['references'] = collection_meta.reference
        self.ds.attrs['geospatial_lat_min'] = "-66.15LL"
        self.ds.attrs['geospatial_lat_max'] = "66.15LL"
        self.ds.attrs['mean_sea_surface'] = self.target_mss
        self.ds.attrs['mean_sea_surface_comment'] = f'Mean sea surface has been switched from {self.source_mss} to {self.target_mss}'
        self.ds.attrs['absolute_offset_applied'] = 0