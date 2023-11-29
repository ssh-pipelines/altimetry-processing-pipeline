from typing import TextIO
import xarray as xr
import numpy as np
from datetime import datetime

from daily_files.processing.daily_file import DailyFile

class S6_DailyFile(DailyFile):
    
    def __init__(self, file_obj:TextIO, date: datetime):
        ds = self.extract_grouped_data(file_obj)
        self.original_ds = ds
        
        ssh: np.ndarray = ds.ssha.values
        lats: np.ndarray = ds.latitude.values
        lons: np.ndarray = ds.longitude.values
        times: np.ndarray = ds.time.values
        cycles: np.ndarray = np.full_like(ds.ssha.values, ds.attrs["cycle_number"])
        passes: np.ndarray = np.full_like(ds.ssha.values, ds.attrs["pass_number"])
        
        super().__init__(ssh, lats, lons, times, cycles, passes)
        
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
        # Add in the gsfc flags we use in order to maintain consistant
        # temporal cropping of data. Will later be removed from ds during 
        # "nasa_flag" creation
        self.make_nasa_flag()
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