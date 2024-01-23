import logging
import xarray as xr
import numpy as np
import pandas as pd
from datetime import datetime
from typing import TextIO
from daily_files.processing.daily_file import DailyFile
from daily_files.collection_metadata import AllCollections

class GSFCDailyFile(DailyFile):

    def __init__(self, file_obj:TextIO, date: datetime, collection_id: str):
        ds = xr.open_dataset(file_obj, engine='h5netcdf')
        ssh: np.ndarray = ds.ssha.values / 1000 # Convert from mm
        lats: np.ndarray = ds.lat.values
        lons: np.ndarray = ds.lon.values
        times: np.ndarray = ds.time.values
        cycles: np.ndarray = np.full_like(ds.ssha.values, ds.attrs["merged_cycle"])
        passes: np.ndarray = self.compute_passes(ds)

        self.collection_id = collection_id

        self.source_mss = 'DTU15'
        self.target_mss = 'DTU21'
        mss_name = f'{self.source_mss}_interp_to_{self.target_mss}'
        mss_path: str = f's3://example-bucket/ref_files/mss_interpolations/{mss_name}.pkl'

        super().__init__(ssh, lats, lons, times, cycles, passes, mss_path)
        
        self.og_ds = ds
        self.make_daily_file_ds(date, ds.flag, ds.Surface_Type.values)
    
    def compute_passes(self, ds: xr.Dataset) -> np.ndarray:
        '''
        Computes passes using look up table that converts a reference_orbit and index value to pass number.
        '''
        logging.info('Computing pass values')
        df = pd.read_csv('daily_files/ref_files/complete_gsfc_pass_lut.csv', converters={'id': str}).set_index('id')
        
        # Convert reference_orbit and index from GSFC file to 7 digit long, left-padded string
        ds_ids = [str(orbit).zfill(3)+str(index).zfill(4) for orbit, index in zip(ds.reference_orbit.values, ds['index'].values)]
        passes = df.loc[ds_ids]['pass'].values
        return passes
    
    def make_daily_file_ds(self, date: datetime, flag: xr.DataArray, surface_type: np.ndarray):
        '''
        Ordering of steps to create daily file from GSFC granule
        '''
        self.make_nasa_flag(flag, surface_type)
        self.clean_date(date)
        if self.ds.time.size < 2:
            return
        self.mss_swap()
        self.make_ssh_smoothed()
        self.map_points_to_basin()
        self.set_metadata()
        self.set_source_attrs()
        
    
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
        logging.info('Converting GSFC flag to NASA flag')
        split_flags = self.gsfc_flag_splitting(gsfc_flag)
        valid_array = np.where((surface_type == 0) | (surface_type == 2), 0, 1)
        flag_list = ['Radiometer_Observation_is_Suspect', 'Sigma0_Ku_Band_Out_of_Range', 'Significant_Wave_Height>8m', 
                    'Possible_Rain_Contamination', 'Sea_Ice_Detected', 'Any_Applied_SSH_Correction_Out_of_Limits', 
                    'Sigma_H_of_fit>15cm', 'Contiguous_1Hz_Data', 'Attitude_Out_of_Range']
        for flag in flag_list:
            valid_array = np.logical_or(valid_array, split_flags[flag])
        self.ds['nasa_flag'] = (('time'), valid_array)
        self.ds['nasa_flag'].attrs['flag_derivation'] = 'Logical AND of Surface_Type = 0 OR 2, Radiometer_Observation_is_Suspect = 0, \
            Attitude_Out_of_Range = 0, Sigma0_Ku_Band_Out_of_Range = 0, Possible_Rain_Contamination = 0, Sea_Ice_Detected = 0, \
            Significant_Wave_Height>8m = 0, Any_Applied_SSH_Correction_Out_of_Limits = 0, Contiguous_1Hz_Data = 0, Sigma_H_of_fit>15cm = 0'
        
        all_flags = [split_flags[flag] for flag in split_flags.keys()]                
        self.ds['gsfc_flag'] = (('time', 'flag_dim'), np.array(all_flags).T.astype('bool'))
        self.ds['gsfc_flag'].attrs = {
            'standard_name': 'source_data_flag',
            'long_name': 'source data flag',
            'Flag info': ', '.join(gsfc_flag.attrs['flag_meanings'].split())
        }
        
    def set_source_attrs(self):
        '''
        Sets GSFC specific global attributes
        '''
        collection_meta = AllCollections.collections[self.collection_id]
        self.ds.attrs['source'] = collection_meta.source
        self.ds.attrs['source_url'] = collection_meta.source_url
        self.ds.attrs['references'] = collection_meta.reference
        self.ds.attrs['geospatial_lat_min'] = "-67LL"
        self.ds.attrs['geospatial_lat_max'] = "67LL"
        self.ds.attrs['mean_sea_surface'] = self.target_mss
        self.ds.attrs['mean_sea_surface_comment'] = f'Mean sea surface has been converted from source native {self.source_mss} to {self.target_mss}'
        self.ds.attrs['absolute_offset_applied'] = 0