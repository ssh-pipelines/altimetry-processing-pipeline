import xarray as xr
import numpy as np
from datetime import datetime

from daily_files.processing.daily_file import DailyFile

class GSFC_DailyFile(DailyFile):
    
    def __init__(self, ds:xr.Dataset, date: datetime):
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