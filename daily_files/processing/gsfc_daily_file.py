import logging
import xarray as xr
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Iterable, TextIO
from daily_files.processing.daily_file import DailyFile
from daily_files.collection_metadata import AllCollections

class GSFCDailyFile(DailyFile):

    def __init__(self, file_objs:Iterable[TextIO], date: datetime, collection_ids: Iterable[str]):
        opened_files = [xr.open_dataset(file_obj, engine='h5netcdf') for file_obj in file_objs]
        cycles = np.concatenate([np.full_like(ds.ssha.values, ds.attrs["merged_cycle"]) for ds in opened_files])
        self.og_ds = xr.concat(opened_files, dim='N_Records')
        
        ssh: np.ndarray = self.og_ds.ssha.values / 1000 # Convert from mm
        lats: np.ndarray = self.og_ds.lat.values
        lons: np.ndarray = self.og_ds.lon.values
        times: np.ndarray = self.og_ds.time.values
        cycles, passes = self.compute_cycles_passes(self.og_ds, cycles)
        dac: np.ndarray = np.full_like(self.og_ds.ssha.values, 0) # Place holder until real DAC data is available
        self.collection_ids = collection_ids

        self.source_mss = 'DTU15'
        self.target_mss = 'DTU21'
        mss_name = f'{self.source_mss}_interp_to_{self.target_mss}'
        mss_path: str = f's3://example-bucket/ref_files/mss_interpolations/{mss_name}.pkl'

        super().__init__(ssh, lats, lons, times, cycles, passes, mss_path, dac)
        
        self.make_daily_file_ds(date)
    
    def compute_cycles_passes(self, ds: xr.Dataset, cycles: np.ndarray) -> np.ndarray:
        '''
        Computes passes using look up table that converts a reference_orbit and index value to pass number.
        '''
        logging.info('Computing pass values')
        df = pd.read_csv('daily_files/ref_files/complete_gsfc_pass_lut.csv', converters={'id': str}).set_index('id')
        
        # Convert reference_orbit and index from GSFC file to 7 digit long, left-padded string
        ds_ids = [str(orbit).zfill(3)+str(index).zfill(4) for orbit, index in zip(ds.reference_orbit.values, ds['index'].values)]
        passes = df.loc[ds_ids]['pass'].values
        
        if 254 in passes and 1 in passes:
            cycles[(cycles==cycles[0]) & (passes==1)] += 1
        return cycles, passes
    
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
        self.apply_basin_to_nasa()
        self.set_metadata()
        self.set_source_attrs()
        
    
    def gsfc_flag_splitting(self) -> np.ndarray:
        '''
        Breaks out individual GSFC flags from comprehensive flag
        '''
        flag = self.og_ds.flag.values
        max_bits = int(np.ceil(np.log2(flag.max())))
        binary_representation = (flag[:, None] & (1 << np.arange(max_bits))).astype(bool)
        return binary_representation
    
    def make_nasa_flag(self):
        '''
        Convert source GSFC flag data into binary nasa_flag
        '''
        logging.info('Converting GSFC flag to NASA flag')
        og_flag_index = [1, 2, 3, 4, 5, 9]
        flag_array = self.gsfc_flag_splitting()
        valid_array = ~flag_array[:, og_flag_index].any(axis=1)
        
        all_flags = self.og_ds.flag.attrs['flag_meanings'].split()
        flag_list = [all_flags[i] for i in og_flag_index]
        
        pflag = ((self.og_ds.Surface_Type.values == 0) | (self.og_ds.Surface_Type.values == 2)) \
            & (valid_array) \
            & (~np.isnan(self.ds.ssh))

        # Make stdflag
        ssh = self.ds.ssh.values
        nmedian = 15
        nstd = 95
        timestamps = np.arange(1, len(ssh)+1)

        rolling_median = pd.Series(ssh[pflag]).rolling(nmedian, center=True, min_periods=1).median().values
        dx = ssh[pflag] - rolling_median

        dx_median = pd.Series(np.square(dx)).rolling(nstd, center=True, min_periods=1).median().values
        rolling_std = np.clip(np.sqrt(dx_median), 0.05, None)

        median_interp = np.interp(timestamps, timestamps[pflag], rolling_median)
        std_interp = np.interp(timestamps, timestamps[pflag], rolling_std)

        stdflag = abs(ssh - median_interp) <= std_interp * 5
        
        nasa_flag = ~(pflag & stdflag)
        
        self.ds['nasa_flag'] = (('time'), nasa_flag.data)
        self.ds['nasa_flag'].attrs['flag_derivation'] = f'nasa_flag is set to 0 if: abs(ssha) < 2 meter & basin_flag is set to any valid, non-fill value & the following gsfc_flag values are set to 0: {", ".join(flag_list)}'
        
        self.ds['gsfc_flag'] = (('time', 'src_flag_dim'), np.array(flag_array[:, og_flag_index]).astype('bool'))
        self.ds['gsfc_flag'].attrs = {
            'standard_name': 'source_data_flag',
            'long_name': 'source data flag',
        }
        
        for i, src_flag in enumerate(flag_list):
            key = f'flag_column_{i+1}'
            self.ds['gsfc_flag'].attrs[key] = src_flag
    
    def apply_basin_to_nasa(self):
        valid_array = np.where(self.ds.basin_flag != 55537, self.ds.nasa_flag, 1)
        self.ds.nasa_flag.values = valid_array    
        
    def set_source_attrs(self):
        '''
        Sets GSFC specific global attributes
        '''
        sources = set()
        source_urls = set()
        references = set()
        
        for collection_id in self.collection_ids:
            collection_meta = AllCollections.collections[collection_id]
            sources.add(collection_meta.source)
            source_urls.add(collection_meta.source_url)
            references.add(collection_meta.reference)
            
        self.ds.attrs['source'] = ', and '.join(sorted(sources))
        self.ds.attrs['source_url'] = ', and '.join(sorted(source_urls))
        self.ds.attrs['references'] = ', and '.join(sorted(references))
        self.ds.attrs['geospatial_lat_min'] = "-67LL"
        self.ds.attrs['geospatial_lat_max'] = "67LL"
        self.ds.attrs['mean_sea_surface'] = self.target_mss
        self.ds.attrs['mean_sea_surface_comment'] = f'Mean sea surface has been converted from source native {self.source_mss} to {self.target_mss}'
        self.ds.attrs['absolute_offset_applied'] = 0