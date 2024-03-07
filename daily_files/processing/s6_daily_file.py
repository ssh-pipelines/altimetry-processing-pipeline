from dataclasses import dataclass
import logging
import time
from typing import Iterable, TextIO
import pandas as pd
import xarray as xr
import netCDF4 as nc
import numpy as np
from datetime import datetime

from daily_files.processing.daily_file import DailyFile
from daily_files.collection_metadata import AllCollections

class S6DailyFile(DailyFile):
    
    def __init__(self, file_objs:Iterable[TextIO], date: datetime, collection_ids: Iterable[str]):
        logging.info(f'Opening {len(file_objs)} files')
        start = time.time()
        opened_files = [self.extract_grouped_data(file_obj) for file_obj in file_objs]
        ds = xr.concat(opened_files, dim='time')
        logging.debug(f'Opening and merging pass files took {time.time() - start} seconds')
        self.original_ds = ds
        self.collection_ids = collection_ids
        
        ssh: np.ndarray = ds.ssha_nr.values
        lats: np.ndarray = ds.latitude.values
        lons: np.ndarray = ds.longitude.values
        times: np.ndarray = ds.time.values
        cycles: np.ndarray = ds.cycle.values
        passes: np.ndarray = ds.passes.values
        dac: np.ndarray = ds.dac.values
        
        self.source_mss = 'DTU18'
        self.target_mss = 'DTU21'
        self.mss_name = f'{self.source_mss}_interp_to_{self.target_mss}'
        mss_path: str = f's3://example-bucket/ref_files/mss_interpolations/{self.mss_name}.pkl'
        
        super().__init__(ssh, lats, lons, times, cycles, passes, mss_path, dac)
        
        self.ds['mean_sea_surface_sol1'] = (('time'), self.original_ds['mean_sea_surface_sol1'].values)
        self.ds['mean_sea_surface_sol2'] = (('time'), self.original_ds['mean_sea_surface_sol2'].values)
        
        self.make_daily_file_ds(date)
    
    def extract_grouped_data(self, file_obj:TextIO) -> xr.Dataset:
        '''
        '''
        ds = nc.Dataset('file_like', 'r', memory=file_obj.read())
        das = []

        for var in ['latitude', 'longitude', 'surface_classification_flag', 'rain_flag', 'rad_water_vapor_qual', 'dac', 'mean_sea_surface_sol1', 'mean_sea_surface_sol2']:
            nc_var = ds.groups['data_01'].variables[var]
            nc_var_data = nc_var[:]
            nc_var_attrs = {k:v for k,v in nc_var.__dict__.items() if k != 'scale_factor'}
            da = xr.DataArray(nc_var_data, dims='time', attrs=nc_var_attrs, name=var)
            das.append(da)
            
        for var in ['sig0_ocean_nr', 'range_ocean_nr_qual', 'swh_ocean_nr', 'ssha_nr']:
            nc_var = ds.groups['data_01'].groups['ku'].variables[var]
            nc_var_data = nc_var[:]
            nc_var_attrs = {k:v for k,v in nc_var.__dict__.items() if k != 'scale_factor'}
            da = xr.DataArray(nc_var_data, dims='time', attrs=nc_var_attrs, name=var)
            das.append(da)
            
        merged_ds = xr.merge(das)
        merged_ds = merged_ds.set_coords(['latitude', 'longitude'])
        merged_ds['time'] = ds.groups['data_01'].variables['time'][:]
        merged_ds['time'].attrs = {k: v for k,v in ds.groups['data_01'].variables['time'].__dict__.items() if k != 'scale_factor' and k != 'add_offset'}
        merged_ds.attrs = {k: v for k,v in ds.__dict__.items() if k != 'scale_factor' and k != 'add_offset'}
        merged_ds['cycle'] = (('time'), np.full(merged_ds.time.values.shape, ds.cycle_number))
        merged_ds['passes'] = (('time'), np.full(merged_ds.time.values.shape, ds.pass_number))
        return xr.decode_cf(merged_ds)
    
    
    def make_daily_file_ds(self, date: datetime):
        '''
        Ordering of steps to create daily file from GSFC granule
        '''
        
        start = time.time()
        self.make_nasa_flag()
        logging.debug(f'Nasa_flag took {time.time() - start} seconds')
        
        start = time.time()
        self.clean_date(date)
        logging.debug(f'Date cleaning took {time.time() - start} seconds')
        
        if self.ds.time.size < 2:
            return
        
        # start = time.time()
        try:
            self.mss_swap()
        except Exception as e:
            logging.error(f'Unable to perform mss swap...{e}')
        # logging.debug(f'MSS swapping took {time.time() - start} seconds')
        
        start = time.time()
        self.make_ssh_smoothed()
        logging.debug(f'Smoothing took {time.time() - start} seconds')
        
        start = time.time()
        self.map_points_to_basin()
        logging.debug(f'Basin mapping took {time.time() - start} seconds')
        
        self.apply_basin_to_nasa()
        self.set_metadata()
        self.ds.dac.attrs = self.original_ds.dac.attrs
        self.set_source_attrs()
    
    
    def make_nasa_flag(self):
        '''
        '''
        logging.info('Making nasa_flag...')
        kqual = self.original_ds.range_ocean_nr_qual.values
        surfc = self.original_ds.surface_classification_flag.values
        rqual = self.original_ds.rad_water_vapor_qual.values
        rain = self.original_ds.rain_flag.values
        s0 = self.original_ds.sig0_ocean_nr.values
        swh = self.original_ds.swh_ocean_nr.values
        ssh = self.original_ds.ssha_nr.values
        
        @dataclass
        class Point:
            x: int
            y: int

        p1, p2 = Point(11, 10), Point(16, 6)
        p3, p4 = Point(26, 3), Point(32, 0)

        # 1st trend line goes from (x1, y1) to (x2, y2)
        swtrend1 = (s0 - p1.x) * ((p2.y - p1.y) / (p2.x - p1.x)) + p1.y
        # 2nd trend line goes from (x2, y2) to (x3, y3)
        swtrend2 = (s0 - p2.x) * ((p3.y - p2.y) / (p3.x - p2.x)) + p2.y
        # 3rd trend line goes from (x3, y3) to (x4, y4)
        swtrend3 = (s0 - p3.x) * ((p4.y - p3.y) / (p4.x - p3.x)) + p3.y

        swflag = (
            (swh > 14)
            | ((s0 > p1.x) & (swh > 10))
            | ((s0 >= p1.x) & (s0 < p2.x) & (swh > swtrend1))
            | ((s0 >= p2.x) & (s0 < p3.x) & (swh > swtrend2))
            | ((s0 >= p2.x) & (swh > swtrend3))
        )

        pflag = ((surfc==0) | (surfc==2)) & (kqual==0) & (rain==0) & (np.abs(ssh) < 5)

        swpflag = pflag & ~swflag
        
        nmedian = 15
        nstd = 95

        timestamps = np.array(range(1, len(ssh) + 1))

        sm = pd.Series(ssh[swpflag]).rolling(nmedian, center=True, min_periods=1).median().values

        dx = ssh[swpflag] - sm

        outlier_index = np.abs(dx) < 2
        pd_roll = pd.Series(np.square(dx[outlier_index])).rolling(nstd, center=True, min_periods=1)
        medians = pd_roll.median().values
        sstd = np.clip(np.sqrt(medians), 0.02, None)

        sminterp = np.interp(timestamps, timestamps[swpflag], sm)
        dx = ssh - sminterp
        sstdinterp = np.interp(timestamps, timestamps[swpflag][outlier_index], sstd)

        stdflag = abs(dx) > sstdinterp * 5
        nasa_flag = ~((~np.isnan(ssh)) & ((surfc==0) | (surfc==2)) & (kqual==0) & (rain==0) & (rqual==0) & (~stdflag))

        self.ds['nasa_flag'] = (('time'), nasa_flag)
        self.ds.nasa_flag.attrs['flag_derivation'] = 'nasa_flag is set to 0 if: ssha pass an along-track median test to filter outliers & basin_flag is set to any valid, non-fill value & the following gsfc_flag values are set to 0: surface_classification_flag = 0 or 2, rain_flag = 0, range_ocean_nr_qual = 0, rain_flag = 0, rad_water_vapor_qual = 0, and derived standard deviation flag = 0'
        
        self.ds['s6_flag'] = (('time', 'src_flag_dim'), np.array([kqual, surfc, rqual, rain, s0, swh]).T)
        self.ds['s6_flag'].attrs = {
            'standard_name': 'source_data_flag',
            'long_name': 'source data flag',
        }
        
        for i, src_flag in enumerate(self.original_ds[['range_ocean_nr_qual', 'surface_classification_flag', 'rad_water_vapor_qual', 'rain_flag', 'sig0_ocean_nr', 'swh_ocean_nr']]):
            key = f'flag_column_{i+1}'
            self.ds['s6_flag'].attrs[key] = src_flag
            
    def mss_swap(self):
        logging.info('Applying mss swap to ssh values...')
        mss_corr_interponrads = self.mss.ev(self.ds.latitude, self.ds.longitude)
        self.ds.ssh.values = self.ds.ssh.values + self.ds.mean_sea_surface_sol1 - self.ds.mean_sea_surface_sol2 - mss_corr_interponrads
        self.ds = self.ds.drop_vars(['mean_sea_surface_sol1', 'mean_sea_surface_sol2'])
    
    def apply_basin_to_nasa(self):
        valid_array = np.where(self.ds.basin_flag != 55537, self.ds.nasa_flag, 1)
        self.ds.nasa_flag.values = valid_array

    def set_source_attrs(self):
        '''
        Sets S6 specific global attributes
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
        self.ds.attrs['geospatial_lat_min'] = "-66.15LL"
        self.ds.attrs['geospatial_lat_max'] = "66.15LL"
        self.ds.attrs['mean_sea_surface'] = self.target_mss
        self.ds.attrs['mean_sea_surface_comment'] = f'Mean sea surface has been switched from {self.source_mss} to {self.target_mss}'
        self.ds.attrs['absolute_offset_applied'] = 0