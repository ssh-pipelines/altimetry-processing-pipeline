from datetime import datetime, timedelta
import logging
import xarray as xr
import os
from glob import glob
from dateutil.rrule import rrule, DAILY

from oer.compute_polygon_correction import create_polygon, evaluate_correction, apply_correction
from oer.utils.s3_utils import S3Utils


class OerCorrection:
    '''
    Class for handling each step required to generate daily file processing level 2,
    from pulling the required crossover and daily file files to uploading 
    the polygon, oer, and daily file p2 netCDFs.
    '''
    def __init__(self, satellite: str, date: datetime, log_level='INFO') -> None:
        self.s3_utils: S3Utils = S3Utils()
        self.satellite: str = satellite
        self.date: datetime = date
        self.daily_file_filename = f'{satellite}-alt_ssh{date.strftime("%Y%m%d")}.nc'
        self.window_len: int = 10  # set window, since xover files "look forward" in time
        self.window_pad: int = 1  # padding to avoid edge effects at window end
        self.setup_logging(log_level)

    def setup_logging(self, log_level: str):
        logging.root.handlers = []
        logging.basicConfig(
            level=log_level,
            format='[%(levelname)s] %(asctime)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )

    def save_ds(self, ds: xr.Dataset, local_filename: str) -> str:
        '''
        Save xarray dataset as netCDF to /tmp
        '''
        out_path = os.path.join('/tmp', local_filename)    
        ds.to_netcdf(out_path)
        return out_path

    def fetch_xovers(self, window_start: datetime, window_end: datetime) -> xr.Dataset:
        date_range = list(rrule(DAILY, dtstart=window_start, until=window_end))
        streams = []
        for d in date_range:
            key = os.path.join(f's3://example-bucket/crossovers/p1/{self.satellite}', 
                               str(d.year), f'xovers_{self.satellite}-{d.strftime("%Y-%m-%d")}.nc')
            if self.s3_utils.key_exists(key):
                stream = self.s3_utils.stream_s3(key)
                streams.append(stream)
            else:
                logging.warning(f'Unable to stream {key} as it does not exist')
        if len(streams) == 0:
            raise RuntimeError('Unable to open any crossover files!')
        try:
            ds =  xr.open_mfdataset(streams, decode_times=False)
        except ValueError:
            # If all xovers are empty, just open one
            ds =  xr.open_mfdataset(streams[0], decode_times=False)
        return ds

    def fetch_daily_file(self) -> xr.Dataset:
        '''
        Streams the p1 daily file from the example-bucket bucket.
        '''
        prefix = os.path.join('s3://example-bucket/daily_files/p1', self.satellite, str(self.date.year), self.daily_file_filename)
        if self.s3_utils.key_exists(prefix):
            stream = self.s3_utils.stream_s3(prefix)
        else:
            raise ValueError(f'Key {prefix} does not exist!')
        return xr.open_dataset(stream)

    def make_polygon(self) -> xr.Dataset:       
        window_start = max(self.date - timedelta(self.window_len) - timedelta(self.window_pad), datetime(1992, 9, 25))
        window_end = self.date + timedelta(self.window_pad)
        
        xover_ds = self.fetch_xovers(window_start, window_end, self.satellite)
        
        polygon_ds = create_polygon(xover_ds, self.date, self.satellite)
        
        # Save the polygon as netCDF and upload to S3
        polygon_filename = f'oerpoly_{self.satellite}_{self.date.strftime("%Y-%m-%d")}.nc'
        out_path = self.save_ds(polygon_ds, polygon_filename)
        target_path = os.path.join('s3://example-bucket/oer', self.satellite, str(self.date.year), polygon_filename)
        self.s3_utils.upload_s3(out_path, target_path)
        return polygon_ds

    def make_correction(self, polygon_ds: xr.Dataset, daily_file_ds: xr.Dataset) -> xr.Dataset:
        correction_ds = evaluate_correction(polygon_ds, daily_file_ds, self.date, self.satellite)

        # Save the correction and upload to S3
        correction_filename = f'oer_correction_{self.satellite}_{self.date.strftime("%Y-%m-%d")}.nc'
        out_path = self.save_ds(correction_ds, correction_filename)
        target_path = os.path.join('s3://example-bucket/oer', self.satellite, str(self.date.year), correction_filename)
        self.s3_utils.upload_s3(out_path, target_path)
        return correction_ds

    def apply_oer(self, daily_file_ds: xr.Dataset, correction_ds: xr.Dataset) -> xr.Dataset:
        ds = apply_correction(daily_file_ds, correction_ds)
        if 'time' in ds['basin_names_table'].dims:
            ds['basin_names_table'] = ds['basin_names_table'].isel(time=0)
        # Save the correction and upload to S3
        out_path = self.save_ds(ds, self.daily_file_filename)
        target_path = os.path.join('s3://example-bucket/daily_files/p2', self.satellite, str(self.date.year), self.daily_file_filename)
        self.s3_utils.upload_s3(out_path, target_path)    
        return ds
    
    def run(self):
        '''
        Executes the three steps for OER correction:
        1. Make the polygon
        2. Compute corrections using polygon and daily file
        3. Apply corrections to daily file
        
        Each step includes uploading netCDF to relevant bucket location
        '''
        polygon_ds = self.make_polygon()
        
        daily_file_ds = self.fetch_daily_file()
        
        correction_ds = self.make_correction(polygon_ds, daily_file_ds)
        
        corrected_df_ds = self.apply_oer(daily_file_ds, correction_ds)
        
        # Cleanup files saved to /tmp
        for f in glob(f'/tmp/*.nc'):
            os.remove(f)
        
        logging.info(f'OER complete for {self.satellite} {self.date}')