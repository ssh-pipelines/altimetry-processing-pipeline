from datetime import datetime
import logging
from typing import Iterable
import xarray as xr
import os

from daily_files.utils.s3_utils import upload_s3
from daily_files.utils.logconfig import configure_logging

from daily_files.fetching.fetcher import Fetcher
from daily_files.fetching.cmr_query import CMRGranule
from daily_files.fetching.gsfc_fetch import GSFCFetch
from daily_files.fetching.s6_fetch import S6Fetch

from daily_files.processing.daily_file import DailyFile
from daily_files.processing.gsfc_daily_file import GSFCDailyFile
from daily_files.processing.s6_daily_file import S6DailyFile
# from daily_files.processing.cmems_daily_file import CMEMS_DailyFile


class SourceNotSupported(Exception):
    pass


class DailyFileJob():
    SOURCE_MAPPINGS = {
        'GSFC': {
            'fetcher': GSFCFetch,
            'processor': GSFCDailyFile
        },
        'S6': {
            'fetcher': S6Fetch,
            'processor': S6DailyFile
        },
        # 'CMEMS': {
        #     'fetcher': Podaac_S3_Fetch,
        #     'processor': S3_Bucket_Fetch
        # }
    }
    
    DAILY_FILE_BUCKET = 'example-bucket'
    
    def __init__(self, date: str, source: str, satellite: str):
        logging.info(f'Starting {source} job for {date}')
        self.date: datetime = datetime.strptime(date, '%Y-%m-%d')
        self.source: str = source
        self.satellite: str = satellite
        self.fetch_type: Fetcher = self.get_fetcher(source)
        self.processor: DailyFile = self.get_processor(source)
        
    @classmethod
    def get_fetcher(cls, source: str) -> Fetcher:
        try:
            fetcher = cls.SOURCE_MAPPINGS[source]['fetcher']
            logging.debug(f'Using {fetcher} fetcher')
        except:
            raise SourceNotSupported
        return fetcher

    @classmethod
    def get_processor(cls, source: str) -> DailyFile:
        try:
            processor = cls.SOURCE_MAPPINGS[source]['processor']
            logging.debug(f'Using {processor} processor')
        except:
            raise SourceNotSupported
        return processor
    
    def fetch_granules(self):
        logging.info('Fetching granules...')
        self.fetcher = self.fetch_type(self.date)
        self.granules: Iterable[CMRGranule] = self.fetcher.granules
        
def unify_global_attrs(ds: xr.Dataset, inputs: Iterable[xr.Dataset]) -> xr.Dataset:
    '''
    Given a list of processed input dataset objects and the concatenated dataset object,
    set the source specific global metadata fields appropriately. This accounts for source
    data pulled from multiple collections (the S6 use case).
    '''  
    source_strings = sorted(set(input_ds.attrs['source'] for input_ds in inputs))
    source_url_strings = sorted(set(input_ds.attrs['source_url'] for input_ds in inputs))
    source_references = sorted(set(input_ds.attrs['references'] for input_ds in inputs))
    ds.attrs['source'] = ', and '.join(source_strings)
    ds.attrs['source_url'] = ', and '.join(source_url_strings)
    ds.attrs['references'] = ', and '.join(source_references)
    return ds
    
        
def merge_passes(inputs: Iterable[xr.Dataset], job_granules: Iterable[CMRGranule]) -> xr.Dataset:
    logging.info(f'Merging {len(inputs)} processed passes into daily file.')
    ds = xr.concat(inputs, 'time')
    ds = ds.sortby('time')
    ds = unify_global_attrs(ds, inputs)
    ds.attrs['source_files'] = ', '.join([granule.title for granule in job_granules])
    return ds

def save_ds(ds: xr.Dataset, output_path: str):
    logging.info(f'Saving netCDF to {output_path}')
    
    encoding = {'time': {'units': 'seconds since 1990-01-01 00:00:00', 'calendar':'proleptic_gregorian'}}
    for var in ds.variables:
        if var not in ['latitude', 'longitude', 'time', 'REFTime']:
            encoding[var] = {'_FillValue': -9999.0, 'complevel': 5, 'zlib': True}
        if 'gsfc_flag' in var or 'nasa_flag' in var:
            encoding[var]['dtype'] = 'byte'
        if 'basin_flag' in var or 'pass' in var or 'cycle' in var:
            encoding[var]['dtype'] = 'uint16'
    ds.to_netcdf(output_path, encoding=encoding)
    

def work(job: DailyFileJob):
    '''
    Opens and processes granules via direct S3 paths
    '''
    processed_passes = []
    for granule in job.granules:
        logging.info(f'Processing {granule.title}')
        file_obj = job.fetcher.fetch(granule.s3_url)
        processed_ds = job.processor(file_obj, job.date, granule.collection_id).ds
        if processed_ds.time.size:
            processed_passes.append(processed_ds)
        else:
            logging.info('Ignoring empty granule')
    if not processed_passes:
        logging.info(f'No data for {job.date}.')
        return
    daily_ds = merge_passes(processed_passes, job.granules)
    
    filename = f'{job.satellite}-alt_ssh{str(job.date)[:10].replace("-","")}.nc'
    out_path = f'/tmp/{filename}'
    save_ds(daily_ds, out_path)
    
    s3_output_path = f'daily_files/{job.satellite}/{job.date.year}/{filename}'
    upload_s3(out_path, job.DAILY_FILE_BUCKET, s3_output_path)
        

def start_job(event: dict):
    date = event['date']
    source = event['source']
    satellite = event['satellite']
    os.environ['EARTHDATA_USER'] = event['EARTHDATA_USER']
    os.environ['EARTHDATA_PASSWORD'] = event['EARTHDATA_PASSWORD']    
    
    configure_logging(file_timestamp=False, log_level=event.get('log_level', 'INFO'))
        
    daily_file_job = DailyFileJob(date, source, satellite)
    daily_file_job.fetch_granules()
    if len(daily_file_job.granules) > 0:
        work(daily_file_job)
    else:
        logging.info(f'No {source} data found for {date}.')