from datetime import datetime
import logging
from typing import Iterable
from daily_files.utils.s3_utils import upload_s3

import xarray as xr
from daily_files.fetching.fetcher import Fetcher
from daily_files.fetching.cmr_query import CMR_Granule
from daily_files.processing.daily_file import DailyFile

from daily_files.fetching.gsfc_fetch import GSFC_Fetch

from daily_files.processing.gsfc_daily_file import GSFC_DailyFile
# from daily_files.processing.s6_daily_file import S6_DailyFile
# from daily_files.processing.cmems_daily_file import CMEMS_DailyFile


from daily_files.utils.logconfig import configure_logging

class SourceNotSupported(Exception):
    pass


class Daily_File_Job():
    SOURCE_MAPPINGS = {
        'GSFC': {
            'fetcher': GSFC_Fetch,
            'processor': GSFC_DailyFile
        },
        # 'S6': {
        #     'fetcher': Podaac_S3_Fetch,
        #     'processor': S6_DailyFile
        # },
        # 'CMEMS': {
        #     'fetcher': Podaac_S3_Fetch,
        #     'processor': S3_Bucket_Fetch
        # }
    }
    
    DAILY_FILE_BUCKET = 'example-bucket'
    
    def __init__(self, date: str, source: str, satellite: str):
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
        self.fetcher = self.fetch_type(self.date)
        self.granules: Iterable[CMR_Granule] = self.fetcher.granules
        
def merge_passes(inputs: Iterable[xr.Dataset], source_granule_names: Iterable[str]) -> xr.Dataset:
    logging.info(f'Merging processed passes into daily file.')
    ds = xr.concat(inputs, 'time')
    ds = ds.sortby('time')
    ds.attrs['source_files'] = ', '.join(source_granule_names)
    return ds

def save_ds(ds: xr.Dataset, output_path: str):
    logging.info(f'Saving netCDF to {output_path}')
    
    encoding = {'time': {'units': 'seconds since 1990-01-01 00:00:00', 'calendar':'proleptic_gregorian'}}
    for var in ds.variables:
        if var not in ['latitude', 'longitude', 'time', 'REFTime']:
            encoding[var] = {'_FillValue': -9999.0}
        if 'gsfc_flag' in var or 'nasa_flag' in var:
            encoding[var]['dtype'] = 'byte'
        if 'basin_flag' in var or 'pass' in var or 'cycle' in var:
            encoding[var]['dtype'] = 'uint16'
    ds.to_netcdf(output_path, encoding=encoding)
    

def work(job: Daily_File_Job):
    '''
    Opens and processes granules via direct S3 paths
    '''
    processed_passes = []
    for granule in job.granules:
        logging.info(f'Processing {granule.title}')
        ds = xr.open_dataset(job.fetcher.fetch(granule.s3_url))
        processed_ds = job.processor(ds, job.date).ds
        if processed_ds.time.size:
            processed_passes.append(processed_ds)
        else:
            logging.info('Ignoring empty pass')
            
    source_granule_names = [granule['title'] for granule in job.granules]
    daily_ds = merge_passes(processed_passes, source_granule_names)
    
    filename = f'{job.satellite}-alt_ssh{str(job.date)[:10].replace("-","")}.nc'
    out_path = f'tmp/{filename}'
    save_ds(daily_ds, out_path)
    
    s3_output_path = f'daily_files/{job.satellite}/{job.date.year}/{filename}'
    upload_s3(out_path, job.DAILY_FILE_BUCKET, s3_output_path)
        

def start_job(event: dict):
    configure_logging(file_timestamp=False)

    date = event['date']
    source = event['source']
    satellite = event['satellite']
    
    daily_file_generator = Daily_File_Job(date, source, satellite)
    daily_file_generator.fetch_granules()
    work(daily_file_generator)