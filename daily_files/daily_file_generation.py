from datetime import datetime
from glob import glob
import logging
from typing import Callable, List

import xarray as xr
from daily_files.utils.s3_utils import get_bucket

from daily_files.processing import gsfc_processing, s6_processing
from daily_files.utils.logconfig import configure_logging
from daily_files.utils import s3_utils


def mock_s3_get(source) -> List[str]:
    logging.info('Mocking collecting pass paths from S3...')
    if source=='gsfc':
        glob_string = '/Users/username/Downloads/GSFC-some-pass-files-2022-09-12/Merged_TOPEX_Jason_OSTM_Jason-3_Cycle_1072.V5_1.nc'
    paths = glob(glob_string)
    paths.sort()
    return paths

def get_paths_from_s3(date, source, satellite, bucket, profile=''):

    if source == 'rads':
        prefix = f'pass_files/{source}/'
        objs = s3_utils.get_objects(bucket, prefix)
    else:
        if satellite == 'merged_alt':
            bucket_name = 'podaac-ops-cumulus-protected/MERGED_TP_J1_OSTM_OST_CYCLES_V51/'
        objs = s3_utils.get_podaac_objects(bucket_name)
        
    paths = [o.key for o in objs if o.key[-1] != '/']

    return paths

def get_processor(source: str) -> Callable:
    processors = {
        's6': s6_processing,
        'gsfc': gsfc_processing
    }
    try:
        processor = processors[source]
    except:
        logging.exception(f'{source} not supported source.')
        raise Exception(f'{source} not supported source.')
    return processor

def merge_passes(inputs: List[xr.Dataset], paths: List[str]) -> xr.Dataset:
    logging.info(f'Merging processed passes into daily file.')
    ds = xr.concat(inputs, 'time')
    ds = ds.sortby('time')
    
    ds.attrs['source_files'] = [p.split('/')[-1] for p in paths]

    return ds

def work(satellite: str, date: datetime, source: str):
    '''
    
    '''
    # Mock S3 - need to account for better date grabbing 
    bucket = get_bucket('sli-granules')
    paths = get_paths_from_s3(date, source, satellite, bucket)
    processor = get_processor(source)
    
    processed_passes = []
    for path in paths:
        logging.info(f'Processing {path}')

        data = s3_utils.read_object(bucket, path)
        ds = xr.open_dataset(data)
        processed_ds = processor(ds, date)
        if processed_ds.time.size:
            processed_passes.append(processed_ds)
        else:
            logging.info('Ignoring empty pass')

    daily_ds = merge_passes(processed_passes, paths)
    filename = f'{satellite}-alt_ssh{str(date)[:10].replace("-","")}.nc'
    out_path = f'tmp/{filename}'
    logging.info(f'Saving {out_path}')
    daily_ds.to_netcdf(out_path)

    s3_output_path = f'daily_files/{satellite}/{date.year}/{filename}'
    s3_utils.upload_s3(out_path, bucket, s3_output_path)

def main(event):
    configure_logging(file_timestamp=False)

    date = event['date']
    source = event['source']
    satellite = event['satellite']

    work(satellite, date, source)