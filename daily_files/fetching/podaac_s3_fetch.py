from datetime import datetime
from io import TextIOWrapper
import logging
import os
import s3fs
from typing import Iterable
from daily_files.fetching.cmr_query import CMRGranule, CMRQuery
from daily_files.fetching.fetcher import Fetcher
from daily_files.fetching.auth import PodaacS3Creds
from daily_files.utils.s3_utils import get_secret

class PodaacS3Fetcher(Fetcher):
    '''
    Requires obtaining temporary podaac s3 creds
    '''
    
    granules: Iterable[CMRGranule]
    
    def __init__(self):
        # edl_secret = get_secret('EDL_auth')
        # self.ed_user = edl_secret.get('user')
        # self.ed_pass = edl_secret.get('password')
        self.ed_user = os.environ['EARTHDATA_USER']
        self.ed_pass = os.environ['EARTHDATA_PASSWORD']
        self.s3 = self.setup_s3()
        
    def cmr_query(self, concept_id: str, date: datetime) -> Iterable[CMRGranule]:
        return CMRQuery(concept_id, date).query()
                
    def setup_s3(self) -> s3fs.S3FileSystem:
        creds = PodaacS3Creds(os.environ['EARTHDATA_USER'], os.environ['EARTHDATA_PASSWORD']).creds
        s3 = s3fs.S3FileSystem(anon=False,
                            key=creds['accessKeyId'],
                            secret=creds['secretAccessKey'], 
                            token=creds['sessionToken'])
        return s3
    
    def fetch(self, src: str) -> TextIOWrapper:
        try:
            logging.debug(f'Loading {src} into memory')
            opened_s3 = self.s3.open(src)
        except Exception as e:
            logging.exception(f'Error opening {src}')
            raise e
        return opened_s3
    
    def fetch_all(self) -> Iterable[s3fs.S3FileSystem]:
        opened_objs = []
        for granule in self.granules:
            obj = self.fetch(granule.s3_url)
            opened_objs.append(obj)
        return opened_objs