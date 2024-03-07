from datetime import datetime
from io import TextIOWrapper
import logging
import os
import requests
import s3fs
from typing import Iterable
from daily_files.fetching.cmr_query import CMRGranule, CMRQuery
from daily_files.fetching.fetcher import Fetcher
from daily_files.fetching.auth import PodaacS3Creds
from daily_files.utils.s3_utils import get_secret

class PodaacFetcher(Fetcher):
    '''
    Requires obtaining temporary podaac s3 creds
    '''
    
    granules: Iterable[CMRGranule]
    
    def __init__(self):
        self.ed_user = os.environ['EARTHDATA_USER']
        self.ed_pass = os.environ['EARTHDATA_PASSWORD']
        
    def cmr_query(self, concept_id: str, date: datetime) -> Iterable[CMRGranule]:
        return CMRQuery(concept_id, date).query()
    
    def fetch(self, src: str) -> TextIOWrapper:
        fname = os.path.basename(src)
        logging.info(f'Downloading {fname}')
        with requests.Session() as session:
            session.auth = (os.environ['EARTHDATA_USER'], os.environ['EARTHDATA_PASSWORD'])
            r1 = session.request('get', src)
            r = session.get(r1.url, auth=(os.environ['EARTHDATA_USER'], os.environ['EARTHDATA_PASSWORD']))

            if r.ok: 
                with open(os.path.join('/tmp', fname), 'wb') as f:
                    f.write(r.content)
            else:
                raise requests.ConnectionError(f'Unable to download {src}')
        return os.path.join('/tmp', fname)
    
    def fetch_all(self):
        return super().fetch_all()
    
    def setup_s3(self):
        return super().setup_s3()