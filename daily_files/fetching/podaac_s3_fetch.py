from datetime import datetime
from io import TextIOWrapper
import os
import s3fs
from typing import Iterable
from daily_files.fetching.cmr_query import CMR_Granule, CMR_Query
from daily_files.fetching.fetcher import Fetcher
from daily_files.fetching.auth import Podaac_S3_Creds

class Podaac_S3_Fetcher(Fetcher):
    '''
    Requires obtaining temporary podaac s3 creds
    '''
    
    granules: Iterable[CMR_Granule]
    
    def __init__(self):
        pass
        
    def cmr_query(self, concept_id: str, date: datetime) -> Iterable[CMR_Granule]:
        return CMR_Query(concept_id, date).query()
                
    def setup_s3(self) -> s3fs.S3FileSystem:
        creds = Podaac_S3_Creds(os.environ['EARTHDATA_USER'], os.environ['EARTHDATA_PASSWORD']).creds
        s3 = s3fs.S3FileSystem(anon=False,
                            key=creds['accessKeyId'],
                            secret=creds['secretAccessKey'], 
                            token=creds['sessionToken'])
        return s3
    
    def fetch(self, src: str) -> TextIOWrapper:
        self.s3 = self.setup_s3()
        return self.s3.open(src)
    
    def fetch_all(self) -> Iterable[s3fs.S3FileSystem]:
        opened_objs = []
        for granule in self.granules:
            obj = self.fetch(granule.s3_url)
            opened_objs.append(obj)
        return opened_objs