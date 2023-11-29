from datetime import datetime, timedelta
import logging
import time
from typing import Iterable
from cmr import GranuleQuery

class S3NotFound(Exception):
    """Raise for S3 URL not available in CMR metadata exception"""

class CMR_Granule():
    
    def __init__(self, query_result: dict):
        self.granule_id: str = query_result.get('id')
        self.title: str = query_result.get('title')
        self.s3_url: str = self.extract_s3_url(query_result['links'])
        self.time_start: datetime = query_result.get('time_start')
        self.time_end: datetime = query_result.get('time_end')
        self.modified_time: datetime = query_result.get('updated')
        self.collection_id: str = query_result.get('collection_concept_id')
    
    def extract_s3_url(self, links: Iterable) -> str:
        for link in links:
            if 'rel' in link and link['rel'] == 'http://esipfed.org/ns/fedsearch/1.1/s3#':
                return link['href']
        raise S3NotFound()

class CMR_Query():
    
    def __init__(self, concept_id: str, date: datetime):
        self.concept_id: str = concept_id
        self.start_date: datetime = date
        self.end_date: datetime = self.start_date + timedelta(days=1) - timedelta(seconds=1)
        
    def query_with_wait(self):
        api = GranuleQuery()
        max_retries = 3
        attempt = 1
        while attempt <= max_retries:
            time.sleep(attempt * 15)
            try:
                query_results = api.concept_id(self.concept_id).provider('POCLOUD').temporal(self.start_date, self.end_date).get_all()
                return query_results
            except RuntimeError:
                attempt += 1
        logging.error('Unable to query CMR')
        raise RuntimeError

    def query(self) -> Iterable[CMR_Granule]:
        api = GranuleQuery()
        try:
            query_results = api.concept_id(self.concept_id).provider('POCLOUD').temporal(self.start_date, self.end_date).get_all()
        except RuntimeError:
            query_results = self.query_with_wait()
        cmr_granules = [CMR_Granule(result) for result in query_results]
        logging.info(f'Found {len(cmr_granules)} granule(s) from CMR query')
        return cmr_granules      
        