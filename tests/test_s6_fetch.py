from datetime import datetime
import re
from typing import Iterable
import unittest
from daily_files.daily_file_job import DailyFileJob
from daily_files.fetching.cmr_query import CMRGranule, CMRQuery
from daily_files.fetching.s6_fetch import S6Collections
from daily_files.utils.logconfig import configure_logging


class S6FetchTestCase(unittest.TestCase):   
    daily_file_job: DailyFileJob
    
    @classmethod
    def setUpClass(cls) -> None:  
        configure_logging(False, 'INFO', True)
        date = '2020-12-01'
        
        source = 'S6'
        satellite = 'S6'
        
        cls.date = datetime.strptime(date, '%Y-%m-%d')
        cls.priority_granules = {}
        cls.granules = cls.select_priority_granules(cls)
        
    def cmr_query(concept_id: str, date: datetime) -> Iterable[CMRGranule]:
        return CMRQuery(concept_id, date).query()
    
    def select_priority_granules(self):
        '''
        Query for multiple S6 collections and select granules based on collection
        priorities as defined in daily_files.fetching.s6_collections.S6_Collections
        '''
        for collection in S6Collections.S6_COLLECTIONS:
            granules = self.cmr_query(collection.concept_id, self.date)
            for granule in granules:
                # Extract cycle_pass from granule file name
                cycle_pass = re.search('_\d{3}_\d{3}_', granule.title).group(0)[1:-1]
                # Get current highest priority granule for this cycle_pass
                queue_status = self.priority_granules.get(cycle_pass, (100, None))
                # Update if current collection has higher priority
                if queue_status[0] > collection.priority:
                    self.priority_granules.update({cycle_pass: (collection.priority, granule)})
                    
        # Return the list of CMR_Granule objects
        return [v[1] for k,v in self.priority_granules.items()]
        
    def test_priority(self):
        '''
        Need to devise test for ensuring priority selection is working.
        Could be tricky as the collections are continuously shifting
        '''
        # print(self.daily_file_job.fetcher.priority_granules.items())
        for granule in self.priority_granules.values():
            print(granule[1].s3_url)

    def test_s3_path(self):
        s3_paths = [granule.s3_url for granule in self.daily_file_job.granules]
        self.assertTrue(s3_paths[0].startswith('s3://'))
        self.assertIn('podaac-ops-cumulus-protected', s3_paths[0])