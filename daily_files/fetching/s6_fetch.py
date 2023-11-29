from datetime import datetime
import logging
import re
from typing import Iterable
from daily_files.fetching.cmr_query import CMR_Granule, CMR_Query
from daily_files.fetching.podaac_s3_fetch import Podaac_S3_Fetcher
from daily_files.fetching.s6_collections import S6_Collections


class S6_Fetch(Podaac_S3_Fetcher):
    '''
    Sentinel 6 data is obtained from multiple collections with different levels of processing
    validity. We want to select granules from collections with the highest level of validity available.
    This is accomplished through some collection priority logic, where a lower priority value
    is more desirable.
    '''
    
    
    date: datetime
    priority_granules: dict
    granules: Iterable[CMR_Granule]
    
    def __init__(self, date: datetime):
        '''
        Sets self.granules from inhereted cmr_query method
        Uses intermediary self.priority_granules dict to select by priority
            where key is cycle_pass and value is (collection priority, CMR_Granule object)
        '''
        self.date = date
        self.priority_granules = {}
        self.granules = self.select_priority_granules()
    
    def select_priority_granules(self):
        '''
        Query for multiple S6 collections and select granules based on collection
        priorities as defined in daily_files.fetching.s6_collections.S6_Collections
        '''
        for collection in S6_Collections.S6_COLLECTIONS:
            logging.info(f'Querying for collection {collection.shortname}')
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