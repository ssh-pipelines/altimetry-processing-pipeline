from datetime import datetime
from typing import Iterable
from daily_files.fetching.cmr_query import CMR_Granule
from daily_files.fetching.podaac_s3_fetch import Podaac_S3_Fetcher


class GSFC_Fetch(Podaac_S3_Fetcher):
    shortname: str = 'MERGED_TP_J1_OSTM_OST_CYCLES_V51'
    concept_id: str = 'C2204129664-POCLOUD'
    granules: Iterable[CMR_Granule]
    
    def __init__(self, date: datetime):
        '''
        Sets self.granules from inhereted cmr_query method
        '''
        self.date = date
        self.granules = self.cmr_query(self.concept_id, self.date)