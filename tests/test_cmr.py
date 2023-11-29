from typing import Iterable
import unittest
from datetime import datetime
from daily_files.fetching.cmr_query import CMR_Query, CMR_Granule
from daily_files.utils.logconfig import configure_logging


class EndToEndCMRQueryTestCase(unittest.TestCase):   
    granules: Iterable[CMR_Granule]
    concept_id: str
    
    @classmethod
    def setUpClass(cls) -> None:
        configure_logging(False, 'INFO', True)
        
        gsfc_concpept_id = 'C2204129664-POCLOUD'
        date = datetime(2021,12,29)
        cmr_query = CMR_Query(gsfc_concpept_id, date)
        
        cls.concept_id = gsfc_concpept_id
        cls.date = date
        cls.granules = cmr_query.query()

    def test_number_hits(self):
        self.assertEqual(2, len(self.granules))

    def test_correct_collection(self):
        for granule in self.granules:
            self.assertEqual(self.concept_id, granule.collection_id)
            
    def test_s3_url(self):
        for granule in self.granules:
            self.assertTrue(granule.s3_url.startswith('s3://'))