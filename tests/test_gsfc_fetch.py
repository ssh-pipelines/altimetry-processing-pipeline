import unittest
from daily_files.daily_file_job import Daily_File_Job
from daily_files.utils.logconfig import configure_logging


class GSFCFetchTestCase(unittest.TestCase):   
    daily_file_generator: Daily_File_Job
    
    @classmethod
    def setUpClass(cls) -> None:
        configure_logging(False, 'INFO', True)
        
        date = '2020-01-01'
        source = 'GSFC'
        satellite = 'GSFC'
            
        cls.daily_file_generator = Daily_File_Job(date, source, satellite)
        cls.daily_file_generator.fetch_granules()
        
    
    def test_correct_granule(self):
        titles = [granule.title for granule in self.daily_file_generator.granules]
        self.assertEqual(1, len(titles))
        self.assertEqual('Merged_TOPEX_Jason_OSTM_Jason-3_Cycle_1005.V5_1', titles[0])
    

    def test_s3_path(self):
        s3_paths = [granule.s3_url for granule in self.daily_file_generator.granules]
        self.assertTrue(s3_paths[0].startswith('s3://'))
        self.assertIn('podaac-ops-cumulus-protected', s3_paths[0])