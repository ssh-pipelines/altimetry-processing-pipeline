import unittest
from daily_files.daily_file_job import DailyFileJob
from daily_files.utils.logconfig import configure_logging


class GSFCFetchTestCase(unittest.TestCase):   
    daily_file_job: DailyFileJob
    
    @classmethod
    def setUpClass(cls) -> None:  
        configure_logging(False, 'INFO', True)
        date = '2023-06-18'
        source = 'S6'
        satellite = 'S6'
            
        cls.daily_file_job = DailyFileJob(date, source, satellite)
        cls.daily_file_job.fetch_granules()
        
    def test_priority(self):
        '''
        Need to devise test for ensuring priority selection is working.
        Could be tricky as the collections are continuously shifting
        '''
        # print(self.daily_file_job.fetcher.priority_granules.items())
        print(self.daily_file_job.granules[0].s3_url)

    def test_s3_path(self):
        s3_paths = [granule.s3_url for granule in self.daily_file_job.granules]
        self.assertTrue(s3_paths[0].startswith('s3://'))
        self.assertIn('podaac-ops-cumulus-protected', s3_paths[0])