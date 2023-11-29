import unittest
from daily_files.daily_file_job import Daily_File_Job
from daily_files.utils.logconfig import configure_logging


class GSFCFetchTestCase(unittest.TestCase):   
    daily_file_job: Daily_File_Job
    
    @classmethod
    def setUpClass(cls) -> None:  
        configure_logging(False, 'INFO', True)
        date = '2023-06-18'
        source = 'S6'
        satellite = 'S6'
            
        cls.daily_file_job = Daily_File_Job(date, source, satellite)
        cls.daily_file_job.fetch_granules()
        
    def test_priority(self):
        '''
        Need to devise test for ensuring priority selection is working.
        Could be tricky as the collections are continuously shifting
        '''
        # print(self.daily_file_job.fetcher.priority_granules.items())
                 

    def test_s3_path(self):
        s3_paths = [granule.s3_url for granule in self.daily_file_job.granules]
        self.assertTrue(s3_paths[0].startswith('s3://'))
        self.assertIn('podaac-ops-cumulus-protected', s3_paths[0])