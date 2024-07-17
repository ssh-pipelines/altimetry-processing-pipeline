import logging
import resource
import unittest
from glob import glob

import numpy as np
from crossover.Crossover import date_from_fp
from crossover.parallel_crossovers import CrossoverProcessor

class XoverTestCase(unittest.TestCase):
    logging.root.handlers = []
    logging.basicConfig(
        level='INFO',
        format='[%(levelname)s] %(asctime)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    @classmethod
    def setUpClass(cls) -> None:       
        # Start tracking memory usage
        start_memory = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        
        cls.day = np.datetime64('2022-01-01')
        cls.source = "GSFC"
        cls.df_version = 'p1'
        
        processor = CrossoverProcessor(cls.day, cls.source, cls.df_version)
        
        processor.source_window.filepaths = sorted(glob('tests/test_granules/GSFC/*.nc'))
        processor.source_window.file_dates = [date_from_fp(fp) for fp in processor.source_window.filepaths]
        processor.source_window.streams = sorted(glob('tests/test_granules/GSFC/*.nc'))
        processor.source_window.init_and_fill_running_window()  
        cls.source_window = processor.source_window
        cls.ds = processor.search_day_for_crossovers()

        # Calculate the maximum memory used
        end_memory = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        max_memory_mb = end_memory / (1024 * 1024 )  # Convert to megabytes
        print("Maximum memory used:", max_memory_mb, "MB")
        
    def test_valid_length(self):
        self.assertGreaterEqual(len(self.ds.time1), 1000)
    
    def test_time1(self):
        self.assertGreaterEqual(self.ds.time1.values.min(), self.day)
        self.assertLessEqual(self.ds.time1.values.max(), np.datetime64(f'{self.day}T23:59:59'))
        self.assertTrue(np.all(np.diff(self.ds.time1.values.astype(float)) >= 0))

    def test_time2(self):
        self.assertGreaterEqual(self.ds.time2.values.min(), self.day)
        self.assertLessEqual(self.ds.time2.values.max(), np.datetime64(f'{self.source_window.window_end}T23:59:59'))