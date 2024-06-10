from io import TextIOWrapper
import logging
import resource
import unittest
from glob import glob

import numpy as np
from crossover.Crossover import date_from_fp
from crossover.parallel_crossovers import window_init, search_day_for_crossovers
from crossover.utils.log_config import configure_logging

class XoverTestCase(unittest.TestCase):
    configure_logging(testing=True)
    @classmethod
    def setUpClass(cls) -> None:       
        # Start tracking memory usage
        start_memory = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        
        cls.day = np.datetime64('2022-01-01')
        cls.source_1 = "GSFC"
        cls.source_2 = "GSFC"
        cls.df_version = 'p1'
        
        cls.source_window_1, cls.source_window_2 = window_init(cls.day, cls.source_1, cls.source_2, cls.df_version)
        
        for i, source_window in enumerate([cls.source_window_1, cls.source_window_2], 1):
            logging.info(f'Initializing and filling data for window {i}...')
            source_window.filepaths = sorted(glob('tests/test_granules/GSFC/*.nc'))
            source_window.file_dates = [date_from_fp(fp) for fp in source_window.filepaths]
            source_window.streams = sorted(glob('tests/test_granules/GSFC/*.nc'))
            source_window.init_and_fill_running_window()    
        
        cls.ds = search_day_for_crossovers(cls.day, cls.source_window_1, cls.source_window_2)
        
        # Calculate the maximum memory used
        end_memory = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        max_memory_mb = end_memory / (1024 * 1024 )  # Convert to megabytes
        print("Maximum memory used:", max_memory_mb, "MB")
        
    def test_valid_length(self):
        self.assertGreaterEqual(len(self.ds.time1), 1000)
    
    def test_time1(self):
        self.assertGreaterEqual(self.ds.time1.values.min(), self.day)
        self.assertLessEqual(self.ds.time1.values.max(), np.datetime64(f'{self.day}T23:59:59'))
        dx = np.diff(self.ds.time1.values.astype(float))
        self.assertTrue(np.all(dx >= 0))

    def test_time2(self):
        self.assertGreaterEqual(self.ds.time2.values.min(), self.day)
        self.assertLessEqual(self.ds.time2.values.max(), np.datetime64(f'{self.source_window_2.window_end}T23:59:59'))