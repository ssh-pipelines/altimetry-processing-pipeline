import unittest
from glob import glob

import numpy as np
from crossover.parallel_crossovers import crossover_setup, search_day_for_crossovers

class XoverTestCase(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls) -> None:       
        cls.day = np.datetime64('2022-01-01')
        cls.source_1 = "GSFC"
        cls.source_2 = "GSFC"
        
        cls.source_window_1, cls.source_window_2 = crossover_setup(cls.day, cls.source_1, cls.source_2)
        
        for source_window in [cls.source_window_1, cls.source_window_2]:
            source_window.filepaths = sorted(glob('tests/test_granules/GSFC/*.nc'))
            source_window.set_file_dates()
            source_window.streams = [open(f, 'rb') for f in source_window.filepaths]
            source_window.init_and_fill_running_window()    
        
        cls.ds = search_day_for_crossovers(cls.day, cls.source_window_1, cls.source_window_2)
        
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