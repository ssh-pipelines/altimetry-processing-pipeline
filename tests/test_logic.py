import os
import unittest
from glob import glob

import numpy as np
from crossover.parallel_crossovers import crossover_setup
from crossover.Crossover import Options, date_from_fp

class XoverTestCase(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls) -> None:       
        cls.day = np.datetime64('2021-01-01')
        cls.source_1 = "GSFC"
        cls.source_2 = "GSFC"
        cls.df_version = "p1"
        
        cls.OPTIONS = Options(cls.source_1, cls.source_2)
        
        cls.source_window_1, cls.source_window_2 = crossover_setup(cls.day, cls.source_1, cls.source_2, cls.df_version)
        
        cls.all_prefixes = []
        
        start_year = str(cls.source_window_1.window_start.astype('datetime64[Y]')).split('-')[0]
        end_year = str(cls.source_window_1.window_end.astype('datetime64[Y]')).split('-')[0]
        unique_years = np.unique([start_year, end_year])

        for year in unique_years:    
            prefix = os.path.join('daily_files', cls.source_window_1.df_version, cls.source_window_1.shortname, year)
            cls.all_prefixes.append(prefix)
            
    def test_options(self):
        self.assertTrue(self.OPTIONS.self_crossovers)
        self.assertEqual(self.OPTIONS.window_size + self.OPTIONS.window_padding, 12)
        
    def test_valid_prefix(self):
        for prefix in self.all_prefixes:
            self.assertEqual(prefix, os.path.join('daily_files', 'p1', 'GSFC', '2021'))

    def test_key_filtering(self):
        filenames = [os.path.join(prefix, f'GSFC-SSH_{date}.nc') for prefix in self.all_prefixes for date in range(20210101,20210131)]
        filtered_keys = list(filter(lambda x: date_from_fp(x)>= self.source_window_1.window_start and 
                                              date_from_fp(x) <= self.source_window_1.window_end,
                                              filenames))
        self.assertEqual(len(filtered_keys), self.OPTIONS.window_size + self.OPTIONS.window_padding + 1)