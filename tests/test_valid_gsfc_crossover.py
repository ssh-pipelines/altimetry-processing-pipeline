import logging
import resource
import unittest
from glob import glob

import numpy as np
from crossover.parallel_crossovers import Crossover, CrossoverData

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
        
        cls.processor = Crossover(cls.day, cls.source, cls.df_version)
        
        cls.processor.crossover_data = CrossoverData.init()
        cls.processor.streams = sorted(glob('tests/test_granules/GSFC/*.nc'))
        if len(cls.processor.streams) > 0:
            cls.processor.extract_and_set_data()
            cls.processor.search_day_for_crossovers()
        cls.ds = cls.processor.create_dataset()
        local_path = cls.processor.save_to_netcdf(cls.ds)
        print(local_path)
        # Calculate the maximum memory used
        end_memory = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        max_memory_mb = end_memory / (1024 * 1024 )  # Convert to megabytes
        print("Maximum memory used:", max_memory_mb, "MB")
        
    def test_valid_length(self):
        self.assertGreaterEqual(len(self.ds['time1']), 1000)
    
    def test_time1(self):
        self.assertGreaterEqual(self.ds['time1'].values.min(), self.day)
        self.assertLessEqual(self.ds['time1'].values.max(), np.datetime64(f'{self.day}T23:59:59'))
        self.assertTrue(np.all(np.diff(self.ds['time1'].values.astype(float)) >= 0))

    def test_time2(self):
        self.assertGreaterEqual(self.ds['time2'].values.min(), self.day)
        self.assertLessEqual(self.ds['time2'].values.max(), np.datetime64(f'{self.processor.window_end}T23:59:59'))