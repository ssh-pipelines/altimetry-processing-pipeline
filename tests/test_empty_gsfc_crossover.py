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
        cls.processor.streams = []
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
        self.assertEqual(len(self.ds.time1), 0)
    
    def test_netcdf_vars(self):
        self.assertIn('time1', self.ds.dims)
        self.assertIn('time2', self.ds.data_vars)
        self.assertIn('ssh1', self.ds.data_vars)
        self.assertIn('ssh2', self.ds.data_vars)
        self.assertIn('cycle1', self.ds.data_vars)
        self.assertIn('cycle2', self.ds.data_vars)
        self.assertIn('pass1', self.ds.data_vars)
        self.assertIn('pass2', self.ds.data_vars)
        self.assertIn('lon', self.ds.data_vars)
        self.assertIn('lat', self.ds.data_vars)
        
    def test_netcdf_attrs(self):
        self.assertIn('GSFC self-crossovers', self.ds.attrs['title'])
        self.assertEqual(self.ds.attrs['input_product_generation_steps'], '1')
        self.assertEqual(self.ds.attrs['satellite_names'], 'GSFC')