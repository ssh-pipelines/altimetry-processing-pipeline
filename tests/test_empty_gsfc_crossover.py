import logging
import resource
import unittest
from glob import glob

import numpy as np
from crossover.parallel_crossovers import CrossoverProcessor, CrossoverData

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
        
        processor.source_window.filepaths = []
        processor.source_window.file_dates = []
        processor.source_window.streams = glob('tests/test_granules/GSFC/BAD_FILEPATHS*.nc')
        if len(processor.source_window.streams) > 0:
            processor.source_window.init_and_fill_running_window()
            cls.ds = processor.search_day_for_crossovers()
        else:
            logging.info(f'No valid data found in {processor.source_window.shortname} window {processor.source_window.window_start} to {processor.source_window.window_end}.')
            processor.source_window.input_filenames = 'None'
            processor.source_window.input_histories = 'None'
            processor.source_window.input_product_generation_steps = 'None'
            
            cls.ds = processor.create_dataset(CrossoverData.create_empty())
        cls.source_window = processor.source_window
        # cls.ds.to_netcdf('empty_test.nc')
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
        self.assertEqual(self.ds.attrs['title'], 'GSFC self-crossovers')
        self.assertEqual(self.ds.attrs['input_filenames'], 'None')
        self.assertEqual(self.ds.attrs['input_histories'], 'None')
        self.assertEqual(self.ds.attrs['input_product_generation_steps'], 'None')
        self.assertEqual(self.ds.attrs['satellite_names'], 'GSFC')