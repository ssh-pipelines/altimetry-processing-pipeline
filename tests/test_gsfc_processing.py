import unittest
import xarray as xr
import numpy as np
from datetime import datetime
from daily_files.processing.gsfc_daily_file import GSFCDailyFile
from daily_files.daily_file_job import merge_passes
from daily_files.utils.logconfig import configure_logging


class EndToEndGSFCProcessingTestCase(unittest.TestCase):   
    temp_dir: str
    daily_ds: xr.Dataset
    
    class Granule():
        def __init__(self, title) -> None:
            self.title = title
    
    @classmethod
    def setUpClass(cls) -> None:
        configure_logging(False, 'DEBUG', True)
        
        paths = ['tests/testing_granules/gsfc/Merged_TOPEX_Jason_OSTM_Jason-3_Cycle_1008.V5_1.nc']
        processed_files = []
        for path in paths:
            with open(path, 'rb') as file_obj:
                processed_files.append(GSFCDailyFile(file_obj, datetime(2020,1,28), 'C2204129664-POCLOUD').ds)
        granules = [cls.Granule(p.split('/')[-1]) for p in paths]
        cls.daily_ds = merge_passes(processed_files, granules)

    def test_file_date_coverage(self):
        self.assertGreaterEqual(self.daily_ds.time.values.min(), np.datetime64('2020-01-28'))
        self.assertLessEqual(self.daily_ds.time.values.max(), np.datetime64('2020-01-28T23:59:59'))
        
    def test_source_attr(self):
        self.assertEqual(self.daily_ds.attrs['source_files'], 'Merged_TOPEX_Jason_OSTM_Jason-3_Cycle_1008.V5_1.nc')
    
