import unittest
import xarray as xr
import numpy as np
from datetime import datetime
from daily_files.processing.gsfc_daily_file import GSFC_DailyFile
from daily_files.daily_file_job import merge_passes, save_ds
from daily_files.utils.logconfig import configure_logging


class EndToEndGSFCProcessingTestCase(unittest.TestCase):   
    temp_dir: str
    daily_ds: xr.Dataset
    
    @classmethod
    def setUpClass(cls) -> None:
        configure_logging(False, 'INFO', True)
        
        paths = ['tests/testing_granules/gsfc/Merged_TOPEX_Jason_OSTM_Jason-3_Cycle_1080.V5_1.nc', 
                 'tests/testing_granules/gsfc/Merged_TOPEX_Jason_OSTM_Jason-3_Cycle_1081.V5_1.nc']
        processed_files = []
        for path in paths:
            with open(path, 'rb') as file_obj:
                processed_files.append(GSFC_DailyFile(file_obj, datetime(2022,1,18)).ds)
        granule_names = [p.split('/')[-1] for p in paths]
        cls.daily_ds = merge_passes(processed_files, granule_names)

    def test_file_date_coverage(self):
        self.assertGreaterEqual(self.daily_ds.time.values.min(), np.datetime64('2022-01-18'))
        self.assertLessEqual(self.daily_ds.time.values.max(), np.datetime64('2022-01-18T23:59:59'))
        
    def test_source_attr(self):
        self.assertEqual(self.daily_ds.attrs['source_files'], 'Merged_TOPEX_Jason_OSTM_Jason-3_Cycle_1080.V5_1.nc, Merged_TOPEX_Jason_OSTM_Jason-3_Cycle_1081.V5_1.nc')
    
