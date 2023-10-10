import unittest
import xarray as xr
import numpy as np
from datetime import datetime
from daily_files.daily_file_generation import merge_passes
from daily_files.processing import gsfc_processing


class EndToEndGSFCProcessingTestCase(unittest.TestCase):   
    temp_dir: str
    daily_ds: xr.Dataset
    
    @classmethod
    def setUpClass(cls) -> None:      
        paths = ['tests/testing_granules/gsfc/Merged_TOPEX_Jason_OSTM_Jason-3_Cycle_1080.V5_1.nc', 
                 'tests/testing_granules/gsfc/Merged_TOPEX_Jason_OSTM_Jason-3_Cycle_1081.V5_1.nc']
        processed_files = []
        for path in paths:
            ds = xr.open_dataset(path)
            processed_files.append(gsfc_processing(ds, datetime(2022,1,18)))
        cls.daily_ds = merge_passes(processed_files, paths)


    def test_file_date_coverage(self):
        self.assertGreaterEqual(self.daily_ds.time.values.min(), np.datetime64('2022-01-18'))
        self.assertLessEqual(self.daily_ds.time.values.max(), np.datetime64('2022-01-18T23:59:59'))
    
