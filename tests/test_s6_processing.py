import pickle
import unittest
import xarray as xr
import numpy as np
from glob import glob
from datetime import datetime
from daily_files.daily_file_job import save_ds
from daily_files.processing.s6_daily_file import S6DailyFile
from daily_files.utils.logconfig import configure_logging


class EndToEndGSFCProcessingTestCase(unittest.TestCase):   
    temp_dir: str
    daily_ds: xr.Dataset
    
    class Granule():
        def __init__(self, title) -> None:
            self.title = title
    
    @classmethod
    def setUpClass(cls) -> None:
        configure_logging(False, 'INFO', True)
        
        cls.paths = glob('tests/testing_granules/s6/*.nc')
        cls.paths.sort()

        opened_paths = [open(p, 'rb') for p in cls.paths]
        
        daily_file_job = S6DailyFile(opened_paths, datetime(2023,12,17), ['C2619443998-POCLOUD'])
        cls.daily_ds = daily_file_job.ds
        [op.close() for op in opened_paths]
        
        mss_path = 'tests/testing_granules/mss_interps/DTU18_interp_to_DTU21.pkl'
        with open(mss_path, 'rb') as f:
            mss_interp = pickle.load(f)
            
        mss_corr_interponrads =mss_interp.ev(cls.daily_ds.latitude, cls.daily_ds.longitude)
        
        og_ds = daily_file_job.original_ds.where(~np.isnat(daily_file_job.original_ds.time), drop=True)
        today = str(datetime(2023,12,17))[:10]
        og_ds = og_ds.sel(time=today)
        og_ds = og_ds.drop_duplicates(dim='time')
        
        swapped_vals = cls.daily_ds.ssh.values + og_ds['mean_sea_surface_sol1'].values - og_ds['mean_sea_surface_sol2'].values - mss_corr_interponrads
        cls.daily_ds.ssh.values = swapped_vals
        cls.daily_ds = cls.daily_ds.drop_vars(['mean_sea_surface_sol1', 'mean_sea_surface_sol2'])
        
        granules = [cls.Granule(p.split('/')[-1]) for p in cls.paths]
        cls.daily_ds.attrs['source_files'] = ', '.join([g.title for g in granules])
        save_ds(cls.daily_ds, 'tests/testing_granules/s6_test.nc')

    def test_file_date_coverage(self):
        self.assertGreaterEqual(self.daily_ds.time.values.min(), np.datetime64('2023-12-17'))
        self.assertLessEqual(self.daily_ds.time.values.max(), np.datetime64('2023-12-17T23:59:59'))
        
    def test_source_attr(self):
        self.assertEqual(self.daily_ds.attrs['source_files'], ', '.join([p.split('/')[-1] for p in self.paths]))
    
