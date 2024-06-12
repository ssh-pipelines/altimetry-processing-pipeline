from datetime import datetime, timedelta
import logging
import xarray as xr
from glob import glob 
import os
import unittest

from oer.compute_polygon_correction import create_polygon, evaluate_correction, apply_correction

class OerTestCase(unittest.TestCase):
    logging.root.handlers = []
    logging.basicConfig(
        level='INFO',
        format='[%(levelname)s] %(asctime)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    
    def stream_xovers(window_start: datetime, window_end: datetime, satellite: str) -> xr.Dataset:
        logging.info(f'Streaming crossover files between {window_start}, {window_end}...')
        files = sorted(glob('tests/test_xovers/*.nc'))
        files = filter(lambda x: os.path.basename(x) >= f'xovers_{satellite}-{window_start.strftime("%Y-%m-%d")}.nc', files)
        files = filter(lambda x: os.path.basename(x) <= f'xovers_{satellite}-{window_end.strftime("%Y-%m-%d")}.nc', files)
        ds = xr.open_mfdataset(sorted(list(files)), decode_times=False)
        return ds

    @classmethod
    def setUpClass(cls):
        date = datetime(1992,11,1) 
        satellite = 'GSFC'
        
        window_len = 10  # set window, since xover files "look forward" in time
        window_pad = 1  # padding to avoid edge effects at window end
        window_start = max(date - timedelta(window_len) - timedelta(window_pad), datetime(1992, 9, 25))
        window_end = date + timedelta(window_pad)
        
        logging.info(f'Testing {satellite} {date}')
        
        # Create polygon from crossovers
        xover_ds = cls.stream_xovers(window_start, window_end, satellite)
        cls.polygon_ds = create_polygon(xover_ds, date, satellite)
        
        # Create corrections from polygon and daily file
        cls.daily_file_ds = xr.open_dataset('tests/test_dailyfiles/GSFC-alt_ssh19921101.nc')
        cls.correction_ds = evaluate_correction(cls.polygon_ds, cls.daily_file_ds, date, satellite)
    
        # Apply correction to daily file
        cls.corrected_ds = apply_correction(cls.daily_file_ds, cls.correction_ds)
        if 'time' in cls.corrected_ds['basin_names_table'].dims:
            cls.corrected_ds['basin_names_table'] = cls.corrected_ds['basin_names_table'].isel(time=0)

    def test_polygon(self):
        self.assertIn('N_order', self.polygon_ds.dims)
        self.assertIn('N_intervals', self.polygon_ds.dims)
        self.assertIn('N_breaks', self.polygon_ds.dims)
    
    def test_correction(self):
        self.assertEqual(len(self.daily_file_ds.time), len(self.correction_ds.time))
        self.assertIn('oer', self.correction_ds.data_vars)
    
    def test_application(self):
        self.assertIn('oer', self.corrected_ds.data_vars)
        self.assertEqual(self.corrected_ds.attrs['product_generation_step'], '2')
        self.assertIn('orbit_error_correction', self.corrected_ds.ssh.attrs)
        self.assertIn('orbit_error_correction', self.corrected_ds.ssh_smoothed.attrs)