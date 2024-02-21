import unittest
import numpy as np
import xarray as xr
from daily_files.processing.smoothing import smooth, mirror_nans, nan_check, make_windows, pad_df, ssh_smoothing
from daily_files.utils.logconfig import configure_logging


class EndToEndSmoothingTestCase(unittest.TestCase):   

    @classmethod
    def setUpClass(cls) -> None:
        # configure_logging(False, 'DEBUG', True)
        
        cls.n = 100
        
        cls.ssh: np.ndarray = np.ones(cls.n + 18)
        cls.flag: np.ndarray = np.full_like(cls.ssh, 0)
        cls.time: np.ndarray = np.arange('2019-12-31T23:59:51', '2020-01-02', 1, dtype='datetime64[s]').astype('datetime64[ns]')[:cls.n + 18]
        cls.og_ds: xr.Dataset = xr.Dataset(
            {'ssh': (('time'), cls.ssh),
             'nasa_flag': (('time'), cls.flag)},
            {'time': cls.time}
        )
        cls.smooth_ds: xr.Dataset = ssh_smoothing(cls.og_ds)
        
    def test_mirror_nans(self):
        arr = np.array([1, np.nan, 2, 3])
        self.assertEqual(mirror_nans(arr).all(), np.array([1, np.nan, np.nan, 3]).all())
        arr = np.array([1, np.nan, 2, np.nan, 3])
        self.assertEqual(mirror_nans(arr).all(), arr.all())
        
    def test_nan_check(self):
        arr = np.full(19, np.nan)
        self.assertTrue(nan_check(arr), 'All nans')
        arr = np.full(19, 1, dtype='float32')
        self.assertFalse(nan_check(arr), 'No nans')
        arr = np.full(19, np.nan)
        arr[6:13] = 1
        self.assertFalse(nan_check(arr), 'Outer nans')
        arr = np.full(19, 1, dtype='float32')
        arr[9] = np.nan
        self.assertFalse(nan_check(arr), 'Center nan')
        arr = np.full(19, 1, dtype='float32')
        arr[6:13] = np.nan
        arr[9] = 1
        self.assertTrue(nan_check(arr), 'Middle nans, center nonnan')
        
    def test_windows(self):
        padded_df = pad_df(self.og_ds)
        windows = make_windows(padded_df)
        for window in windows:
            if (len(window) != 19):
                self.assertTrue((window.index.values <= np.datetime64('2020-01-01', 's')).all() | (window.index.values >= np.datetime64('2020-01-01T00:01:00', 's')).all())
        
    def test_no_nans(self):
        arr = np.full(19, 1, dtype='float32')
        smoothed_val = smooth(arr)
        self.assertEqual(1, smoothed_val)
        
    def test_all_nans(self):
        arr = np.full(19, np.nan)
        smoothed_val = smooth(arr)
        self.assertIs(np.nan, smoothed_val)
        
    def test_center_nans(self):
        arr = np.full(19, 1, dtype='float32')
        arr[9] = np.nan
        smoothed_val = smooth(arr)
        self.assertAlmostEqual(1, smoothed_val, 5)
        
    def test_smoothing(self):
        arr = np.array([1,2,3,4,5,6,7,8,9,10,9,8,7,6,5,4,3,2,1])
        smoothed_val = smooth(arr)
        self.assertAlmostEqual(8.82084, smoothed_val, 5)