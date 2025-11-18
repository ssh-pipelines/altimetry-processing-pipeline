import logging
import os
import unittest
import xarray as xr

from glob import glob
from simple_gridder.gridding import Gridder
from simple_gridder.simple_gridder import SimpleGridderJob


class EndToEndGSFCProcessingTestCase(unittest.TestCase):   
    temp_dir: str
    daily_ds: xr.Dataset
    
    @classmethod
    def setUpClass(cls) -> None:
        logging.root.handlers = []
        logging.basicConfig(
            level="INFO", format="[%(levelname)s] %(asctime)s - %(message)s", handlers=[logging.StreamHandler()]
        )        
        
        cls.paths = sorted(glob('tests/test_granules/S6/*.nc'))
        cls.source = 'S6'
        cls.date = '2024-09-30'
        cls.resolution = None

        simple_gridder_job = SimpleGridderJob(cls.date, cls.source, cls.resolution)
        
        streamed_files = sorted(glob('tests/test_granules/S6/*.nc'))
        filenames = [os.path.basename(f) for f in streamed_files]
        
        gridder = Gridder(simple_gridder_job.center_date, simple_gridder_job.start_date, simple_gridder_job.end_date, filenames, streamed_files, cls.resolution)       
        gridder.granule_paths = cls.paths
        
        cls.ds = gridder.make_grid(simple_gridder_job.filename)        
        simple_gridder_job.save_grid(cls.ds, 'tests/test_granules')


    def test_file_date_coverage(self):
        # self.assertGreaterEqual(self.daily_ds.time.values.min(), np.datetime64('2022-01-18'))
        # self.assertLessEqual(self.daily_ds.time.values.max(), np.datetime64('2022-01-18T23:59:59'))
        pass
    def test_source_attr(self):
        # self.assertEqual(self.daily_ds.attrs['source_files'], ', '.join([p.split('/')[-1] for p in self.paths]))
        pass
