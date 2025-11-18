import logging
import os
import unittest
import xarray as xr
from glob import glob
from datetime import datetime, timedelta
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
                
        cur_date = datetime(2024,9,16)
        while cur_date < datetime(2025,3,15):
            cls.source = 'GSFC'
            cls.date = cur_date.strftime("%Y-%m-%d")
            cls.resolution = 'quart'
            
            simple_gridder_job = SimpleGridderJob(cls.date, cls.source, cls.resolution)
            simple_gridder_job.center_date, simple_gridder_job.start_date, simple_gridder_job.end_date
            
            # Get paths in window
            files = sorted(glob("/Users/marlis/Desktop/df_for_quart_grids/*.nc"))

            files = filter(
                lambda x: os.path.basename(x) >= f'NASA-SSH_alt_ref_at_v1_{simple_gridder_job.start_date.strftime("%Y%m%d")}.nc',
                files,
            )
            files = filter(
                lambda x: os.path.basename(x) <= f'NASA-SSH_alt_ref_at_v1_{simple_gridder_job.end_date.strftime("%Y%m%d")}.nc',
                files,
            )
            
            streamed_files = sorted(list(files))

            filenames = [os.path.basename(f) for f in streamed_files]
            
            gridder = Gridder(simple_gridder_job.center_date, simple_gridder_job.start_date, simple_gridder_job.end_date, filenames, streamed_files, cls.resolution)       
            
            cls.ds = gridder.make_grid(simple_gridder_job.filename)
            simple_gridder_job.save_grid(cls.ds, '/Users/marlis/Desktop/quart_grids',)
            
            cur_date = cur_date + timedelta(days=7)


    def test_file_date_coverage(self):
        # self.assertGreaterEqual(self.daily_ds.time.values.min(), np.datetime64('2022-01-18'))
        # self.assertLessEqual(self.daily_ds.time.values.max(), np.datetime64('2022-01-18T23:59:59'))
        pass
    def test_source_attr(self):
        # self.assertEqual(self.daily_ds.attrs['source_files'], ', '.join([p.split('/')[-1] for p in self.paths]))
        pass
