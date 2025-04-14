import logging
import os
import unittest
import xarray as xr
from datetime import datetime
from glob import glob
from enso_jobs.ensogridder import ENSOGridder
from enso_jobs.ensomapper import ENSOMapper

logging.root.handlers = []
logging.basicConfig(
    level="INFO",
    format="[%(levelname)s] %(asctime)s - %(message)s",
    handlers=[logging.StreamHandler()],
)


class EndToEndGSFCProcessingTestCase(unittest.TestCase):
    temp_dir: str
    daily_ds: xr.Dataset

    @classmethod
    def setUpClass(cls) -> None:
        try:
            grid_processer = ENSOGridder()
            mapper = ENSOMapper()
        except Exception as e:
            logging.exception(e)
            raise RuntimeError(e)

        try:
            # Make grids
            grid_paths = sorted(
                glob(
                    "/Users/username/Developer/Measures-Cloud/data/simple_grids/p3/2024/*.nc"
                )
            )
            # grid_paths = ['/Users/username/Desktop/NASA-SSH_alt_ref_simple_grid_v1_20240923.nc']
            for path in grid_paths:
                filename = os.path.basename(path)
                date = datetime.strptime(
                    os.path.splitext(filename)[0].split("_")[-1], "%Y%m%d"
                )

                grid_ds = grid_processer.process_grid(path, date)
                logging.info("Grid making complete")

                # Make maps
                mapper.make_maps(grid_ds)
                logging.info("Map making complete")

        except Exception as e:
            logging.exception(f"Error processing {date}: {e}")

    def test_file_date_coverage(self):
        # self.assertGreaterEqual(self.daily_ds.time.values.min(), np.datetime64('2022-01-18'))
        # self.assertLessEqual(self.daily_ds.time.values.max(), np.datetime64('2022-01-18T23:59:59'))
        pass
