import unittest
import xarray as xr
import numpy as np
from glob import glob
from datetime import datetime
from daily_files.daily_file_job import save_ds
from daily_files.processing.s6_daily_file import S6DailyFile


class EndToEndGSFCProcessingTestCase(unittest.TestCase):
    temp_dir: str
    daily_ds: xr.Dataset

    class Granule:
        def __init__(self, title) -> None:
            self.title = title

    @classmethod
    def setUpClass(cls) -> None:
        cls.paths = glob("tests/testing_granules/s6/*202312*.nc")
        cls.paths.sort()

        opened_paths = [open(p, "rb") for p in cls.paths]

        daily_file_job = S6DailyFile(opened_paths, datetime(2023, 12, 17), ["C2619443998-POCLOUD"])
        cls.daily_ds = daily_file_job.ds
        [op.close() for op in opened_paths]

        og_ds = daily_file_job.original_ds.where(~np.isnat(daily_file_job.original_ds["time"]), drop=True)
        today = str(datetime(2023, 12, 17))[:10]
        og_ds = og_ds.sel(time=today)
        og_ds = og_ds.drop_duplicates(dim="time")

        granules = [cls.Granule(p.split("/")[-1]) for p in cls.paths]
        cls.daily_ds.attrs["source_files"] = ", ".join([g.title for g in granules])
        save_ds(cls.daily_ds, "tests/testing_granules/s6_test_20231217.nc")

    def test_file_date_coverage(self):
        self.assertGreaterEqual(self.daily_ds["time"].values.min(), np.datetime64("2023-12-17"))
        self.assertLessEqual(self.daily_ds["time"].values.max(), np.datetime64("2023-12-17T23:59:59"))

    def test_source_attr(self):
        self.assertEqual(
            self.daily_ds.attrs["source_files"],
            ", ".join([p.split("/")[-1] for p in self.paths]),
        )
