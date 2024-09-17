import unittest
import xarray as xr
import numpy as np
from datetime import datetime
from daily_files.processing.gsfc_daily_file import GSFCDailyFile
from daily_files.daily_file_job import save_ds


class EndToEndGSFCProcessingTestCase(unittest.TestCase):
    temp_dir: str
    daily_ds: xr.Dataset

    class Granule:
        def __init__(self, title) -> None:
            self.title = title

    @classmethod
    def setUpClass(cls) -> None:
        cls.paths = [
            "tests/testing_granules/gsfc/Merged_TOPEX_Jason_OSTM_Jason-3_Sentinel-6_Cycle_0001.V5_2.nc"
            # "tests/testing_granules/gsfc/Merged_TOPEX_Jason_OSTM_Jason-3_Cycle_1080.V5_1.nc",
            # "tests/testing_granules/gsfc/Merged_TOPEX_Jason_OSTM_Jason-3_Cycle_1081.V5_1.nc",
        ]
        opened_paths = [open(p, "rb") for p in cls.paths]

        cls.daily_ds = GSFCDailyFile(
            # opened_paths, datetime(2022, 1, 18), ["C2901523432-POCLOUD"]
            opened_paths,
            datetime(1992, 10, 1),
            ["C2901523432-POCLOUD"],
        ).ds
        [op.close() for op in opened_paths]
        granules = [cls.Granule(p.split("/")[-1]) for p in cls.paths]
        cls.daily_ds.attrs["source_files"] = ", ".join([g.title for g in granules])
        save_ds(cls.daily_ds, "tests/testing_granules/gsfc_test_19921001.nc")

    def test_file_date_coverage(self):
        self.assertGreaterEqual(
            self.daily_ds["time"].values.min(), np.datetime64("1992-10-01")
        )
        self.assertLessEqual(
            self.daily_ds["time"].values.max(), np.datetime64("1992-10-01T23:59:59")
        )

    def test_source_attr(self):
        self.assertEqual(
            self.daily_ds.attrs["source_files"],
            ", ".join([p.split("/")[-1] for p in self.paths]),
        )
