import logging
import unittest
import xarray as xr
import numpy as np
from datetime import datetime
from daily_files.config.source_config import get_source_config
from daily_files.ingestion.ingest import IngestedData
from daily_files.processing.gsfc_daily_file import GSFCDailyFile
from daily_files.daily_file_job import save_ds
from daily_files.config.dataset_schema import validate_dataset


def _make_gsfc_ingested_data(n=500, date=datetime(1995, 6, 7)):
    """Build a synthetic IngestedData mimicking GSFC ingestor output."""
    rng = np.random.RandomState(42)
    times = np.arange(
        np.datetime64(date),
        np.datetime64(date) + np.timedelta64(1, "D"),
        np.timedelta64(86400 // n, "s"),
    ).astype("datetime64[ns]")[:n]

    ssha = rng.normal(0, 0.1, n)
    lats = np.linspace(-66, 66, n)
    lons = np.linspace(0, 360, n, endpoint=False)
    cycles = np.full(n, 100, dtype=np.int32)
    passes = np.tile(np.arange(1, 11), n // 10 + 1)[:n].astype(np.int32)
    dac = rng.normal(0, 0.01, n)

    # Build a minimal og_ds with the fields GSFCDailyFile.make_nasa_flag expects.
    # Real GSFC data has 15 flag bits (src_flag_dim=15 in the empty template).
    max_flag_bit = 15
    flag_values = rng.randint(0, 2**max_flag_bit, n).astype(np.int32)
    surface_type = rng.choice([0, 2, 1], n, p=[0.8, 0.15, 0.05]).astype(np.int32)
    flag_meanings = " ".join(
        [
            "abs(SSH(cycle)-SSH(cycle +/-1))>50cm",
            "Radiometer_Observation_is_Suspect",
            "Attitude_Out_of_Range",
            "Sigma0_Ku_Band_Out_of_Range",
            "Possible_Rain_Contamination",
            "Sea_Ice_Detected",
            "Significant_Wave_Height>8m",
            "Cross_Track_slope>10cm/km",
            "Cross_Track_Distance>1km",
            "Any_Applied_SSH_Correction_Out_of_Limits",
            "Contiguous_1Hz_Data",
            "Sigma_H_of_fit>15cm",
            "Distance_to_Land<50km",
            "Water_Depth<200m",
            "Single_Frequency_Altimeter",
        ]
    )
    og_ds = xr.Dataset(
        {
            "flag": (("N_Records",), flag_values, {"flag_meanings": flag_meanings}),
            "Surface_Type": (("N_Records",), surface_type),
        }
    )

    inv_bar_cor = rng.normal(0, 0.01, n)

    return IngestedData(
        ssha=ssha,
        lat=lats,
        lon=lons,
        time=times,
        cycles=cycles,
        passes=passes,
        dac=dac,
        inv_bar_cor=inv_bar_cor,
        source_specific={"og_ds": og_ds},
    )


class TestGSFCProcessing(unittest.TestCase):
    daily_ds: xr.Dataset

    @classmethod
    def setUpClass(cls):
        logging.root.handlers = []
        logging.basicConfig(
            level="INFO",
            format="[%(levelname)s] %(asctime)s - %(message)s",
            handlers=[logging.StreamHandler()],
        )

        cls.date = datetime(1995, 6, 7)
        source_config = get_source_config("GSFC")
        ingested = _make_gsfc_ingested_data(date=cls.date)
        cls.daily_ds = GSFCDailyFile(
            ingested,
            cls.date,
            source_config,
            source_files="test_granule.nc",
        ).ds

    def test_has_required_vars(self):
        for var in [
            "ssha",
            "ssha_smoothed",
            "nasa_flag",
            "source_flag",
            "median_filter_flag",
            "dac",
            "inv_bar_cor",
            "basin_flag",
            "basin_names_table",
            "latitude",
            "longitude",
            "cycle",
            "pass",
        ]:
            self.assertIn(var, self.daily_ds, f"Missing variable: {var}")

    def test_file_date_coverage(self):
        if len(self.daily_ds["time"]) == 0:
            self.skipTest("No data after subsetting")
        self.assertGreaterEqual(self.daily_ds["time"].values.min(), np.datetime64("1995-06-07"))
        self.assertLessEqual(self.daily_ds["time"].values.max(), np.datetime64("1995-06-07T23:59:59"))

    def test_nasa_flag_values(self):
        flag_vals = np.unique(self.daily_ds["nasa_flag"].values)
        for v in flag_vals:
            self.assertIn(v, [0, 1, True, False])

    def test_absolute_offset_attr(self):
        self.assertEqual(self.daily_ds.attrs["absolute_offset_applied"], 0)

    def test_mean_sea_surface_attr(self):
        self.assertEqual(self.daily_ds.attrs["mean_sea_surface"], "DTU21")

    def test_source_attrs_populated(self):
        for attr in ["source", "source_url", "references"]:
            self.assertNotEqual(self.daily_ds.attrs[attr], "")

    def test_source_flag_dimensions(self):
        """source_flag should have 15 columns matching real GSFC data (src_flag_dim=15)."""
        self.assertEqual(self.daily_ds["source_flag"].shape[1], 15)

    def test_schema_validation(self):
        errors = validate_dataset(self.daily_ds)
        self.assertEqual(errors, [], f"Schema validation errors: {errors}")

    def test_save_ds(self):
        """Verify the dataset can be serialized to NetCDF without error."""
        import tempfile, os

        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "test_gsfc.nc")
            save_ds(self.daily_ds, path)
            self.assertTrue(os.path.exists(path))
            with xr.open_dataset(path) as ds:
                self.assertIn("ssha", ds)
