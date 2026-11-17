import os
import tempfile
import unittest
from io import BytesIO
from unittest.mock import patch

import netCDF4 as nc
import numpy as np
import requests
from daily_files.ingestion import s6_ingest
from daily_files.ingestion.ingest import IngestedData
from daily_files.ingestion.orbit_swap import OrbitSwapError
from daily_files.ingestion.s6_ingest import S6Ingestor

_DATA_01_VARS = [
    "latitude",
    "longitude",
    "surface_classification_flag",
    "rain_flag_nr",
    "rad_water_vapor_qual",
    "dac",
    "inv_bar_cor",
    "mean_sea_surface_sol1",
    "mean_sea_surface_sol2",
]
_KU_VARS = ["sig0_ocean_nr", "range_ocean_nr_qual", "swh_ocean_nr", "ssha_nr"]


def _make_s6_bytes(cycle: int, pass_num: int, n: int) -> BytesIO:
    """Build a synthetic Sentinel-6 LR pass file mirroring the grouped wire format
    (data_01 + data_01/ku) that _extract_grouped_data reads."""
    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
        path = tmp.name
    try:
        ds = nc.Dataset(path, "w")
        ds.cycle_number = cycle
        ds.pass_number = pass_num
        d01 = ds.createGroup("data_01")
        d01.createDimension("time", n)
        d01.createVariable("time", "f8", ("time",))[:] = np.arange(n, dtype=np.float64)
        for var in _DATA_01_VARS:
            d01.createVariable(var, "f4", ("time",))[:] = np.linspace(0, 1, n, dtype=np.float32)
        ku = d01.createGroup("ku")
        for var in _KU_VARS:
            ku.createVariable(var, "f4", ("time",))[:] = np.linspace(0, 1, n, dtype=np.float32)
        ds.close()
        with open(path, "rb") as f:
            return BytesIO(f.read())
    finally:
        os.remove(path)


class TestS6Ingest(unittest.TestCase):
    def test_ingest_extracts_grouped_schema(self):
        buf = _make_s6_bytes(cycle=252, pass_num=211, n=6)
        ingested = S6Ingestor().ingest([buf])

        self.assertIsInstance(ingested, IngestedData)
        self.assertEqual(len(ingested.ssha), 6)
        for arr in [ingested.lat, ingested.lon, ingested.time,
                    ingested.dac, ingested.inv_bar_cor]:
            self.assertEqual(len(arr), 6)
        self.assertTrue(np.all(ingested.cycles == 252))
        self.assertTrue(np.all(ingested.passes == 211))
        for key in ("original_ds", "mean_sea_surface_sol1", "mean_sea_surface_sol2"):
            self.assertIn(key, ingested.source_specific)

    def test_unopenable_file_is_skipped(self):
        # A good file plus a garbage buffer: the bad one is skipped, the good one
        # still yields a product (exercises the open-loop except/continue).
        good = _make_s6_bytes(cycle=252, pass_num=211, n=5)
        bad = BytesIO(b"not a netcdf file")
        ingested = S6Ingestor().ingest([good, bad])
        self.assertEqual(len(ingested.ssha), 5)

    def _ingest_with_swap(self, swap_return, fetch_return="/fake/orbit.nc", n=6):
        """Run ingest() with filenames set (orbit-swap path) while mocking the
        orbit fetcher and the swap executable."""
        buf = _make_s6_bytes(cycle=252, pass_num=211, n=n)
        with patch.object(s6_ingest, "OrbitFetcher") as mock_of, \
                patch.object(s6_ingest, "run_orbit_swap") as mock_swap:
            mock_of.return_value.fetch.return_value = fetch_return
            mock_swap.return_value = swap_return
            return S6Ingestor().ingest([buf], filenames=["S6_test_pass.nc"])

    def test_orbit_swap_applied(self):
        swapped = np.arange(6, dtype=np.float64)
        ingested = self._ingest_with_swap(swap_return=swapped)
        np.testing.assert_allclose(ingested.ssha, swapped)

    # Unswapped ssha_nr must never reach a daily file, so every swap failure
    # fails the job rather than falling back to the original values.
    def test_orbit_download_failure_fails_job(self):
        buf = _make_s6_bytes(cycle=252, pass_num=211, n=6)
        with patch.object(s6_ingest, "OrbitFetcher") as mock_of, \
                patch.object(s6_ingest, "run_orbit_swap") as mock_swap:
            mock_of.return_value.fetch.side_effect = requests.HTTPError("404 Not Found")
            with self.assertRaisesRegex(OrbitSwapError, "404"):
                S6Ingestor().ingest([buf], filenames=["S6_test_pass.nc"])
        mock_swap.assert_not_called()

    def test_orbit_swap_length_mismatch_fails_job(self):
        with self.assertRaisesRegex(OrbitSwapError, "length mismatch"):
            self._ingest_with_swap(swap_return=np.arange(3))

    def test_orbit_swap_error_fails_job(self):
        buf = _make_s6_bytes(cycle=252, pass_num=211, n=6)
        with patch.object(s6_ingest, "OrbitFetcher") as mock_of, \
                patch.object(s6_ingest, "run_orbit_swap", side_effect=RuntimeError("boom")):
            mock_of.return_value.fetch.return_value = "/fake/orbit.nc"
            with self.assertRaisesRegex(OrbitSwapError, "boom"):
                S6Ingestor().ingest([buf], filenames=["S6_test_pass.nc"])

    def test_orbit_swap_error_fails_job_despite_good_files(self):
        # One failed pass among good ones still fails the whole job.
        good = _make_s6_bytes(cycle=252, pass_num=211, n=6)
        bad = _make_s6_bytes(cycle=252, pass_num=212, n=6)
        with patch.object(s6_ingest, "OrbitFetcher") as mock_of, \
                patch.object(s6_ingest, "run_orbit_swap",
                             side_effect=[np.arange(6.0), OrbitSwapError("outside span")]):
            mock_of.return_value.fetch.return_value = "/fake/orbit.nc"
            with self.assertRaises(OrbitSwapError):
                S6Ingestor().ingest([good, bad], filenames=["a.nc", "b.nc"])


if __name__ == "__main__":
    unittest.main()
