import os
import tempfile
import unittest
from io import BytesIO
from unittest.mock import patch

import numpy as np
import xarray as xr
from daily_files.ingestion import gsfc_ingest
from daily_files.ingestion.gsfc_ingest import GSFCIngestor
from daily_files.ingestion.ingest import IngestedData


def _to_bytes(ds: xr.Dataset) -> BytesIO:
    """Serialize a dataset to an in-memory NetCDF buffer (h5netcdf closes the
    BytesIO it writes to, so route through a temp file)."""
    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
        path = tmp.name
    try:
        ds.to_netcdf(path, engine="h5netcdf")
        with open(path, "rb") as f:
            return BytesIO(f.read())
    finally:
        os.remove(path)


def _make_reference_bytes(cycle: int, ssha_mm: np.ndarray,
                          reference_orbit: np.ndarray, index: np.ndarray) -> BytesIO:
    """Build a synthetic GSFC REFERENCE (main product) pass file. merged_cycle is
    a global attr; reference_orbit/index drive the pass lookup."""
    n = ssha_mm.size
    ds = xr.Dataset(
        data_vars={
            "ssha": (("N_Records",), ssha_mm.astype(np.float32)),
            "lat": (("N_Records",), np.linspace(-66, 66, n, dtype=np.float32)),
            "lon": (("N_Records",), np.linspace(0, 359, n, dtype=np.float32)),
            "time": (("N_Records",), np.arange(n, dtype=np.float64)),
            "reference_orbit": (("N_Records",), reference_orbit.astype(np.int32)),
            "index": (("N_Records",), index.astype(np.int32)),
            "flag": (("N_Records",), np.zeros(n, dtype=np.int32), {"flag_meanings": "good bad"}),
            "Surface_Type": (("N_Records",), np.zeros(n, dtype=np.int32)),
        },
        attrs={"merged_cycle": cycle},
    )
    return _to_bytes(ds)


def _make_flavor_bytes(ssha_mm: np.ndarray) -> BytesIO:
    """Build a synthetic GSFC flavor file (IB_APPLIED / NO_ATMOS): only ssha is read."""
    ds = xr.Dataset({"ssha": (("N_Records",), ssha_mm.astype(np.float32))})
    return _to_bytes(ds)


# reference_orbit 127 with these indices maps to passes [254, 254, 1, 1] in the
# pass LUT, giving the descending step _compute_cycles_passes requires.
_ORBIT = np.array([127, 127, 127, 127])
_INDEX = np.array([5059, 5060, 5061, 5062])


class TestGSFCIngest(unittest.TestCase):
    def test_ingest_requires_bucket(self):
        with self.assertRaises(ValueError):
            GSFCIngestor().ingest([], bucket=None)

    def test_ingest_end_to_end(self):
        ssha_mm = np.array([100.0, 200.0, 300.0, 400.0])
        no_atmos_mm = ssha_mm + 50.0   # dac  = 50 mm  -> 0.05 m
        ib_mm = ssha_mm + 20.0         # ib   = 30 mm applied differently

        ref_buf = _make_reference_bytes(185, ssha_mm, _ORBIT, _INDEX)
        # _compute_dac_and_inv_bar streams ib first, then no_atmos, per cycle.
        with patch.object(gsfc_ingest.aws_manager, "stream_obj",
                          side_effect=[_make_flavor_bytes(ib_mm), _make_flavor_bytes(no_atmos_mm)]):
            ingested = GSFCIngestor().ingest([ref_buf], bucket="unit-test-bucket")

        self.assertIsInstance(ingested, IngestedData)
        self.assertEqual(len(ingested.ssha), 4)
        # dac = no_atmos - ssha = 0.05 m everywhere
        np.testing.assert_allclose(ingested.dac, 0.05, atol=1e-6)
        # inv_bar_cor = no_atmos - ib = 0.03 m everywhere
        np.testing.assert_allclose(ingested.inv_bar_cor, 0.03, atol=1e-6)
        # og_ds carries only flag + Surface_Type
        self.assertEqual(set(ingested.source_specific["og_ds"].data_vars), {"flag", "Surface_Type"})
        self.assertEqual(len(ingested.passes), 4)

    def test_compute_dac_and_inv_bar_shape_mismatch_raises(self):
        ssha = np.array([0.1, 0.2, 0.3, 0.4])  # 4 records
        ib_short = _make_flavor_bytes(np.array([100.0, 200.0]))       # 2 records
        no_atmos = _make_flavor_bytes(np.array([150.0, 250.0, 350.0]))  # 3 records
        with patch.object(gsfc_ingest.aws_manager, "stream_obj",
                          side_effect=[ib_short, no_atmos]):
            with self.assertRaises(ValueError):
                GSFCIngestor._compute_dac_and_inv_bar(np.array([185]), ssha, "unit-test-bucket")


if __name__ == "__main__":
    unittest.main()
