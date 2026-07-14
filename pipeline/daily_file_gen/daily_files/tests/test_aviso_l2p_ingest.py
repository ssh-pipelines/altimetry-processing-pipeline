import os
import tempfile
import unittest
from io import BytesIO

import numpy as np
import xarray as xr
from daily_files.ingestion.aviso_l2p_ingest import AvisoL2PIngestor
from daily_files.ingestion.ingest import IngestedData


def _make_l2p_bytes(cycle: int, pass_num: int, n: int, t_start: str) -> BytesIO:
    """Build a synthetic AVISO L2P pass NetCDF in memory. Schema mirrors the
    real wire format: cycle/pass come from global attrs, per-record vars hold
    SSHA, MSS, inter-mission bias, validation flag, and DAC."""
    rng = np.random.RandomState(cycle * 1000 + pass_num)
    times = np.arange(
        np.datetime64(t_start),
        np.datetime64(t_start) + np.timedelta64(n, "s"),
        np.timedelta64(1, "s"),
    ).astype("datetime64[ns]")[:n]
    ds = xr.Dataset(
        data_vars={
            "sea_level_anomaly": (("time",), rng.normal(0, 0.2, n).astype(np.float32)),
            "dynamic_atmospheric_correction": (
                ("time",), rng.normal(0, 0.01, n).astype(np.float32),
            ),
            "mean_sea_surface": (
                ("time",), rng.normal(20, 5, n).astype(np.float32),
            ),
            "inter_mission_bias": (
                ("time",), np.full(n, -0.047, dtype=np.float32),
            ),
            "validation_flag": (
                ("time",), rng.choice([0, 1], n, p=[0.9, 0.1]).astype(np.int8),
            ),
            "latitude": (("time",), np.linspace(-66, 81, n, dtype=np.float32)),
            "longitude": (("time",), np.linspace(232, 358, n, dtype=np.float32)),
        },
        coords={"time": times},
        attrs={"cycle_number": cycle, "pass_number": pass_num},
    )
    # h5netcdf closes the BytesIO after writing, so route through a temp file.
    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
        path = tmp.name
    try:
        ds.to_netcdf(path, engine="h5netcdf")
        with open(path, "rb") as f:
            return BytesIO(f.read())
    finally:
        os.remove(path)


class TestAvisoL2PIngest(unittest.TestCase):
    def test_single_pass_schema(self):
        buf = _make_l2p_bytes(cycle=101, pass_num=745, n=300, t_start="2025-01-06T23:55:00")
        ingested = AvisoL2PIngestor().ingest([buf])

        self.assertIsInstance(ingested, IngestedData)
        n = len(ingested.ssha)
        self.assertEqual(n, 300)
        for arr in [ingested.lat, ingested.lon, ingested.time,
                    ingested.cycles, ingested.passes,
                    ingested.dac, ingested.inv_bar_cor]:
            self.assertEqual(len(arr), n)

        self.assertTrue(np.all(ingested.cycles == 101))
        self.assertTrue(np.all(ingested.passes == 745))
        self.assertEqual(ingested.cycles.dtype, np.int32)
        self.assertEqual(ingested.passes.dtype, np.int32)

    def test_inv_bar_cor_zero_filled(self):
        buf = _make_l2p_bytes(cycle=101, pass_num=745, n=100, t_start="2025-01-07T00:00:00")
        ingested = AvisoL2PIngestor().ingest([buf])
        self.assertTrue(np.all(ingested.inv_bar_cor == 0.0))
        self.assertEqual(ingested.inv_bar_cor.dtype, np.float64)

    def test_source_specific_carries_l2p_extras(self):
        buf = _make_l2p_bytes(cycle=101, pass_num=745, n=100, t_start="2025-01-07T00:00:00")
        ingested = AvisoL2PIngestor().ingest([buf])
        for key in ("original_ds", "mean_sea_surface",
                    "inter_mission_bias", "validation_flag"):
            self.assertIn(key, ingested.source_specific)
        self.assertIsInstance(
            ingested.source_specific["original_ds"], xr.Dataset
        )

    def test_multi_pass_concat_is_time_sorted(self):
        """Pass files supplied out of order are concatenated and time-sorted;
        cycle/pass broadcast preserves per-granule identity."""
        buf_a = _make_l2p_bytes(cycle=101, pass_num=746, n=50, t_start="2025-01-07T01:00:00")
        buf_b = _make_l2p_bytes(cycle=101, pass_num=745, n=50, t_start="2025-01-07T00:00:00")
        ingested = AvisoL2PIngestor().ingest([buf_a, buf_b])

        times_ns = ingested.time.astype("datetime64[ns]").astype(np.int64)
        self.assertTrue(np.all(np.diff(times_ns) >= 0))
        # First half (earlier times) came from pass 745, second half from 746
        self.assertTrue(np.all(ingested.passes[:50] == 745))
        self.assertTrue(np.all(ingested.passes[50:] == 746))


if __name__ == "__main__":
    unittest.main()
