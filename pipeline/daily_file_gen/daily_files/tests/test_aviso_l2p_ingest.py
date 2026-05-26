import os
import unittest

import numpy as np
import xarray as xr

from daily_files.ingestion.aviso_l2p_ingest import AvisoL2PIngestor
from daily_files.ingestion.ingest import IngestedData


_FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
S3B_SAMPLE = os.path.join(
    _FIXTURE_DIR,
    "global_sla_l2p_ntc_s3b_C0101_P0745_20250106T234659_20250107T003225_20250213T162727.nc",
)


class TestAvisoL2PIngest(unittest.TestCase):
    def test_single_pass_schema(self):
        ingestor = AvisoL2PIngestor()
        with open(S3B_SAMPLE, "rb") as f:
            ingested = ingestor.ingest([f])

        self.assertIsInstance(ingested, IngestedData)
        n = len(ingested.ssha)
        self.assertGreater(n, 0)
        # All time-aligned arrays share the same length
        for arr in [ingested.lat, ingested.lon, ingested.time,
                    ingested.cycles, ingested.passes,
                    ingested.dac, ingested.inv_bar_cor]:
            self.assertEqual(len(arr), n)

        # cycle/pass come from per-file global attrs (C0101 / P0745)
        self.assertTrue(np.all(ingested.cycles == 101))
        self.assertTrue(np.all(ingested.passes == 745))
        self.assertEqual(ingested.cycles.dtype, np.int32)
        self.assertEqual(ingested.passes.dtype, np.int32)

    def test_inv_bar_cor_zero_filled(self):
        ingestor = AvisoL2PIngestor()
        with open(S3B_SAMPLE, "rb") as f:
            ingested = ingestor.ingest([f])
        self.assertTrue(np.all(ingested.inv_bar_cor == 0.0))
        self.assertEqual(ingested.inv_bar_cor.dtype, np.float64)

    def test_source_specific_carries_l2p_extras(self):
        ingestor = AvisoL2PIngestor()
        with open(S3B_SAMPLE, "rb") as f:
            ingested = ingestor.ingest([f])
        for key in ("original_ds", "mean_sea_surface",
                    "inter_mission_bias", "validation_flag"):
            self.assertIn(key, ingested.source_specific)
        self.assertIsInstance(
            ingested.source_specific["original_ds"], xr.Dataset
        )

    def test_multi_file_concat_is_time_sorted(self):
        """Two copies of the same granule open in either order produce a
        time-monotonic concat (the ingestor argsort enforces this)."""
        ingestor = AvisoL2PIngestor()
        with open(S3B_SAMPLE, "rb") as f1, open(S3B_SAMPLE, "rb") as f2:
            ingested = ingestor.ingest([f1, f2])
        times = ingested.time
        self.assertTrue(np.all(np.diff(times.astype("datetime64[ns]").astype(np.int64)) >= 0))
        # cycles/passes broadcast: every record carries the granule's (cycle, pass)
        self.assertTrue(np.all(ingested.cycles == 101))
        self.assertTrue(np.all(ingested.passes == 745))


if __name__ == "__main__":
    unittest.main()
