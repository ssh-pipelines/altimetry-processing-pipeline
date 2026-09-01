import unittest
from datetime import datetime
from unittest.mock import patch

import numpy as np
import xarray as xr


class TestSimpleGridsHandlerOutcome(unittest.TestCase):

    @patch("app.start_job")
    def test_success_declares_simple_grid_output(self, mock_start_job):
        from app import handler

        mock_start_job.return_value = "simple_grids/S6/2025/S6_alt_ref_simple_grid_v1_1_20250107.nc"

        event = {"bucket": "b", "date": "2025-01-07", "source": "S6", "resolution": None}
        result = handler(event, None)

        self.assertEqual(result["stage"], "simple_grids")
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["date"], "2025-01-07")
        self.assertEqual(result["source"], "S6")
        self.assertEqual(
            result["outputs"],
            [{
                "key": "simple_grids/S6/2025/S6_alt_ref_simple_grid_v1_1_20250107.nc",
                "kind": "simple_grid",
            }],
        )

    @patch("app.start_job")
    def test_skip_returns_skipped_outcome(self, mock_start_job):
        from app import handler

        mock_start_job.return_value = None

        event = {"bucket": "b", "date": "2025-03-10", "source": "S6", "resolution": None}
        result = handler(event, None)

        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["outputs"], [])
        self.assertIn("skip_reason", result["metadata"])


class TestSimpleGridderKey(unittest.TestCase):

    def test_key_for_default_resolution(self):
        from simple_gridder.gridder import SimpleGridderJob

        job = SimpleGridderJob("2025-01-07", "b", "S6", None)
        self.assertEqual(
            job.key,
            "simple_grids/S6/2025/S6_alt_ref_simple_grid_v1_1_20250107.nc",
        )

    def test_quart_resolution_key_under_quart_deg(self):
        from simple_gridder.gridder import SimpleGridderJob

        job = SimpleGridderJob("2025-01-07", "b", "S6", "quart")
        self.assertTrue(job.key.startswith("simple_grids/quart_deg/S6/2025/"))
        # dst stays consistent with the key
        self.assertTrue(job.dst.endswith(job.key))

    def test_high_latitude_source_uses_hilat_filename(self):
        """A high_latitude source (S3B) resolves the simple_grid_high_latitude
        product; previously this raised ValueError because that product was not
        registered. The filename mirrors the along-track _alt_hilat_ convention."""
        from simple_gridder.gridder import SimpleGridderJob

        job = SimpleGridderJob("2025-01-07", "b", "S3B", None)
        self.assertEqual(
            job.key,
            "simple_grids/S3B/2025/S3B_alt_hilat_simple_grid_v1_1_20250107.nc",
        )


class TestGenerateKeys(unittest.TestCase):
    """Date windowing + P3 key generation (the 10-day [center-5, center+4] window)."""

    def _job(self, date="2025-01-07", source="S6", resolution=None):
        from simple_gridder.gridder import SimpleGridderJob

        return SimpleGridderJob(date, "b", source, resolution)

    def test_window_bounds(self):
        job = self._job()
        self.assertEqual(job.center_date, datetime(2025, 1, 7))
        # window is center-5 .. center+4 (start_date + 9 days)
        self.assertEqual(job.start_date, datetime(2025, 1, 2))
        self.assertEqual(job.end_date, datetime(2025, 1, 11))

    def test_generates_ten_daily_keys(self):
        keys = self._job().generate_keys()
        self.assertEqual(len(keys), 10)

    def test_key_endpoints_and_prefix(self):
        keys = self._job().generate_keys()
        # inclusive of both window ends; reads the P3 lifecycle version
        self.assertEqual(
            keys[0], "s3://b/daily_files/p3/S6/2025/S6_alt_ref_at_v1_1_20250102.nc"
        )
        self.assertEqual(
            keys[-1], "s3://b/daily_files/p3/S6/2025/S6_alt_ref_at_v1_1_20250111.nc"
        )

    def test_window_spans_year_and_month_boundaries(self):
        # center Jan 3 → window Dec 29 .. Jan 7, so keys cross into 2024/12
        keys = self._job(date="2025-01-03").generate_keys()
        self.assertEqual(len(keys), 10)
        self.assertIn("daily_files/p3/S6/2024/S6_alt_ref_at_v1_1_20241229.nc", keys[0])
        self.assertIn("daily_files/p3/S6/2025/S6_alt_ref_at_v1_1_20250107.nc", keys[-1])

    def test_high_latitude_source_reads_hilat_daily_files(self):
        keys = self._job(source="S3B").generate_keys()
        self.assertTrue(
            all("S3B_alt_hilat_at_v1_1_" in k for k in keys),
            "high_latitude source must read _alt_hilat_ P3 daily files",
        )


class TestDsEncoding(unittest.TestCase):
    """Per-variable NetCDF encoding: compression + dtype/_FillValue overrides."""

    def _encoding(self):
        from simple_gridder.gridder import SimpleGridderJob

        job = SimpleGridderJob("2025-01-07", "b", "S6", None)
        ds = xr.Dataset(
            {
                "ssha": (("latitude", "longitude"), np.zeros((2, 2))),
                "counts": (("latitude", "longitude"), np.zeros((2, 2))),
                "basin_flag": (("latitude", "longitude"), np.zeros((2, 2))),
            },
            coords={"latitude": [0.0, 1.0], "longitude": [0.0, 1.0]},
        )
        return job.ds_encoding(ds)

    def test_ssha_is_float64_with_max_fill(self):
        enc = self._encoding()["ssha"]
        self.assertEqual(enc["dtype"], "float64")
        self.assertEqual(enc["_FillValue"], np.finfo(np.float64).max)
        self.assertTrue(enc["zlib"])
        self.assertEqual(enc["complevel"], 5)

    def test_counts_and_basin_flag_are_int32_with_max_fill(self):
        enc = self._encoding()
        for var in ("counts", "basin_flag"):
            self.assertEqual(enc[var]["dtype"], "int32", var)
            self.assertEqual(enc[var]["_FillValue"], np.iinfo(np.int32).max, var)

    def test_time_uses_reference_epoch_units(self):
        enc = self._encoding()["time"]
        self.assertEqual(enc["units"], "seconds since 1990-01-01 00:00:00")
        self.assertEqual(enc["dtype"], "float64")
        self.assertIsNone(enc["_FillValue"])

    def test_lat_lon_are_float32_uncompressed_coords(self):
        enc = self._encoding()
        for var in ("latitude", "longitude"):
            self.assertEqual(enc[var]["dtype"], "float32", var)
            self.assertIsNone(enc[var]["_FillValue"], var)


class TestSource(unittest.TestCase):
    """Source field selection: prefers ssha_smoothed, falls back to legacy ssh_smoothed."""

    def _ds(self, **extra_vars):
        base = {
            "basin_flag": ("t", np.array([1, 2])),
            "longitude": ("t", np.array([10.0, 20.0])),
            "latitude": ("t", np.array([1.0, 2.0])),
        }
        base.update(extra_vars)
        return xr.Dataset(base)

    def test_prefers_ssha_smoothed(self):
        from simple_gridder.gridding import Source

        ds = self._ds(
            ssha_smoothed=("t", np.array([1.0, 2.0])),
            ssh_smoothed=("t", np.array([9.0, 9.0])),
        )
        np.testing.assert_array_equal(Source(ds).smssh, [1.0, 2.0])

    def test_falls_back_to_legacy_ssh_smoothed(self):
        from simple_gridder.gridding import Source

        ds = self._ds(ssh_smoothed=("t", np.array([5.0, 6.0])))
        np.testing.assert_array_equal(Source(ds).smssh, [5.0, 6.0])


class TestMergeGranulesThreshold(unittest.TestCase):
    """The coverage guard: below 150k valid points, merge_granules raises
    InsufficientData (which make_grid catches → empty grid)."""

    def _gridder(self, streamed_files):
        from simple_gridder.gridding import Gridder

        return Gridder(
            datetime(2025, 1, 7),
            datetime(2025, 1, 2),
            datetime(2025, 1, 11),
            ["f.nc"],
            streamed_files,
            None,
        )

    @patch("simple_gridder.gridding.xr.open_mfdataset")
    def test_below_threshold_raises_insufficient_data(self, mock_open):
        from simple_gridder.gridding import InsufficientData

        n = 100  # well below the 150000 threshold
        ds = xr.Dataset(
            {"ssha_smoothed": ("time", np.ones(n))},
            coords={"time": np.arange(n)},
        )
        mock_open.return_value = ds

        gridder = self._gridder(["obj"])
        with self.assertRaises(InsufficientData):
            gridder.merge_granules()

    @patch("simple_gridder.gridding.xr.open_mfdataset")
    def test_empty_window_raises_insufficient_data(self, mock_open):
        from simple_gridder.gridding import InsufficientData

        ds = xr.Dataset(
            {"ssha_smoothed": ("time", np.array([], dtype=float))},
            coords={"time": np.array([], dtype=float)},
        )
        mock_open.return_value = ds

        gridder = self._gridder([])
        with self.assertRaises(InsufficientData):
            gridder.merge_granules()


class TestParseBasinConnections(unittest.TestCase):
    """The basin adjacency parser skips the HDR-prefixed product-style header (and
    blank lines) that ships on the PO.DAAC-served copy of basin_connection_table_v2.txt."""

    def test_parses_plain_rows(self):
        from simple_gridder.gridding import parse_basin_connections

        conns = parse_basin_connections(["1:1,14,28", "2:2,23"])
        self.assertEqual([c.id for c in conns], [1, 2])
        np.testing.assert_array_equal(conns[0].valid_basins, [1, 14, 28])
        np.testing.assert_array_equal(conns[1].valid_basins, [2, 23])

    def test_skips_hdr_header_and_blank_lines(self):
        from simple_gridder.gridding import parse_basin_connections

        lines = [
            "HDR NASA-SSH Basin Connection Table",
            "HDR",
            "HDR Format: <id>:<comma-separated ids>",
            "HDR Header_End-------------------------------------",
            "",
            "1:1,14,28",
            "   ",
            "2:2,23",
        ]
        conns = parse_basin_connections(lines)
        self.assertEqual([c.id for c in conns], [1, 2])

    def test_valid_basins_are_int16(self):
        from simple_gridder.gridding import parse_basin_connections

        conns = parse_basin_connections(["1:1,14,28"])
        self.assertEqual(conns[0].valid_basins.dtype, np.int16)

    def test_real_table_parses_and_holds_invariants(self):
        """The shipped basin_connection_table_v2.txt loads through the header-tolerant
        parser and satisfies the invariants documented in its header / ADR 0007."""
        import os

        from simple_gridder.gridding import parse_basin_connections

        path = os.path.join(
            os.path.dirname(__file__),
            "..",
            "simple_gridder",
            "ref_files",
            "basin_connection_table_v2.txt",
        )
        with open(path) as f:
            conns = parse_basin_connections(f)

        self.assertGreater(len(conns), 0)
        table = {c.id: list(c.valid_basins) for c in conns}

        # every basin lists itself
        self.assertEqual([i for i, vs in table.items() if i not in vs], [])
        # no duplicates within a row
        self.assertEqual([i for i, vs in table.items() if len(vs) != len(set(vs))], [])
        # symmetric for ids < 1000
        asym = [
            (a, b)
            for a, vs in table.items()
            if a < 1000
            for b in vs
            if b < 1000 and b != a and b in table and a not in table[b]
        ]
        self.assertEqual(asym, [])


if __name__ == "__main__":
    unittest.main()
