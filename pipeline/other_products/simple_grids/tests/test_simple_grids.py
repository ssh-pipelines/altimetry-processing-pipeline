import unittest
from datetime import datetime
from unittest.mock import patch

import numpy as np
import xarray as xr


class TestSimpleGridsHandlerOutcome(unittest.TestCase):

    @patch("app.start_job")
    def test_success_declares_simple_grid_output(self, mock_start_job):
        from app import handler

        key = "simple_grids/NASA-SSH/2025/NASA-SSH_alt_ref_simple_grid_v1_1_20250107.nc"
        mock_start_job.return_value = key

        event = {
            "bucket": "b",
            "date": "2025-01-07",
            "contributors": ["NASA-SSH"],
            "publish_as": "NASA-SSH",
            "resolution": None,
        }
        result = handler(event, None)

        self.assertEqual(result["stage"], "simple_grids")
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["date"], "2025-01-07")
        self.assertEqual(result["source"], "NASA-SSH")
        self.assertEqual(result["outputs"], [{"key": key, "kind": "simple_grid"}])
        self.assertEqual(result["metadata"]["contributors"], ["NASA-SSH"])

    @patch("app.start_job")
    def test_development_grid_outcome_is_not_keyed_on_a_product(self, mock_start_job):
        from app import handler

        mock_start_job.return_value = "simple_grids/development/x/2025/x.nc"
        event = {
            "bucket": "b",
            "date": "2025-01-07",
            "contributors": ["NASA-SSH"],
            "publish_as": None,
            "resolution": None,
        }
        result = handler(event, None)
        self.assertEqual(result["source"], "development")

    @patch("app.start_job")
    def test_skip_returns_skipped_outcome(self, mock_start_job):
        from app import handler

        mock_start_job.return_value = None

        event = {
            "bucket": "b",
            "date": "2025-03-10",
            "contributors": ["NASA-SSH"],
            "publish_as": "NASA-SSH",
            "resolution": None,
        }
        result = handler(event, None)

        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["outputs"], [])
        self.assertIn("skip_reason", result["metadata"])

    def test_omitted_publish_as_raises(self):
        """Absent and null are different: a null publish_as is the deliberate
        'development output' choice, so it cannot be defaulted."""
        from app import handler

        event = {
            "bucket": "b",
            "date": "2025-01-07",
            "contributors": ["NASA-SSH"],
            "resolution": None,
        }
        with self.assertRaises(ValueError):
            handler(event, None)

    def test_no_contributors_raises(self):
        from app import handler

        event = {
            "bucket": "b",
            "date": "2025-01-07",
            "contributors": [],
            "publish_as": "NASA-SSH",
            "resolution": None,
        }
        with self.assertRaises(ValueError):
            handler(event, None)


class TestSimpleGridderKey(unittest.TestCase):
    """`publish_as` decides both the output identity and whether the grid is a
    product at all. The contributor set never does."""

    def test_production_grid_keeps_the_existing_staged_address(self):
        from simple_gridder.gridder import SimpleGridderJob

        job = SimpleGridderJob("2025-01-07", "b", ["NASA-SSH"], "NASA-SSH", None)
        self.assertEqual(
            job.key,
            "simple_grids/NASA-SSH/2025/NASA-SSH_alt_ref_simple_grid_v1_1_20250107.nc",
        )
        self.assertEqual(job.product.name, "simple_grid_reference")
        self.assertEqual(job.filename, job.key.rsplit("/", 1)[-1])

    def test_development_grid_over_the_real_reference_is_still_development(self):
        """The defect this guards: deriving the identity from the contributors
        would write the real reference DOI into a throwaway grid."""
        from simple_gridder.gridder import SimpleGridderJob

        job = SimpleGridderJob("2025-01-07", "b", ["NASA-SSH"], None, None)
        self.assertEqual(job.product.name, "simple_grid_development")
        self.assertEqual(job.product.id, "TBD")
        self.assertTrue(job.key.startswith("simple_grids/development/NASA-SSH/"))
        self.assertIn("_dev_", job.key)

    def test_development_grid_identity_names_its_contributors(self):
        from simple_gridder.gridder import SimpleGridderJob

        job = SimpleGridderJob(
            "2025-01-07", "b", ["NASA-SSH", "NASA-SSH_S3B"], None, None
        )
        self.assertTrue(
            job.key.startswith("simple_grids/development/NASA-SSH_NASA-SSH_S3B/")
        )

    def test_high_latitude_line_cannot_be_published_as_a_grid(self):
        """Each high-latitude satellite publishes along-track only; there is at
        most one high-latitude grid and it is deferred."""
        from simple_gridder.gridder import SimpleGridderJob

        with self.assertRaises(ValueError):
            SimpleGridderJob("2025-01-07", "b", ["NASA-SSH_S3B"], "NASA-SSH_S3B", None)

    def test_quart_resolution_key_under_quart_deg(self):
        from simple_gridder.gridder import SimpleGridderJob

        job = SimpleGridderJob("2025-01-07", "b", ["NASA-SSH"], "NASA-SSH", "quart")
        self.assertTrue(job.key.startswith("simple_grids/quart_deg/NASA-SSH/2025/"))
        self.assertIn("simple_grid_quart_v1_1", job.filename)
        # dst stays consistent with the key
        self.assertTrue(job.dst.endswith(job.key))

    def test_empty_contributors_raises(self):
        from simple_gridder.gridder import SimpleGridderJob

        with self.assertRaises(ValueError):
            SimpleGridderJob("2025-01-07", "b", [], "NASA-SSH", None)


class TestGenerateKeys(unittest.TestCase):
    """Date windowing + published-key generation (the 10-day [center-5, center+4]
    window). Contributors are published products, so the gridder reads `products/`
    and never internal staging."""

    def _job(self, date="2025-01-07", contributors=None, publish_as="NASA-SSH",
             resolution=None):
        from simple_gridder.gridder import SimpleGridderJob

        return SimpleGridderJob(
            date, "b", contributors or ["NASA-SSH"], publish_as, resolution
        )

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
        # inclusive of both window ends; reads the published along-track product
        self.assertEqual(
            keys[0],
            "s3://b/products/NASA-SSH/along_track/2025/"
            "NASA-SSH_alt_ref_at_v1_1_20250102.nc",
        )
        self.assertEqual(
            keys[-1],
            "s3://b/products/NASA-SSH/along_track/2025/"
            "NASA-SSH_alt_ref_at_v1_1_20250111.nc",
        )

    def test_reads_nothing_from_internal_staging(self):
        for key in self._job().generate_keys():
            with self.subTest(key=key):
                self.assertNotIn("/daily_files/", key)

    def test_window_spans_year_and_month_boundaries(self):
        # center Jan 3 → window Dec 29 .. Jan 7, so keys cross into 2024/12
        keys = self._job(date="2025-01-03").generate_keys()
        self.assertEqual(len(keys), 10)
        self.assertIn(
            "products/NASA-SSH/along_track/2024/NASA-SSH_alt_ref_at_v1_1_20241229.nc",
            keys[0],
        )
        self.assertIn(
            "products/NASA-SSH/along_track/2025/NASA-SSH_alt_ref_at_v1_1_20250107.nc",
            keys[-1],
        )

    def test_high_latitude_contributor_reads_its_hilat_product(self):
        keys = self._job(contributors=["NASA-SSH_S3B"], publish_as=None).generate_keys()
        self.assertTrue(
            all("NASA-SSH_S3B_alt_hilat_at_v1_1_" in k for k in keys),
            "a high-latitude contributor must read its _alt_hilat_ published files",
        )

    def test_every_contributor_is_read_on_every_date(self):
        keys = self._job(
            contributors=["NASA-SSH", "NASA-SSH_S3B"], publish_as=None
        ).generate_keys()
        self.assertEqual(len(keys), 20)
        self.assertEqual(len([k for k in keys if "/NASA-SSH/along_track/" in k]), 10)
        self.assertEqual(len([k for k in keys if "/NASA-SSH_S3B/along_track/" in k]), 10)


class TestDsEncoding(unittest.TestCase):
    """Per-variable NetCDF encoding: compression + dtype/_FillValue overrides."""

    def _encoding(self):
        from simple_gridder.gridder import SimpleGridderJob

        job = SimpleGridderJob("2025-01-07", "b", ["NASA-SSH"], "NASA-SSH", None)
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

        from utilities.source_profile import get_product

        return Gridder(
            datetime(2025, 1, 7),
            datetime(2025, 1, 2),
            datetime(2025, 1, 11),
            ["f.nc"],
            streamed_files,
            None,
            get_product("simple_grid_reference"),
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


class TestReferenceGridIdentity(unittest.TestCase):
    """In-file identity of the reference simple-grid product.

    Built through the empty-grid path (no streamed files → InsufficientData →
    all-NaN grid), which still writes the full global-attr set offline. Pinned
    because these are the values PO.DAAC ingested: moving identity out of
    `gridding.py` literals and into `utilities/products.yaml` must not change them.
    """

    @classmethod
    def setUpClass(cls):
        from simple_gridder.gridding import Gridder

        from utilities.source_profile import get_product

        gridder = Gridder(
            datetime(2025, 1, 7),
            datetime(2025, 1, 2),
            datetime(2025, 1, 11),
            ["f.nc"],
            [],
            None,
            get_product("simple_grid_reference"),
        )
        ds = gridder.make_grid("out.nc")
        cls.attrs = dict(ds.attrs)
        ds.close()

    def test_title(self):
        self.assertEqual(
            self.attrs["title"],
            "NASA-SSH Simple Gridded Sea Surface Height from Standardized Reference Missions Only Version 1.1",
        )

    def test_doi(self):
        self.assertEqual(self.attrs["id"], "10.5067/NSREF-SG0V11")

    def test_product_short_name(self):
        self.assertEqual(self.attrs["product_short_name"], "NASA_SSH_REF_SIMPLE_GRID_V11")

    def test_processing_level_is_gridded(self):
        """Level 3, not the along-track product's Level 2."""
        self.assertEqual(self.attrs["processing_level"], "Level 3")

    def test_product_version(self):
        self.assertEqual(self.attrs["product_version"], "V1.1")

    def test_references_and_source_url_point_at_the_along_track_product(self):
        self.assertEqual(self.attrs["references"], "https://doi.org/10.5067/NSREF-AT0V1")
        self.assertEqual(
            self.attrs["source_url"],
            "https://podaac.jpl.nasa.gov/dataset/nasa_ssh_ref_alongtrack_v11",
        )

    def test_shared_attrs_survive(self):
        self.assertEqual(self.attrs["project"], "NASA-SSH")
        self.assertEqual(self.attrs["institution"], "NASA/Jet Propulsion Laboratory")
        self.assertEqual(self.attrs["mean_sea_surface"], "DTU21")


class TestHighLatitudeGridIdentity(unittest.TestCase):
    """A high-latitude grid must not inherit the reference grid's identity. The DOI
    is the placeholder `TBD` until the science side settles this product."""

    @classmethod
    def setUpClass(cls):
        from simple_gridder.gridding import Gridder

        from utilities.source_profile import get_product

        gridder = Gridder(
            datetime(2025, 1, 7),
            datetime(2025, 1, 2),
            datetime(2025, 1, 11),
            ["f.nc"],
            [],
            None,
            get_product("simple_grid_high_latitude"),
        )
        ds = gridder.make_grid("out.nc")
        cls.attrs = dict(ds.attrs)
        ds.close()

    def test_does_not_claim_the_reference_doi(self):
        self.assertNotEqual(self.attrs["id"], "10.5067/NSREF-SG0V11")
        self.assertEqual(self.attrs["id"], "TBD")

    def test_product_short_name(self):
        self.assertEqual(self.attrs["product_short_name"], "NASA_SSH_HILAT_SIMPLE_GRID_V11")

    def test_title_does_not_claim_reference_missions(self):
        self.assertNotIn("Reference Missions", self.attrs["title"])

    def test_processing_level_still_gridded(self):
        self.assertEqual(self.attrs["processing_level"], "Level 3")

    def test_does_not_point_at_the_reference_along_track_product(self):
        self.assertEqual(self.attrs["references"], "")
        self.assertEqual(self.attrs["source_url"], "")


class TestDevelopmentGridIdentity(unittest.TestCase):
    """A development grid must declare in-file that it is not a product. The
    title says so and the DOI is the `TBD` placeholder."""

    @classmethod
    def setUpClass(cls):
        from simple_gridder.gridding import Gridder

        from utilities.source_profile import get_product

        gridder = Gridder(
            datetime(2025, 1, 7),
            datetime(2025, 1, 2),
            datetime(2025, 1, 11),
            ["f.nc"],
            [],
            None,
            get_product("simple_grid_development"),
        )
        ds = gridder.make_grid("out.nc")
        cls.attrs = dict(ds.attrs)
        ds.close()

    def test_does_not_claim_the_reference_identity(self):
        self.assertNotEqual(self.attrs["id"], "10.5067/NSREF-SG0V11")
        self.assertEqual(self.attrs["id"], "TBD")
        self.assertNotEqual(
            self.attrs["product_short_name"], "NASA_SSH_REF_SIMPLE_GRID_V11"
        )

    def test_title_says_it_is_not_a_product(self):
        self.assertIn("not a product", self.attrs["title"])


if __name__ == "__main__":
    unittest.main()
