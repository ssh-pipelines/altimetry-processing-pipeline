import unittest
from datetime import date, datetime

from utilities import pipeline_layout as layout
from utilities.pipeline_layout import (
    bad_pass_key,
    crossover_filename,
    crossover_key,
    crossover_prefix,
    daily_file_filename,
    daily_file_key,
    daily_file_prefix,
    development_simple_grid_key,
    enso_filename,
    enso_grid_key,
    enso_map_key,
    jobs_manifest_key,
    oer_correction_key,
    oer_polygon_key,
    published_along_track_filename,
    published_along_track_key,
    published_indicators_key,
    published_indicators_prefix,
    published_simple_grid_key,
    s3_uri,
    simple_grid_key,
    simple_grid_prefix,
    stage_results_prefix,
)
from utilities.source_profile import (
    Product,
    SourceCommon,
    clear_caches,
)


def _profile(source: str, product_type: str = "reference") -> SourceCommon:
    return SourceCommon(
        source=source,
        product_type=product_type,
        discovery_type="cmr",
        start_date=date(2020, 1, 1),
    )


class TestDailyFile(unittest.TestCase):
    def test_daily_file_key_reference(self):
        key = daily_file_key(_profile("S6"), date(2025, 1, 7), "p2")
        self.assertEqual(key, "daily_files/p2/S6/2025/S6_alt_ref_at_v1_1_20250107.nc")

    def test_daily_file_key_high_latitude(self):
        key = daily_file_key(_profile("S3B", "high_latitude"), date(2025, 1, 7), "p1")
        self.assertEqual(key, "daily_files/p1/S3B/2025/S3B_alt_hilat_at_v1_1_20250107.nc")

    def test_daily_file_key_accepts_datetime(self):
        key = daily_file_key(_profile("GSFC"), datetime(2025, 1, 7, 12, 0), "p3")
        self.assertEqual(key, "daily_files/p3/GSFC/2025/GSFC_alt_ref_at_v1_1_20250107.nc")

    def test_daily_file_filename_just_basename(self):
        fname = daily_file_filename(_profile("S6"), date(2025, 1, 7))
        self.assertEqual(fname, "S6_alt_ref_at_v1_1_20250107.nc")
        self.assertNotIn("/", fname)

    def test_daily_file_prefix(self):
        self.assertEqual(daily_file_prefix("S6", 2025, "p3"), "daily_files/p3/S6/2025/")

    def test_unknown_product_type_raises(self):
        bad = SourceCommon(
            source="X",
            product_type="bogus",
            discovery_type="cmr",
            start_date=date(2020, 1, 1),
        )
        with self.assertRaises(ValueError):
            daily_file_key(bad, date(2025, 1, 7), "p1")


class TestCrossover(unittest.TestCase):
    def test_crossover_filename_iso_date(self):
        self.assertEqual(crossover_filename("S6", date(2025, 1, 7)), "xovers_S6-2025-01-07.nc")

    def test_crossover_key_iso_date(self):
        key = crossover_key("S6", date(2025, 1, 7), "p2")
        self.assertEqual(key, "crossovers/p2/S6/2025/xovers_S6-2025-01-07.nc")

    def test_crossover_prefix(self):
        self.assertEqual(crossover_prefix("S6", 2025, "p1"), "crossovers/p1/S6/2025/")


class TestOER(unittest.TestCase):
    def test_oer_polygon_key(self):
        key = oer_polygon_key("S6", date(2025, 1, 7))
        self.assertEqual(key, "oer/S6/2025/oerpoly_S6_2025-01-07.nc")

    def test_oer_correction_key(self):
        key = oer_correction_key("S6", date(2025, 1, 7))
        self.assertEqual(key, "oer/S6/2025/oer_correction_S6_2025-01-07.nc")


class TestBadPass(unittest.TestCase):
    def test_bad_pass_key_iso_date(self):
        key = bad_pass_key("S6", date(2025, 1, 7))
        self.assertEqual(key, "bad_passes/S6/2025-01-07.json")


class TestPipelineRuns(unittest.TestCase):
    def test_jobs_manifest_key(self):
        key = jobs_manifest_key("S6", "20250219T120000")
        self.assertEqual(key, "pipeline_runs/S6/20250219T120000/jobs.json")

    def test_stage_results_prefix_at_side(self):
        self.assertEqual(
            stage_results_prefix(
                "pipeline_runs/S6/20250528T120000/jobs.json", "daily_file"
            ),
            "pipeline_runs/S6/20250528T120000/results/daily_file/",
        )

    def test_stage_results_prefix_sg_side_uses_original_source(self):
        """Gridded-tier sg_jobs.json: $p[1] is the original source, $p[2] is run_id."""
        self.assertEqual(
            stage_results_prefix(
                "pipeline_runs/S6/20250528T120000/NASA-SSH/sg_jobs.json", "enso"
            ),
            "pipeline_runs/S6/20250528T120000/results/enso/",
        )

    def test_stage_results_prefix_rejects_short_jobs_key(self):
        with self.assertRaises(ValueError):
            stage_results_prefix("a/b", "enso")

    def test_jobs_key_identity(self):
        self.assertEqual(
            layout.jobs_key_identity("pipeline_runs/S6/20250528T120000/jobs.json"),
            ("S6", "20250528T120000"),
        )

    def test_jobs_key_identity_gridded_tier_uses_original_source(self):
        self.assertEqual(
            layout.jobs_key_identity(
                "pipeline_runs/S6/20250528T120000/NASA-SSH/jobs.json"
            ),
            ("S6", "20250528T120000"),
        )

    def test_jobs_key_identity_rejects_short(self):
        with self.assertRaises(ValueError):
            layout.jobs_key_identity("a/b")

    def test_sg_jobs_key(self):
        self.assertEqual(
            layout.sg_jobs_key("pipeline_runs/S6/20250528T120000/NASA-SSH/jobs.json"),
            "pipeline_runs/S6/20250528T120000/NASA-SSH/sg_jobs.json",
        )

    def test_run_summary_key(self):
        self.assertEqual(
            layout.run_summary_key("S6", "20250528T120000"),
            "pipeline_runs/S6/20250528T120000/summary.json",
        )


class TestSimpleGrid(unittest.TestCase):
    """Staged grids are keyed on the grid's own output identity, never on a Source."""

    def test_simple_grid_key_for_published_destined_grid(self):
        key = simple_grid_key("NASA-SSH", date(2025, 1, 7))
        self.assertEqual(
            key,
            "simple_grids/NASA-SSH/2025/NASA-SSH_alt_ref_simple_grid_v1_1_20250107.nc",
        )

    def test_unpublishable_identity_raises(self):
        """A published line with no declared simple_grid has no staged grid key —
        a single high-latitude satellite publishes along-track only."""
        with self.assertRaises(ValueError):
            simple_grid_key("NASA-SSH_S3B", date(2025, 1, 7))

    def test_development_key_segments_and_filename_family(self):
        """A development grid must be unmistakable in both the path and the file:
        its own `development/` segment and its own `_dev_` filename family."""
        key = development_simple_grid_key("NASA-SSH_S3B", date(2025, 1, 7))
        self.assertEqual(
            key,
            "simple_grids/development/NASA-SSH_S3B/2025/"
            "NASA-SSH_S3B_dev_simple_grid_v1_1_20250107.nc",
        )
        self.assertNotIn("_alt_ref_", key)
        self.assertNotIn("_alt_hilat_", key)

    def test_development_key_for_the_reference_contributors(self):
        """Gridding the real reference product for development still writes a
        development file — the identity does not follow the contributors."""
        key = development_simple_grid_key("NASA-SSH", date(2025, 1, 7))
        self.assertTrue(key.startswith("simple_grids/development/NASA-SSH/"))
        self.assertIn("NASA-SSH_dev_simple_grid_v1_1_20250107.nc", key)

    def test_simple_grid_prefix(self):
        self.assertEqual(
            simple_grid_prefix("NASA-SSH", 2025), "simple_grids/NASA-SSH/2025/"
        )


class TestEnso(unittest.TestCase):
    def test_enso_filename(self):
        self.assertEqual(enso_filename(date(2025, 1, 7)), "ENSO_20250107.nc")

    def test_enso_grid_key(self):
        self.assertEqual(
            enso_grid_key("NASA-SSH", date(2025, 1, 7)),
            "enso_grids/NASA-SSH/ENSO_20250107.nc",
        )

    def test_enso_map_key_ortho(self):
        self.assertEqual(
            enso_map_key("NASA-SSH", date(2025, 1, 7), "ortho"),
            "maps/enso_maps/NASA-SSH/ortho/ENSO_ortho_20250107.png",
        )

    def test_enso_map_key_plate(self):
        self.assertEqual(
            enso_map_key("NASA-SSH", date(2025, 1, 7), "plate"),
            "maps/enso_maps/NASA-SSH/plate/ENSO_plate_20250107.png",
        )


class TestPublishedProducts(unittest.TestCase):
    """Everything under `products/` comes from this one family of functions."""

    def test_published_along_track_key_reference(self):
        self.assertEqual(
            published_along_track_key("NASA-SSH", date(2025, 1, 7)),
            "products/NASA-SSH/along_track/2025/NASA-SSH_alt_ref_at_v1_1_20250107.nc",
        )

    def test_published_along_track_key_high_latitude(self):
        """The identity string is both the directory and the filename prefix."""
        self.assertEqual(
            published_along_track_key("NASA-SSH_S3B", date(2025, 1, 7)),
            "products/NASA-SSH_S3B/along_track/2025/"
            "NASA-SSH_S3B_alt_hilat_at_v1_1_20250107.nc",
        )

    def test_published_filename_matches_the_internal_pseudo_source_name(self):
        """Seeding `products/` is a prefix copy with no renaming: the published
        reference filename is byte-identical to what the pre-change internal
        NASA-SSH P3 was called."""
        self.assertEqual(
            published_along_track_filename("NASA-SSH", date(2025, 1, 7)),
            "NASA-SSH_alt_ref_at_v1_1_20250107.nc",
        )

    def test_published_along_track_key_accepts_datetime(self):
        self.assertEqual(
            published_along_track_key("NASA-SSH", datetime(2025, 1, 7, 12, 0)),
            "products/NASA-SSH/along_track/2025/NASA-SSH_alt_ref_at_v1_1_20250107.nc",
        )

    def test_unknown_identity_raises(self):
        with self.assertRaises(ValueError):
            published_along_track_key("NASA-SSH_NOPE", date(2025, 1, 7))

    def test_published_simple_grid_key(self):
        self.assertEqual(
            published_simple_grid_key("NASA-SSH", date(2025, 1, 7)),
            "products/NASA-SSH/simple_grid/2025/"
            "NASA-SSH_alt_ref_simple_grid_v1_1_20250107.nc",
        )

    def test_high_latitude_publishes_no_grid(self):
        """Each high-latitude satellite publishes its own along-track product, but
        there is at most one high-latitude *grid*, and it is deferred."""
        with self.assertRaises(ValueError):
            published_simple_grid_key("NASA-SSH_S3B", date(2025, 1, 7))

    def test_published_indicators_key_and_prefix(self):
        self.assertEqual(
            published_indicators_key("NASA-SSH"),
            "products/NASA-SSH/indicators/indicators.nc",
        )
        self.assertEqual(
            published_indicators_prefix("NASA-SSH"), "products/NASA-SSH/indicators/"
        )

    def test_every_published_key_lives_under_one_prefix(self):
        """A later move to a separate products bucket should be this one module
        plus a config value, which only holds while every key shares the prefix."""
        keys = [
            published_along_track_key("NASA-SSH", date(2025, 1, 7)),
            published_along_track_key("NASA-SSH_S3B", date(2025, 1, 7)),
            published_simple_grid_key("NASA-SSH", date(2025, 1, 7)),
            published_indicators_key("NASA-SSH"),
        ]
        for key in keys:
            with self.subTest(key=key):
                self.assertTrue(key.startswith("products/"))

    def test_staged_keys_stay_out_of_the_published_prefix(self):
        staged = [
            daily_file_key(_profile("S6"), date(2025, 1, 7), "p3"),
            simple_grid_key("NASA-SSH", date(2025, 1, 7)),
            development_simple_grid_key("NASA-SSH", date(2025, 1, 7)),
            enso_grid_key("NASA-SSH", date(2025, 1, 7)),
        ]
        for key in staged:
            with self.subTest(key=key):
                self.assertFalse(key.startswith("products/"))


class TestS3Uri(unittest.TestCase):
    def test_basic_concatenation(self):
        self.assertEqual(s3_uri("my-bucket", "foo/bar.nc"), "s3://my-bucket/foo/bar.nc")


class TestVersionBump(unittest.TestCase):
    """Drift detector: changing the product version in products.yaml should
    propagate to filenames produced by the layout module."""

    def setUp(self):
        clear_caches()

    def tearDown(self):
        clear_caches()

    def test_filename_picks_up_modified_version(self):
        from unittest.mock import patch

        from utilities import products

        original = layout.get_product("along_track_reference")
        bumped = Product(
            name=original.name,
            version="v9_9",
            filename_template=original.filename_template,
            published_filename_template=original.published_filename_template,
        )
        with patch.object(products, "get_product", return_value=bumped):
            key = daily_file_key(_profile("S6"), date(2025, 1, 7), "p2")
            published = published_along_track_key("NASA-SSH", date(2025, 1, 7))
        self.assertIn("v9_9", key)
        self.assertIn("v9_9", published)


if __name__ == "__main__":
    unittest.main()
