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
    enso_filename,
    enso_grid_key,
    enso_map_key,
    indicators_key,
    indicators_prefix,
    jobs_manifest_key,
    oer_correction_key,
    oer_polygon_key,
    s3_uri,
    simple_grid_filename,
    simple_grid_key,
    simple_grid_prefix,
    stage_results_prefix,
)
from utilities.source_profile import (
    Product,
    SourceCommon,
    clear_caches,
    get_source_profile,
)


def _profile(source: str, product_type: str = "reference") -> SourceCommon:
    return SourceCommon(
        source=source,
        product_type=product_type,
        discovery_type="cmr",
        unify=False,
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
            unify=False,
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
        """Post-unifier sg_jobs.json: $p[1] is the original source, $p[2] is run_id."""
        self.assertEqual(
            stage_results_prefix(
                "pipeline_runs/S6/20250528T120000/NASA-SSH/sg_jobs.json", "enso"
            ),
            "pipeline_runs/S6/20250528T120000/results/enso/",
        )

    def test_stage_results_prefix_rejects_short_jobs_key(self):
        with self.assertRaises(ValueError):
            stage_results_prefix("a/b", "enso")


class TestSimpleGrid(unittest.TestCase):
    def test_simple_grid_key(self):
        key = simple_grid_key(_profile("S6"), date(2025, 1, 7))
        self.assertEqual(
            key,
            "simple_grids/S6/2025/S6_alt_ref_simple_grid_v1_1_20250107.nc",
        )

    def test_simple_grid_filename(self):
        fname = simple_grid_filename(_profile("GSFC"), date(2025, 1, 7))
        self.assertEqual(fname, "GSFC_alt_ref_simple_grid_v1_1_20250107.nc")

    def test_simple_grid_prefix(self):
        self.assertEqual(simple_grid_prefix("S6", 2025), "simple_grids/S6/2025/")


class TestEnso(unittest.TestCase):
    def test_enso_filename(self):
        self.assertEqual(enso_filename(date(2025, 1, 7)), "ENSO_20250107.nc")

    def test_enso_grid_key(self):
        self.assertEqual(enso_grid_key("S6", date(2025, 1, 7)), "enso_grids/S6/ENSO_20250107.nc")

    def test_enso_map_key_ortho(self):
        self.assertEqual(
            enso_map_key("S6", date(2025, 1, 7), "ortho"),
            "maps/enso_maps/S6/ortho/ENSO_ortho_20250107.png",
        )

    def test_enso_map_key_plate(self):
        self.assertEqual(
            enso_map_key("S6", date(2025, 1, 7), "plate"),
            "maps/enso_maps/S6/plate/ENSO_plate_20250107.png",
        )


class TestIndicators(unittest.TestCase):
    def test_indicators_key(self):
        self.assertEqual(indicators_key("S6"), "indicators/S6/indicators.nc")

    def test_indicators_prefix(self):
        self.assertEqual(indicators_prefix("S6"), "indicators/S6/")


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

        original = layout.get_product("along_track_reference")
        bumped = Product(
            name=original.name,
            version="v9_9",
            filename_template=original.filename_template,
        )
        with patch.object(layout, "get_product", return_value=bumped):
            key = daily_file_key(_profile("S6"), date(2025, 1, 7), "p2")
        self.assertIn("v9_9", key)


if __name__ == "__main__":
    unittest.main()
