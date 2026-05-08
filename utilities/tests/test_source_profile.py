import unittest
from dataclasses import dataclass
from datetime import date

from utilities.source_profile import (
    CollectionConfig,
    Product,
    SourceCommon,
    clear_caches,
    daily_filename_prefix,
    get_product,
    get_registered_sources,
    get_source_profile,
    list_sources_for_stage,
    load_source_config,
)


@dataclass(kw_only=True, frozen=True)
class _StageA(SourceCommon):
    s3_prefix: str
    filename_template: str


class TestLoadSourceCommon(unittest.TestCase):
    def test_loads_gsfc_common(self):
        profile = get_source_profile("GSFC")
        self.assertIsInstance(profile, SourceCommon)
        self.assertEqual(profile.source, "GSFC")
        self.assertEqual(profile.product_type, "reference")
        self.assertEqual(profile.discovery_type, "cmr")
        self.assertTrue(profile.unify)
        self.assertEqual(profile.start_date, date(1992, 10, 25))
        self.assertEqual(profile.end_date, date(2025, 12, 31))

    def test_collections_parsed_into_dataclasses(self):
        profile = get_source_profile("S6")
        self.assertGreater(len(profile.collections), 0)
        for c in profile.collections:
            self.assertIsInstance(c, CollectionConfig)
            self.assertTrue(c.concept_id)
            self.assertTrue(c.shortname)
        self.assertEqual([c.priority for c in profile.collections], [1, 2, 3])

    def test_s3_bucket_fields_only_for_s3_sources(self):
        gsfc = get_source_profile("GSFC")
        self.assertIsNone(gsfc.source_bucket)
        self.assertIsNone(gsfc.source_filename_pattern)
        ex = get_source_profile("EXAMPLE_S3")
        self.assertEqual(ex.source_bucket, "example-source-bucket")
        self.assertEqual(ex.source_filename_pattern, "{source}_{date}.nc")


class TestLoadWithStage(unittest.TestCase):
    def test_merges_common_and_stage_section(self):
        cfg = load_source_config(_StageA, "pipeline_init", "GSFC")
        self.assertEqual(cfg.source, "GSFC")
        self.assertEqual(cfg.product_type, "reference")  # from common
        self.assertEqual(cfg.s3_prefix, "daily_files/p3/GSFC")  # from stage
        self.assertTrue(cfg.filename_template.startswith("GSFC_alt_ref_at_"))


class TestErrors(unittest.TestCase):
    def test_missing_yaml_raises(self):
        with self.assertRaises(ValueError) as ctx:
            get_source_profile("DOES_NOT_EXIST")
        self.assertIn("not configured", str(ctx.exception))

    def test_missing_stage_required_field_raises_typeerror(self):
        # NASA-SSH has no `pipeline_init:` section, so s3_prefix is missing
        with self.assertRaises(TypeError):
            load_source_config(_StageA, "pipeline_init", "NASA-SSH")

    def test_unknown_field_raises_typeerror(self):
        # Construct a dataclass that doesn't accept all common fields
        @dataclass(frozen=True)
        class _NarrowDC:
            source: str

        with self.assertRaises(TypeError):
            load_source_config(_NarrowDC, None, "GSFC")


class TestProducts(unittest.TestCase):
    def test_get_along_track_reference(self):
        p = get_product("along_track_reference")
        self.assertIsInstance(p, Product)
        self.assertEqual(p.version, "v1_1")
        self.assertIn("{source}", p.filename_template)
        self.assertIn("{YYYYMMDD}", p.filename_template)

    def test_unknown_product_raises(self):
        with self.assertRaises(ValueError):
            get_product("does-not-exist")


class TestDailyFilenamePrefix(unittest.TestCase):
    def test_reference_sources(self):
        self.assertEqual(daily_filename_prefix("GSFC"), "GSFC_alt_ref_at_v1_1")
        self.assertEqual(daily_filename_prefix("S6"), "S6_alt_ref_at_v1_1")

    def test_high_latitude_sources(self):
        self.assertEqual(daily_filename_prefix("S3B"), "S3B_alt_hilat_at_v1_1")


class TestSourceCommonHelpers(unittest.TestCase):
    def test_daily_filename_method(self):
        profile = get_source_profile("S6")
        fname = profile.daily_filename(date(2025, 3, 15))
        self.assertEqual(fname, "S6_alt_ref_at_v1_1_20250315.nc")


class TestListSourcesForStage(unittest.TestCase):
    def test_pipeline_init_excludes_nasa_ssh(self):
        sources = list_sources_for_stage("pipeline_init")
        self.assertIn("GSFC", sources)
        self.assertIn("S6", sources)
        # NASA-SSH has no pipeline_init: section
        self.assertNotIn("NASA-SSH", sources)

    def test_unifier_excludes_s6b(self):
        sources = list_sources_for_stage("unifier")
        self.assertIn("GSFC", sources)
        self.assertIn("S6", sources)
        self.assertNotIn("S6B", sources)

    def test_none_returns_all_sources(self):
        sources = list_sources_for_stage(None)
        # All known sources should appear
        for expected in ["GSFC", "S6", "S6B", "S3B", "EXAMPLE_S3", "NASA-SSH"]:
            self.assertIn(expected, sources)

    def test_get_registered_sources(self):
        self.assertEqual(get_registered_sources(), list_sources_for_stage(None))


class TestCaching(unittest.TestCase):
    def test_clear_caches_does_not_raise(self):
        get_source_profile("GSFC")  # warm cache
        clear_caches()
        # Should still work after cache clear
        profile = get_source_profile("GSFC")
        self.assertEqual(profile.source, "GSFC")


if __name__ == "__main__":
    unittest.main()
