import unittest
from daily_files.config.source_config import (
    get_source_config,
    get_available_sources,
    SourceConfig,
    CollectionConfig,
    SmoothingConfig,
)


class TestSourceConfig(unittest.TestCase):
    def test_available_sources(self):
        sources = get_available_sources()
        self.assertIn("GSFC", sources)
        self.assertIn("S6", sources)

    def test_invalid_source_raises(self):
        with self.assertRaises(ValueError):
            get_source_config("NONEXISTENT")

    def test_satellite_field(self):
        self.assertEqual(get_source_config("GSFC").satellite, "GSFC")
        self.assertEqual(get_source_config("S6").satellite, "S6")

    def test_gsfc_config_fields(self):
        cfg = get_source_config("GSFC")
        self.assertIsInstance(cfg, SourceConfig)
        self.assertEqual(cfg.source, "GSFC")
        self.assertIsInstance(cfg.smoothing, SmoothingConfig)
        self.assertGreater(len(cfg.collections), 0)
        self.assertIsInstance(cfg.collections[0], CollectionConfig)
        self.assertTrue(cfg.filename_template)
        self.assertTrue(cfg.s3_prefix)

    def test_s6_config_has_multiple_collections(self):
        cfg = get_source_config("S6")
        self.assertGreater(len(cfg.collections), 1)
        priorities = [c.priority for c in cfg.collections]
        self.assertEqual(priorities, sorted(priorities))

    def test_collection_metadata_fields(self):
        cfg = get_source_config("S6")
        for col in cfg.collections:
            self.assertTrue(col.concept_id)
            self.assertTrue(col.shortname)
            self.assertTrue(col.source_label)
            self.assertTrue(col.source_url)
            self.assertTrue(col.reference)
