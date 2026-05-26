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
        self.assertIn("S6B", sources)

    def test_invalid_source_raises(self):
        with self.assertRaises(ValueError):
            get_source_config("NONEXISTENT")

    def test_gsfc_config_fields(self):
        cfg = get_source_config("GSFC")
        self.assertIsInstance(cfg, SourceConfig)
        self.assertEqual(cfg.source, "GSFC")
        self.assertIsInstance(cfg.smoothing, SmoothingConfig)
        self.assertGreater(len(cfg.collections), 0)
        self.assertIsInstance(cfg.collections[0], CollectionConfig)
        self.assertEqual(cfg.source_mss, "DTU15")

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

    def test_s6b_config_fields(self):
        cfg = get_source_config("S6B")
        self.assertIsInstance(cfg, SourceConfig)
        self.assertEqual(cfg.source, "S6B")
        self.assertEqual(cfg.source_mss, "DTU18")
        self.assertIsInstance(cfg.smoothing, SmoothingConfig)

    def test_cmr_sources_default_discovery_type(self):
        for source in ["GSFC", "S6", "S6B"]:
            cfg = get_source_config(source)
            self.assertEqual(cfg.discovery_type, "cmr", f"{source} should have discovery_type 'cmr'")

    def test_example_s3_config(self):
        cfg = get_source_config("EXAMPLE_S3")
        self.assertIsInstance(cfg, SourceConfig)
        self.assertEqual(cfg.source, "EXAMPLE_S3")
        self.assertEqual(cfg.discovery_type, "s3_bucket")
        self.assertEqual(cfg.source_bucket, "example-source-bucket")
        self.assertEqual(cfg.source_prefix_pattern, "data/{source}/{year}")
        self.assertEqual(cfg.source_filename_pattern, "{source}_{date}.nc")
        # Source-level metadata now lives in collections[0]
        self.assertEqual(len(cfg.collections), 1)
        self.assertEqual(cfg.collections[0].source_label, "Example S3-hosted dataset")
        self.assertEqual(cfg.collections[0].source_url, "https://example.com/dataset")
        self.assertEqual(cfg.collections[0].reference, "https://doi.org/10.0000/example")

    def test_example_s3_cycle_index_key(self):
        cfg = get_source_config("EXAMPLE_S3")
        self.assertEqual(cfg.cycle_index_key, "data/EXAMPLE_S3/cycle_index.json")

    def test_cmr_sources_have_no_cycle_index_key(self):
        for source in ["GSFC", "S6", "S6B"]:
            cfg = get_source_config(source)
            self.assertIsNone(cfg.cycle_index_key, f"{source} should not have cycle_index_key")

    def test_example_s3_in_available_sources(self):
        sources = get_available_sources()
        self.assertIn("EXAMPLE_S3", sources)

    def test_sources_without_bad_points_are_none(self):
        for source in ["S6", "S6B"]:
            cfg = get_source_config(source)
            self.assertIsNone(cfg.bad_points, f"{source} should have bad_points=None")

    def test_s3b_high_latitude_has_no_mss_fields(self):
        """High-latitude sources interpolate DTU21 directly (see ADR 0002),
        so the config must not carry source_mss / target_mss / mss_diff_file."""
        cfg = get_source_config("S3B")
        self.assertEqual(cfg.product_type, "high_latitude")
        self.assertIsNone(cfg.source_mss)
        self.assertIsNone(cfg.target_mss)
        self.assertIsNone(cfg.mss_diff_file)

    def test_high_latitude_with_mss_field_raises(self):
        """Adding any MSS field to a high_latitude source should fail validation."""
        for offending in ("source_mss", "target_mss", "mss_diff_file"):
            with self.subTest(field=offending):
                with self.assertRaises(ValueError) as ctx:
                    SourceConfig(
                        source="TEST",
                        product_type="high_latitude",
                        start_date=__import__("datetime").date(2020, 1, 1),
                        smoothing=SmoothingConfig(speed=5.0, sigma=10.0),
                        **{offending: "DTU21"},
                    )
                self.assertIn(offending, str(ctx.exception))

    def test_reference_without_mss_field_raises(self):
        """Reference sources require all three MSS fields."""
        import datetime as _dt
        for missing in ("source_mss", "target_mss", "mss_diff_file"):
            with self.subTest(missing=missing):
                kwargs = {
                    "source_mss": "DTU18",
                    "target_mss": "DTU21",
                    "mss_diff_file": "DTU18_minus_DTU21.nc",
                }
                kwargs.pop(missing)
                with self.assertRaises(ValueError) as ctx:
                    SourceConfig(
                        source="TEST",
                        product_type="reference",
                        start_date=_dt.date(2020, 1, 1),
                        smoothing=SmoothingConfig(speed=5.0, sigma=10.0),
                        **kwargs,
                    )
                self.assertIn(missing, str(ctx.exception))
