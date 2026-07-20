import unittest

from crossover.config.source_config import (
    SourceConfig,
    get_available_sources,
    get_source_config,
)


class TestSourceConfig(unittest.TestCase):
    def test_available_sources(self):
        sources = get_available_sources()
        self.assertIn("GSFC", sources)
        self.assertIn("S6", sources)
        self.assertIn("S6B", sources)
        self.assertIn("S3B", sources)

    def test_invalid_source_raises(self):
        with self.assertRaises(ValueError):
            get_source_config("NONEXISTENT")

    def test_gsfc_config_fields(self):
        cfg = get_source_config("GSFC")
        self.assertIsInstance(cfg, SourceConfig)
        self.assertEqual(cfg.source, "GSFC")
        self.assertEqual(cfg.crossover_type, "self")
        self.assertEqual(cfg.cycle_length, 9.9156)
        self.assertEqual(cfg.window_size, 10)
        self.assertEqual(cfg.window_padding, 2)
        self.assertEqual(cfg.max_pass_number, 9999)

    def test_s6_config_fields(self):
        cfg = get_source_config("S6")
        self.assertIsInstance(cfg, SourceConfig)
        self.assertEqual(cfg.source, "S6")
        self.assertEqual(cfg.crossover_type, "self")
        self.assertEqual(cfg.cycle_length, 9.9156)
        self.assertEqual(cfg.window_size, 10)
        self.assertEqual(cfg.window_padding, 2)
        self.assertEqual(cfg.max_pass_number, 9999)

    def test_s6b_config_fields(self):
        cfg = get_source_config("S6B")
        self.assertIsInstance(cfg, SourceConfig)
        self.assertEqual(cfg.source, "S6B")
        self.assertEqual(cfg.crossover_type, "self")
        self.assertEqual(cfg.cycle_length, 9.9156)

    def test_self_source_has_no_reference_fields(self):
        cfg = get_source_config("S6")
        self.assertIsNone(cfg.reference_source)
        self.assertIsNone(cfg.reference_version)

    def test_s3b_reference_config_fields(self):
        cfg = get_source_config("S3B")
        self.assertIsInstance(cfg, SourceConfig)
        self.assertEqual(cfg.source, "S3B")
        self.assertEqual(cfg.crossover_type, "reference")
        self.assertEqual(cfg.reference_source, "NASA-SSH")
        self.assertEqual(cfg.reference_version, "p3")
        self.assertEqual(cfg.window_size, 12)
        self.assertEqual(cfg.window_padding, 2)

    def test_reference_type_requires_reference_fields(self):
        # Constructing a reference config without a reference_source/version is
        # rejected by __post_init__.
        with self.assertRaises(ValueError):
            SourceConfig(
                source="X",
                product_type="high_latitude",
                start_date=None,
                crossover_type="reference",
                cycle_length=9.9156,
                window_size=12,
                window_padding=2,
                max_pass_number=9999,
            )
