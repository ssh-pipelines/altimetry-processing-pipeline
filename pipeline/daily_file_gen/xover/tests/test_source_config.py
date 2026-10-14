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

    def test_s3b_reference_config_fields(self):
        cfg = get_source_config("S3B")
        self.assertIsInstance(cfg, SourceConfig)
        self.assertEqual(cfg.source, "S3B")
        self.assertEqual(cfg.crossover_type, "reference")
        self.assertEqual(cfg.reference_product, "NASA-SSH")
        self.assertEqual(cfg.window_size, 12)
        self.assertEqual(cfg.window_padding, 2)

    def test_reference_product_defaults_without_config(self):
        """A high-lat source needs no reference wiring: there is one published
        along-track line, so the default is the whole configuration."""
        cfg = SourceConfig(
            source="X",
            product_type="high_latitude",
            start_date=None,
            crossover_type="reference",
            cycle_length=9.9156,
            window_size=12,
            window_padding=2,
            max_pass_number=9999,
        )
        self.assertEqual(cfg.reference_product, "NASA-SSH")

    def test_reference_type_rejects_a_blank_reference_product(self):
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
                reference_product="",
            )
