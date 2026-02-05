import unittest
from crossover.config.source_config import (
    get_source_config,
    get_available_sources,
    SourceConfig,
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
        self.assertEqual(cfg.cycle_length, 9.9156)
        self.assertEqual(cfg.window_size, 10)
        self.assertEqual(cfg.window_padding, 2)
        self.assertEqual(cfg.max_pass_number, 9999)

    def test_s6_config_fields(self):
        cfg = get_source_config("S6")
        self.assertIsInstance(cfg, SourceConfig)
        self.assertEqual(cfg.source, "S6")
        self.assertEqual(cfg.cycle_length, 9.9156)
        self.assertEqual(cfg.window_size, 10)
        self.assertEqual(cfg.window_padding, 2)
        self.assertEqual(cfg.max_pass_number, 9999)
