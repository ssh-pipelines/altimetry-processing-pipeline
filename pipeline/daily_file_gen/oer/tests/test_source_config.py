"""
Tests for OER source config loading.

OerConfig inherits the canonical per-source ``common.ground_speed`` and
``product_type`` from ``SourceCommon`` and adds only the reference-path
``reference_window_size``. It has no ``oer:`` YAML section, so it is driven
entirely by ``common`` plus dataclass defaults.
"""
import unittest

from oer.config.source_config import OerConfig, get_source_config
from oer.oer import _CROSSOVER_TYPE_BY_PRODUCT_TYPE


class OerConfigTestCase(unittest.TestCase):
    def test_returns_oer_config(self):
        cfg = get_source_config("S6")
        self.assertIsInstance(cfg, OerConfig)
        self.assertEqual(cfg.source, "S6")

    def test_invalid_source_raises(self):
        with self.assertRaises(ValueError):
            get_source_config("NONEXISTENT")

    def test_s6_ground_speed_is_canonical_value(self):
        """S6's YAML sets the computed canonical ground speed (5.7529)."""
        cfg = get_source_config("S6")
        self.assertEqual(cfg.ground_speed, 5.7529)

    def test_reference_window_default(self):
        cfg = get_source_config("S6")
        self.assertEqual(cfg.reference_window_size, 2)

    def test_s3b_ground_speed_is_canonical_value(self):
        """S3B's YAML sets its characterized ground speed (6.6943)."""
        cfg = get_source_config("S3B")
        self.assertEqual(cfg.ground_speed, 6.6943)

    # ── product_type → crossover_type dispatch ─────────────────────

    def test_reference_mission_dispatches_to_self(self):
        cfg = get_source_config("S6")
        self.assertEqual(cfg.product_type, "reference")
        self.assertEqual(
            _CROSSOVER_TYPE_BY_PRODUCT_TYPE[cfg.product_type], "self"
        )

    def test_high_latitude_dispatches_to_reference(self):
        cfg = get_source_config("S3B")
        self.assertEqual(cfg.product_type, "high_latitude")
        self.assertEqual(
            _CROSSOVER_TYPE_BY_PRODUCT_TYPE[cfg.product_type], "reference"
        )


if __name__ == "__main__":
    unittest.main()
