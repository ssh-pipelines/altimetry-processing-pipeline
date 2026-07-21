"""Tests for the Lambda handler's processor dispatch (app.py).

Pins the crossover_type -> spec dispatch so future registry changes preserve it.
"""
import unittest

import numpy as np
from app import get_processor
from crossover.processor import SPECS, CrossoverProcessor, ReferenceSpec, SelfSpec


class GetProcessorTestCase(unittest.TestCase):
    DAY = np.datetime64("2025-01-01")
    DF_VERSION = "p1"

    def test_self_source_returns_processor(self):
        proc = get_processor(self.DAY, "S6", self.DF_VERSION)
        self.assertIsInstance(proc, CrossoverProcessor)
        self.assertIsInstance(proc.spec, SelfSpec)
        self.assertEqual(proc.source, "S6")
        self.assertEqual(proc.df_version, self.DF_VERSION)

    def test_all_self_sources_dispatch(self):
        for source in ("GSFC", "S6", "S6B"):
            proc = get_processor(self.DAY, source, self.DF_VERSION)
            self.assertIsInstance(proc, CrossoverProcessor)
            self.assertIs(proc.spec, SPECS["self"])

    def test_reference_source_returns_reference_spec(self):
        proc = get_processor(self.DAY, "S3B", self.DF_VERSION)
        self.assertIsInstance(proc, CrossoverProcessor)
        self.assertIsInstance(proc.spec, ReferenceSpec)
        self.assertIs(proc.spec, SPECS["reference"])
        self.assertEqual(proc.source, "S3B")

    def test_reference_processor_has_centered_window(self):
        # Reference windows are centered on the day (±), unlike the self path's
        # forward-only window, and carry the reference mission's profile.
        proc = get_processor(self.DAY, "S3B", self.DF_VERSION)
        self.assertLess(proc.window_start, self.DAY)
        self.assertGreater(proc.window_end, self.DAY)
        self.assertEqual(proc.reference_profile.source, "NASA-SSH")

    def test_unknown_source_raises(self):
        with self.assertRaises(ValueError):
            get_processor(self.DAY, "NONEXISTENT", self.DF_VERSION)


if __name__ == "__main__":
    unittest.main()
