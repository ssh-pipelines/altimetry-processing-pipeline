"""Tests for the Lambda handler's processor dispatch (app.py).

Pins the current crossover_type -> processor mapping so the SPECS-registry
rewrite (composed-processor refactor) preserves dispatch behavior.
"""
import unittest

import numpy as np
from app import get_processor
from crossover.parallel_crossovers import Crossover


class GetProcessorTestCase(unittest.TestCase):
    DAY = np.datetime64("2025-01-01")
    DF_VERSION = "p1"

    def test_self_source_returns_crossover(self):
        proc = get_processor(self.DAY, "S6", self.DF_VERSION)
        self.assertIsInstance(proc, Crossover)
        self.assertEqual(proc.source, "S6")
        self.assertEqual(proc.df_version, self.DF_VERSION)

    def test_all_self_sources_dispatch(self):
        for source in ("GSFC", "S6", "S6B"):
            proc = get_processor(self.DAY, source, self.DF_VERSION)
            self.assertIsInstance(proc, Crossover)

    def test_unknown_source_raises(self):
        with self.assertRaises(ValueError):
            get_processor(self.DAY, "NONEXISTENT", self.DF_VERSION)


if __name__ == "__main__":
    unittest.main()
