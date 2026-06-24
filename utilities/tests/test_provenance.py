import json
import unittest

from utilities.provenance import (
    PROCESSING_HISTORY_ATTR,
    append_step,
    processing_complete,
    read_steps,
)


class _FakeXrDataset:
    """Stand-in for an xarray Dataset exposing only `.attrs`."""

    def __init__(self, attrs=None):
        self.attrs = dict(attrs or {})


class _FakeNcDataset:
    """Stand-in for a netCDF4 Dataset exposing ncattrs/get/set semantics."""

    def __init__(self, attrs=None):
        self._attrs = dict(attrs or {})

    def ncattrs(self):
        return list(self._attrs)

    def getncattr(self, name):
        return self._attrs[name]

    def setncattr(self, name, value):
        self._attrs[name] = value


class TestReadSteps(unittest.TestCase):
    def test_missing_value_is_empty(self):
        self.assertEqual(read_steps(None), [])

    def test_blank_value_is_empty(self):
        self.assertEqual(read_steps(""), [])

    def test_corrupt_value_is_empty(self):
        self.assertEqual(read_steps("{not json"), [])

    def test_non_list_value_is_empty(self):
        self.assertEqual(read_steps('{"a": 1}'), [])

    def test_round_trip(self):
        steps = read_steps('[{"stage": "oer"}]')
        self.assertEqual(steps, [{"stage": "oer"}])


class TestAppendStep(unittest.TestCase):
    def test_append_to_absent_starts_list(self):
        raw = append_step(None, stage="daily_files", generation_step=1)
        steps = json.loads(raw)
        self.assertEqual(len(steps), 1)
        self.assertEqual(steps[0]["stage"], "daily_files")
        self.assertEqual(steps[0]["product_generation_step"], "1")
        self.assertIn("timestamp", steps[0])

    def test_append_preserves_prior_steps(self):
        raw = append_step(None, stage="daily_files", generation_step=1)
        raw = append_step(raw, stage="oer", generation_step=2)
        raw = append_step(raw, stage="finalizer", generation_step=3)
        steps = json.loads(raw)
        self.assertEqual(
            [s["stage"] for s in steps], ["daily_files", "oer", "finalizer"]
        )

    def test_extra_fields_recorded(self):
        raw = append_step(None, stage="finalizer", generation_step=3, bad_passes_applied=True)
        self.assertTrue(json.loads(raw)[0]["bad_passes_applied"])


class TestProcessingComplete(unittest.TestCase):
    def test_complete_chain(self):
        steps = [
            {"product_generation_step": "1"},
            {"product_generation_step": "2"},
            {"product_generation_step": "3"},
        ]
        self.assertTrue(processing_complete(steps, 3))

    def test_gap_is_incomplete(self):
        # legacy file with only the later step recorded
        steps = [{"product_generation_step": "3"}]
        self.assertFalse(processing_complete(steps, 3))

    def test_empty_is_incomplete(self):
        self.assertFalse(processing_complete([], 3))


class TestAdapters(unittest.TestCase):
    def test_xr_append_round_trip(self):
        from utilities.provenance import append_to_xr

        ds = _FakeXrDataset()
        append_to_xr(ds, stage="daily_files", generation_step=1)
        append_to_xr(ds, stage="oer", generation_step=2)
        steps = json.loads(ds.attrs[PROCESSING_HISTORY_ATTR])
        self.assertEqual([s["stage"] for s in steps], ["daily_files", "oer"])

    def test_nc_append_and_read(self):
        from utilities.provenance import append_to_nc, read_from_nc

        ds = _FakeNcDataset()
        append_to_nc(ds, stage="finalizer", generation_step=3)
        steps = read_from_nc(ds)
        self.assertEqual(steps[0]["stage"], "finalizer")

    def test_nc_read_absent_is_empty(self):
        from utilities.provenance import read_from_nc

        self.assertEqual(read_from_nc(_FakeNcDataset()), [])


if __name__ == "__main__":
    unittest.main()
