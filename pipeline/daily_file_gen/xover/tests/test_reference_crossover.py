"""Real-data reference-crossover test.

Drives the full reference path through ``CrossoverProcessor.run()`` against real
S3B (high-latitude) + NASA-SSH (reference mission) granules, mocking aws_manager
at the S3 boundary so no network/bucket is needed. The granules are large and
live in the gitignored golden dataset (``test_data/golden/reference_crossover/``,
pulled on demand, not committed); this test ``skipUnless`` they are present, so
CI without them is green and a developer with them gets a real end-to-end check.

Unlike the self ConsistencyTestCase there is no frozen reference NetCDF here —
the assertions instead pin the *invariants* of a correct reference run
(bracket ordering, in-day time filter, interpolation-within-bracket, finiteness,
schema), which is what a real-data reference consistency check can guarantee
without a byte-for-byte oracle.
"""
import os
import re
import unittest
from unittest import mock

import numpy as np
import xarray as xr
from crossover.processor import SPECS, CrossoverProcessor

# Repo root: pipeline/daily_file_gen/xover/tests/ -> up 5.
REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")
)
# Golden dataset (gitignored; pulled on demand, not committed — too large to
# vendor because the reference mission's ~9.9-day repeat forces an ~11-day
# minimum window to bracket any crossing). See test_data/golden/.
GOLDEN_DIR = os.path.join(REPO_ROOT, "test_data", "golden", "reference_crossover", "input")
REF_GRANULE_DIR = os.path.join(GOLDEN_DIR, "Reference_Mission")
_HL_DIR = os.path.join(GOLDEN_DIR, "S3B")

DAY = np.datetime64("2025-02-07")
SOURCE = "S3B"
DF_VERSION = "p1"

# Local high-lat granules (D-1..D+1 overhang for the seed day).
_HL_FILES = {
    "20250206": os.path.join(_HL_DIR, "S3B_alt_hilat_at_v1_1_20250206.nc"),
    "20250207": os.path.join(_HL_DIR, "S3B_alt_hilat_at_v1_1_20250207.nc"),
    "20250208": os.path.join(_HL_DIR, "S3B_alt_hilat_at_v1_1_20250208.nc"),
}


def _sample_data_present() -> bool:
    if not all(os.path.exists(p) for p in _HL_FILES.values()):
        return False
    return os.path.isdir(REF_GRANULE_DIR) and bool(
        [f for f in os.listdir(REF_GRANULE_DIR) if f.endswith(".nc")]
    )


@unittest.skipUnless(
    _sample_data_present(),
    "golden reference dataset not present (test_data/golden/reference_crossover/; "
    "gitignored, pulled on demand)",
)
class ReferenceRealDataTestCase(unittest.TestCase):
    """End-to-end reference run on real S3B x NASA-SSH granules."""

    @classmethod
    def setUpClass(cls):
        # Map the 8-digit date token in any requested daily-file key to a local
        # granule: high-lat keys resolve to the S3B files, reference keys to the
        # NASA-SSH window directory.
        cls.ref_by_date = {}
        for name in os.listdir(REF_GRANULE_DIR):
            m = re.search(r"(\d{8})", name)
            if m:
                cls.ref_by_date[m.group(1)] = os.path.join(REF_GRANULE_DIR, name)

        def fake_key_exists(key):
            m = re.search(r"(\d{8})", key)
            if not m:
                return False
            token = m.group(1)
            if "NASA-SSH" in key:
                return token in cls.ref_by_date
            return token in _HL_FILES

        def fake_stream_obj(key):
            token = re.search(r"(\d{8})", key).group(1)
            path = cls.ref_by_date[token] if "NASA-SSH" in key else _HL_FILES[token]
            return open(path, "rb")

        cls.uploaded = []
        processor = CrossoverProcessor(DAY, SOURCE, DF_VERSION, SPECS["reference"])
        with mock.patch("crossover.loader.aws_manager") as loader_mgr, mock.patch(
            "crossover.processor.aws_manager"
        ) as proc_mgr:
            loader_mgr.key_exists.side_effect = fake_key_exists
            loader_mgr.stream_obj.side_effect = fake_stream_obj
            proc_mgr.upload_obj.side_effect = lambda src, dest: cls.uploaded.append(src)
            processor.run(bucket="test-bucket")

        cls.local_path = cls.uploaded[0]
        cls.ds = xr.open_dataset(cls.local_path, engine="h5netcdf")

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "ds"):
            cls.ds.close()
        if getattr(cls, "local_path", None) and os.path.exists(cls.local_path):
            os.remove(cls.local_path)

    def test_uploaded_exactly_once(self):
        self.assertEqual(len(self.uploaded), 1)

    def test_produced_records(self):
        # The 22-day NASA-SSH window brackets many crossings for the S3B day.
        self.assertGreater(len(self.ds["time1"]), 0)

    def test_reference_schema(self):
        for var in (
            "lon",
            "lat",
            "ssh1",
            "cycle1",
            "pass1",
            "ssh2",
            "pass2",
            "ref_cycle_before",
            "ref_time_before",
            "ref_ssha_before",
            "ref_cycle_after",
            "ref_time_after",
            "ref_ssha_after",
        ):
            self.assertIn(var, self.ds.data_vars, f"missing {var}")
        self.assertIn("time1", self.ds.dims)

    def test_all_values_finite(self):
        for var in ("lon", "lat", "ssh1", "ssh2", "ref_ssha_before", "ref_ssha_after"):
            values = self.ds[var].values
            self.assertTrue(
                np.all(np.isfinite(values)), f"{var} has non-finite values"
            )

    def test_sorted_and_within_processing_day(self):
        time1 = self.ds["time1"].values
        self.assertTrue(np.all(np.diff(time1.astype("int64")) >= 0))
        self.assertGreaterEqual(time1.min(), DAY)
        self.assertLess(time1.max(), DAY + np.timedelta64(1, "D"))

    def test_bracket_straddles_high_lat_time(self):
        # ref_time_before < time1 <= ref_time_after for every record (decision 4).
        t1 = self.ds["time1"].values.astype("int64")
        tb = self.ds["ref_time_before"].values.astype("int64")
        ta = self.ds["ref_time_after"].values.astype("int64")
        self.assertTrue(np.all(tb < t1), "some before-times are not strictly before")
        self.assertTrue(np.all(t1 <= ta), "some after-times precede the crossover")

    def test_ssh2_within_bracket(self):
        # Time-interpolation never extrapolates: ssh2 lies between the bracket ssh.
        before = self.ds["ref_ssha_before"].values
        after = self.ds["ref_ssha_after"].values
        ssh2 = self.ds["ssh2"].values
        lo = np.minimum(before, after)
        hi = np.maximum(before, after)
        self.assertTrue(np.all(ssh2 >= lo - 1e-9))
        self.assertTrue(np.all(ssh2 <= hi + 1e-9))

    def test_same_origin_bracket(self):
        # Both bracket cycles come from the same contributing mission (decision 8):
        # their cycle numbers are adjacent, never thousands apart.
        cb = self.ds["ref_cycle_before"].values
        ca = self.ds["ref_cycle_after"].values
        self.assertTrue(np.all(np.abs(ca - cb) < 100))

    def test_attrs_name_both_missions(self):
        self.assertIn("S3B", self.ds.attrs["satellite_names"])
        self.assertIn("NASA-SSH", self.ds.attrs["satellite_names"])
        self.assertEqual(self.ds.attrs["reference_version"], "p3")

    def test_crossover_type_attr(self):
        self.assertEqual(self.ds.attrs["crossover_type"], "reference")


if __name__ == "__main__":
    unittest.main()
