"""Unit tests for tools/local_daily_files.py.

Drive the real ingest and processing path on synthetic L2P granules, stubbing
only discovery, the HTTP download, and the DTU21 grid. Assert the P1 lands at the
bucket-mirroring local path and passes the output schema, that an existing file is
not rebuilt, and that a non-THREDDS source, an empty window, and a window outside
the source's range are refused.
"""
import os
import sys
import tempfile
import unittest
from datetime import date, datetime, timezone
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

import numpy as np
import xarray as xr
from scipy.interpolate import RegularGridInterpolator

# tools/ is the parent of this tests/ dir.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import local_daily_files  # noqa: E402
from daily_files.config.dataset_schema import validate_dataset  # noqa: E402
from daily_files.fetching.downloader import HttpDownloader  # noqa: E402
from daily_files.processing import dtu21  # noqa: E402
from enumeration.base import GranuleRef  # noqa: E402
from local_daily_files import build, granules_by_date, summarize  # noqa: E402

DAY = date(2025, 1, 7)


def _l2p_bytes(cycle: int, pass_num: int, n: int, t_start: str) -> BytesIO:
    """A synthetic L2P pass in memory: cycle/pass in global attrs, per-record
    SSHA, MSS, inter-mission bias, validation flag, and DAC."""
    rng = np.random.RandomState(cycle * 1000 + pass_num)
    times = (np.datetime64(t_start, "ns") + np.arange(n) * np.timedelta64(1, "s")).astype("datetime64[ns]")
    lat = np.linspace(-80, 80, n, dtype=np.float32)
    lon = np.full(n, 200.0, dtype=np.float32)
    ds = xr.Dataset(
        data_vars={
            "sea_level_anomaly": (("time",), rng.normal(0, 0.05, n).astype(np.float32)),
            "dynamic_atmospheric_correction": (("time",), rng.normal(0, 0.01, n).astype(np.float32)),
            # Equal to the synthetic DTU21 below, so the MSS swap is a no-op.
            "mean_sea_surface": (("time",), (20.0 + 0.1 * lat + 0.01 * lon).astype(np.float32)),
            "inter_mission_bias": (("time",), np.zeros(n, dtype=np.float32)),
            "validation_flag": (("time",), np.zeros(n, dtype=np.int8)),
            "latitude": (("time",), lat),
            "longitude": (("time",), lon),
        },
        coords={"time": times},
        attrs={"cycle_number": cycle, "pass_number": pass_num},
    )
    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
        path = tmp.name
    try:
        ds.to_netcdf(path, engine="h5netcdf")
        with open(path, "rb") as f:
            return BytesIO(f.read())
    finally:
        os.remove(path)


def _synthetic_dtu21() -> RegularGridInterpolator:
    lat = np.linspace(-90.0, 90.0, 181)
    lon = np.linspace(0.0, 360.0, 361)
    values = (20.0 + 0.1 * lat.reshape(-1, 1) + 0.01 * lon.reshape(1, -1)).astype(np.float32)
    return RegularGridInterpolator((lat, lon), values, method="linear", bounds_error=False, fill_value=np.nan)


GRANULES = {
    "https://tds.example/fileServer/c/global_sla_l2p_ntc_3b_C0101_P0001_x.nc": (101, 1, "2025-01-07T01:00:00"),
    "https://tds.example/fileServer/c/global_sla_l2p_ntc_3b_C0101_P0002_x.nc": (101, 2, "2025-01-07T02:00:00"),
}


def _refs(d: date = DAY) -> list[GranuleRef]:
    mod = datetime(2025, 3, 1, tzinfo=timezone.utc)
    # Deliberately out of sort_key order, to check the manifest ordering.
    return [GranuleRef(date=d, uri=uri, mod_time=mod, sort_key=(c, p)) for uri, (c, p, _) in reversed(GRANULES.items())]


class GranulesByDateTestCase(unittest.TestCase):
    def test_groups_and_orders_like_plan_jobs(self):
        refs = _refs() + _refs(date(2025, 1, 8))
        grouped = granules_by_date(refs)
        self.assertEqual(list(grouped), [DAY, date(2025, 1, 8)])
        self.assertEqual(grouped[DAY], list(GRANULES))


class BuildTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        dtu21.set_interpolator_for_test(_synthetic_dtu21())

    @classmethod
    def tearDownClass(cls):
        dtu21.set_interpolator_for_test(None)

    def setUp(self):
        self.out_dir = Path(tempfile.mkdtemp())
        self.downloads: list[str] = []

        def download(_self, uri):
            self.downloads.append(uri)
            c, p, t0 = GRANULES[uri]
            return _l2p_bytes(c, p, 1200, t0)

        patches = [
            # The registry captured build_aviso_session at import, so stub the
            # downloader itself rather than the session.
            patch.object(HttpDownloader, "__init__", lambda _self, **_kw: None),
            patch.object(HttpDownloader, "download", download),
            patch.object(local_daily_files.ThreddsEnumerator, "enumerate", lambda _self, s, e: _refs()),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def test_writes_p1_at_bucket_mirroring_path(self):
        written = build("S3B", DAY, DAY, self.out_dir)

        self.assertEqual(len(written), 1)
        rel = written[0].relative_to(self.out_dir)
        self.assertEqual(rel.parts[:4], ("daily_files", "p1", "S3B", "2025"))
        self.assertTrue(rel.name.endswith("20250107.nc"))

        with xr.open_dataset(written[0]) as ds:
            self.assertEqual(validate_dataset(ds), [])
            self.assertEqual(ds.sizes["time"], 2400)
            self.assertEqual(sorted(np.unique(ds["pass"].values)), [1, 2])
            self.assertIn("daily_files", ds.attrs["processing_history"])
            s = summarize(ds)
        self.assertGreater(s["smoothed_finite"], 0.0)
        self.assertLess(abs(s["ssha_median"]), 0.05)

    def test_existing_file_is_not_rebuilt(self):
        build("S3B", DAY, DAY, self.out_dir)
        self.downloads.clear()

        build("S3B", DAY, DAY, self.out_dir)
        self.assertEqual(self.downloads, [])

        build("S3B", DAY, DAY, self.out_dir, overwrite=True)
        self.assertEqual(len(self.downloads), 2)

    def test_refuses_non_thredds_source(self):
        with self.assertRaises(SystemExit) as cm:
            build("GSFC", date(2010, 1, 1), date(2010, 1, 2), self.out_dir)
        self.assertIn("THREDDS", str(cm.exception))

    def test_refuses_window_with_no_granules(self):
        with patch.object(local_daily_files.ThreddsEnumerator, "enumerate", lambda _self, s, e: []):
            with self.assertRaises(SystemExit) as cm:
                build("S3B", DAY, DAY, self.out_dir)
        self.assertIn("No S3B granules", str(cm.exception))

    def test_refuses_window_outside_source_range(self):
        # S3B starts 2018-11-24; the window is clamped to it and ends up empty.
        with self.assertRaises(SystemExit) as cm:
            build("S3B", date(2010, 1, 1), date(2010, 1, 14), self.out_dir)
        self.assertIn("outside", str(cm.exception))


class SummarizeTestCase(unittest.TestCase):
    def test_empty_file(self):
        ds = xr.Dataset({"ssha": (("time",), np.array([]))})
        self.assertEqual(summarize(ds), {"points": 0})

    def test_median_ignores_flagged_points(self):
        n = 10
        ds = xr.Dataset(
            {
                "ssha": (("time",), np.r_[np.zeros(8), 5.0, np.nan]),
                "ssha_smoothed": (("time",), np.r_[np.zeros(5), np.full(5, np.nan)]),
                "nasa_flag": (("time",), np.r_[np.zeros(8), 1, 1]),
                "latitude": (("time",), np.linspace(-70, 85, n)),
            }
        )
        s = summarize(ds)
        self.assertEqual(s["ssha_median"], 0.0)
        self.assertEqual(s["smoothed_finite"], 0.5)
        self.assertEqual(s["lat_max"], 85.0)


if __name__ == "__main__":
    unittest.main()
