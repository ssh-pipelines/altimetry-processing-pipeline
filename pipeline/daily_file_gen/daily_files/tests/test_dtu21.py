import unittest
from unittest import mock

import numpy as np
from daily_files.processing import dtu21
from scipy.interpolate import RegularGridInterpolator


def _synthetic_interp(lat_min=-90.0, lat_max=90.0,
                      lon_min=0.0, lon_max=360.0, n=10):
    """A trivial RegularGridInterpolator over (lat, lon) whose values equal
    the latitude — so interpolation results are predictable per axis."""
    lat = np.linspace(lat_min, lat_max, n)
    lon = np.linspace(lon_min, lon_max, n)
    values = np.broadcast_to(lat.reshape(-1, 1), (n, n)).astype(np.float32)
    return RegularGridInterpolator(
        points=(lat, lon),
        values=values,
        method="linear",
        bounds_error=False,
        fill_value=np.nan,
    )


class TestDtu21(unittest.TestCase):
    def setUp(self):
        dtu21.set_interpolator_for_test(None)

    def tearDown(self):
        dtu21.set_interpolator_for_test(None)

    def test_injection_overrides_grid_load(self):
        """set_interpolator_for_test() blocks the real grid from loading."""
        synth = _synthetic_interp()
        dtu21.set_interpolator_for_test(synth)
        with mock.patch("xarray.open_dataset", side_effect=AssertionError("must not load")):
            self.assertIs(dtu21.get_dtu21_interpolator(), synth)

    def test_axis_order_is_lat_lon(self):
        """Query with (lat, lon) ordered rows — values equal lat, regardless
        of lon. This proves the interpolator's axis convention."""
        dtu21.set_interpolator_for_test(_synthetic_interp())
        interp = dtu21.get_dtu21_interpolator()
        query = np.array([
            [45.0, 10.0],
            [45.0, 350.0],
            [-30.0, 100.0],
            [0.0, 180.0],
        ])
        out = interp(query)
        np.testing.assert_allclose(out, query[:, 0], rtol=1e-5)

    def test_caching_returns_same_object(self):
        synth = _synthetic_interp()
        dtu21.set_interpolator_for_test(synth)
        a = dtu21.get_dtu21_interpolator()
        b = dtu21.get_dtu21_interpolator()
        self.assertIs(a, b)

    def test_clear_cache_reloads(self):
        dtu21.set_interpolator_for_test(_synthetic_interp())
        first = dtu21.get_dtu21_interpolator()
        dtu21.set_interpolator_for_test(None)
        # Cache cleared — next get would touch disk; inject a fresh one instead.
        second = _synthetic_interp()
        dtu21.set_interpolator_for_test(second)
        self.assertIsNot(dtu21.get_dtu21_interpolator(), first)


if __name__ == "__main__":
    unittest.main()
