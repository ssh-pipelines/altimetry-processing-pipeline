import os
import tempfile
import unittest

import netCDF4 as nc
import numpy as np
from daily_files.ingestion import orbit_swap
from pyproj import Transformer

SAMPLE_DATA = os.path.join(os.path.dirname(__file__), "sample_data")
GRANULE = os.path.join(SAMPLE_DATA, "S6A_orbit_swap_subset.nc")
ORBIT = os.path.join(SAMPLE_DATA, "2026-09-05_s6an_jplpoe_subset.pos")
C_CORRECTION = os.path.join(SAMPLE_DATA, "S6A_orbit_swap_subset_c_correction.txt")

# 1 um is the resolution the C program printed its output at in production, and
# a thousandth of the ~mm orbit correction. The port agrees with it to a few nm.
C_AGREEMENT_M = 1.0e-6


def _granule_bytes() -> bytes:
    with open(GRANULE, "rb") as f:
        return f.read()


def _ku(name: str) -> np.ndarray:
    with nc.Dataset(GRANULE) as ds:
        return np.ma.filled(ds["data_01"]["ku"][name][:].astype(np.float64), np.nan)


class TestOrbitSwapMatchesC(unittest.TestCase):
    """Guards the port against interpPosGoaToNetCDFtimes.c, which it replaced.

    The fixture is 137 samples of S6A NTC cycle 214 pass 137 (2026-09-05): every
    30th sample plus two runs where only one of ssha and ssha_nr is fill, with the
    matching window of the JPL POE orbit. The C applied the correction to ssha and
    cannot read ssha_nr, so the reference is its orbit correction column, printed
    at full precision, and the expected output is ssha_nr plus that correction.
    """

    @classmethod
    def setUpClass(cls):
        cls.correction = np.loadtxt(C_CORRECTION)[:, 1]
        cls.ssha_nr = _ku("ssha_nr")
        cls.swapped = orbit_swap.run_orbit_swap(_granule_bytes(), ORBIT)

    def test_matches_c_correction_applied_to_ssha_nr(self):
        self.assertEqual(len(self.swapped), len(self.correction))
        np.testing.assert_allclose(
            self.swapped, self.ssha_nr + self.correction, rtol=0, atol=C_AGREEMENT_M
        )

    def test_fill_pattern_follows_ssha_nr(self):
        ssha_fill = np.isnan(_ku("ssha"))
        nr_fill = np.isnan(self.ssha_nr)
        # The fixture must separate the two retrackers' fill patterns in both directions.
        self.assertTrue((ssha_fill & ~nr_fill).any())
        self.assertTrue((nr_fill & ~ssha_fill).any())
        np.testing.assert_array_equal(np.isnan(self.swapped), nr_fill)


class TestOrbitSwapFailurePaths(unittest.TestCase):
    def test_orbit_not_covering_pass_raises(self):
        with open(ORBIT) as f:
            lines = f.readlines()
        with tempfile.NamedTemporaryFile("w", suffix=".pos", delete=False) as tmp:
            tmp.writelines(lines[len(lines) // 2:])
        try:
            with self.assertRaisesRegex(orbit_swap.OrbitSwapError, "outside the orbit file span"):
                orbit_swap.run_orbit_swap(_granule_bytes(), tmp.name)
        finally:
            os.remove(tmp.name)

    def test_orbit_too_short_for_stencil_raises(self):
        with self.assertRaises(ValueError):
            orbit_swap.lagrange(np.array([0.5]), np.arange(4.0), (np.arange(4.0),))

    def test_non_positive_flattening_raises(self):
        with self.assertRaises(ValueError):
            orbit_swap.geodetic_height(
                np.array([7.0e6]), np.array([0.0]), np.array([0.0]), 6378137.0, 0.0
            )


class TestOrbitSwapComponents(unittest.TestCase):
    def test_lagrange_reproduces_degree_seven_polynomial(self):
        xt = np.arange(0.0, 200.0, 10.0)
        coef = [3.0, -1.0, 0.5, 2.0e-2, -1.0e-3, 2.0e-5, -1.0e-7, 1.0e-10]
        yt = np.polynomial.polynomial.polyval(xt, coef)
        x = np.array([0.0, 3.3, 47.1, 105.0, 189.9, 190.0])
        (y,) = orbit_swap.lagrange(x, xt, (yt,))
        np.testing.assert_allclose(y, np.polynomial.polynomial.polyval(x, coef), rtol=1e-12)

    def test_geodetic_height_matches_pyproj(self):
        # Independent check of the Newton iteration at S6 altitude across latitudes.
        ae, f = 6378137.0, 0.003352810664747
        to_ecef = Transformer.from_crs(
            {"proj": "longlat", "a": ae, "rf": 1.0 / f},
            {"proj": "geocent", "a": ae, "rf": 1.0 / f},
        )
        lat = np.array([-66.2, -30.0, 0.0, 12.5, 45.0, 66.2])
        lon = np.array([10.0, 200.0, 359.0, 90.0, -45.0, 180.0])
        h = np.full(lat.shape, 1.336e6)
        x, y, z = to_ecef.transform(lon, lat, h)
        glat, gh = orbit_swap.geodetic_height(x, y, z, ae, f)
        np.testing.assert_allclose(np.degrees(glat), lat, rtol=0, atol=1e-9)
        np.testing.assert_allclose(gh, h, rtol=0, atol=1e-6)


if __name__ == "__main__":
    unittest.main()
