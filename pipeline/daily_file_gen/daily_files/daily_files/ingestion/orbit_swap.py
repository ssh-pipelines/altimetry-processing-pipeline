"""Orbit swap: recompute S6 ssha_nr against a JPL precise orbit.

Port of interpPosGoaToNetCDFtimes.c (S. Desai, JPL), which this replaces. The orbit
is Lagrange-interpolated to each 1 Hz time and converted to geodetic height on the
product's ellipsoid; the height difference from the product's own altitude is added
to ssha_nr. The operation order follows the C so the correction agrees to a few nm.
"""

import netCDF4 as nc
import numpy as np

TAI_MINUS_GPS = 19.0
# pos_goa times count from 2000-01-01 12:00 GPS, product time_tai from 00:00 TAI.
POS_GOA_EPOCH_OFFSET = 43200.0
LAGRANGE_DEGREE = 7
GEODETIC_TOLERANCE = 1.0e-14  # fraction of the semi-major axis
MAX_NEWTON_ITERATIONS = 100


class OrbitSwapError(Exception):
    """A pass file could not be swapped onto the precise orbit."""


def read_pos_goa(path: str) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Return GPS seconds since 2000-01-01 12:00 and ECEF X, Y, Z in metres."""
    cols = np.loadtxt(path, usecols=(2, 3, 4, 5, 6), dtype=np.float64, ndmin=2)
    return cols[:, 0] + cols[:, 1], cols[:, 2] * 1.0e3, cols[:, 3] * 1.0e3, cols[:, 4] * 1.0e3


def _bracket(x: np.ndarray, xt: np.ndarray) -> np.ndarray:
    """Index i with xt[i] <= x <= xt[i+1], found the way the C finds it."""
    ntab = len(xt)
    i = np.floor((ntab - 1) * (x - xt[0]) / (xt[-1] - xt[0])).astype(np.int64)
    i[i == ntab - 1] -= 1
    below = x < xt[i]
    above = x > xt[i + 1]
    i[below] = np.searchsorted(xt, x[below], side="right") - 1
    i[above] = np.searchsorted(xt, x[above], side="left") - 1
    return i


def lagrange(x: np.ndarray, xt: np.ndarray, yts: tuple[np.ndarray, ...]) -> list[np.ndarray]:
    """Interpolate each table in yts at x with an 8-point Lagrange stencil."""
    ntab = len(xt)
    n = LAGRANGE_DEGREE + 1
    if ntab < n:
        raise ValueError(f"orbit has {ntab} records, need at least {n}")
    i1 = _bracket(x, xt) - n // 2 + 1
    i1 = np.clip(i1, 0, ntab - n)
    idx = i1[:, None] + np.arange(n)[None, :]
    xi = xt[idx]

    w = np.empty_like(xi)
    for a in range(n):
        df = np.ones(len(x))
        for b in range(n):
            if b != a:
                df = df / (xi[:, a] - xi[:, b])
        for b in range(n):
            if b != a:
                df = df * (x - xi[:, b])
        w[:, a] = df

    out = []
    for yt in yts:
        yi = yt[idx]
        y = np.zeros(len(x))
        for a in range(n):
            y = y + w[:, a] * yi[:, a]
        out.append(y)
    return out


def geodetic_height(
    x: np.ndarray, y: np.ndarray, z: np.ndarray, ae: float, f: float
) -> tuple[np.ndarray, np.ndarray]:
    """ECEF metres to geodetic latitude (rad) and height (m) by Newton iteration."""
    if f <= 0.0:
        raise ValueError(f"ellipsoid flattening must be positive, got {f}")
    r = np.sqrt(x * x + y * y + z * z)
    p = np.sqrt(x * x + y * y)
    if np.any(p == 0.0):
        raise ValueError("orbit sample on the polar axis")
    tolerance = ae * GEODETIC_TOLERANCE

    oof = 1.0 / f
    esq = f * (2.0 - f)
    lat = np.arctan((oof * oof * z) / (p * (1.0 - oof) * (1.0 - oof)))
    N = ae / np.sqrt(1.0 - esq * np.sin(lat) * np.sin(lat))
    x0 = N * np.cos(lat)
    y0 = N * (1.0 - esq) * np.sin(lat)
    hm = np.sqrt((p - x0) * (p - x0) + (z - y0) * (z - y0))
    h = np.where(r - np.sqrt(x0 * x0 + y0 * y0) < 0.0, -hm, hm)

    u = (N + h) * np.cos(lat) - p
    v = (N * (1.0 - esq) + h) * np.sin(lat) - z
    active = (np.abs(u) > tolerance) | (np.abs(v) > tolerance)
    iteration = 0
    # Each sample is iterated only until it converges, as the C's per-sample loop does.
    while active.any():
        if iteration > MAX_NEWTON_ITERATIONS:
            raise RuntimeError("geodetic latitude and height did not converge")
        la, hh, Na = lat[active], h[active], N[active]
        Nw2g = (Na / ae) * (Na / ae) * Na * esq * np.sin(la) * np.cos(la)
        uw2g = Nw2g * np.cos(la) - (Na + hh) * np.sin(la)
        vw2g = Nw2g * np.sin(la) * (1.0 - esq) + (Na * (1.0 - esq) + hh) * np.cos(la)
        uw2h = np.cos(la)
        vw2h = np.sin(la)
        det = uw2g * vw2h - uw2h * vw2g
        la = la + (-vw2h * u[active] + uw2h * v[active]) / det
        hh = hh + (vw2g * u[active] - uw2g * v[active]) / det
        Na = ae / np.sqrt(1.0 - esq * np.sin(la) * np.sin(la))
        lat[active], h[active], N[active] = la, hh, Na
        u[active] = (Na + hh) * np.cos(la) - p[active]
        v[active] = (Na * (1.0 - esq) + hh) * np.sin(la) - z[active]
        iteration += 1
        active = (np.abs(u) > tolerance) | (np.abs(v) > tolerance)
    return lat, h


def _scaled(var: nc.Variable) -> tuple[np.ndarray, float, float, int | None]:
    var.set_auto_maskandscale(False)
    return (
        np.asarray(var[:]),
        float(var.scale_factor),
        float(getattr(var, "add_offset", 0.0)),
        getattr(var, "_FillValue", None),
    )


def run_orbit_swap(data: bytes, orbit_path: str) -> np.ndarray:
    """Return orbit-swapped ssha_nr (m) for every 1 Hz sample of an S6 pass file.

    Fill ssha_nr stays NaN. Raises OrbitSwapError when any sample falls outside
    the orbit file's span, since the pass cannot be swapped whole.
    """
    t_orb, X, Y, Z = read_pos_goa(orbit_path)
    with nc.Dataset("orbit_swap", "r", memory=data) as ds:
        ae = float(ds.ellipsoid_semi_major_axis)
        f = float(ds.ellipsoid_flattening)
        g = ds["data_01"]
        g["time_tai"].set_auto_maskandscale(False)
        ttai = np.asarray(g["time_tai"][:], dtype=np.float64)
        alt_raw, alt_scale, alt_offset, _ = _scaled(g["altitude"])
        ssha_raw, ssha_scale, _, ssha_fill = _scaled(g["ku"]["ssha_nr"])

    t_gps = ttai - TAI_MINUS_GPS - POS_GOA_EPOCH_OFFSET
    outside = (t_gps < t_orb[0]) | (t_gps > t_orb[-1])
    if outside.any():
        raise OrbitSwapError(f"{outside.sum()} of {len(t_gps)} samples outside the orbit file span")

    xs, ys, zs = lagrange(t_gps, t_orb, (X, Y, Z))
    _, h_orbit = geodetic_height(xs, ys, zs, ae, f)
    correction = h_orbit - (alt_raw.astype(np.float64) * alt_scale + alt_offset)
    ssha = np.where(ssha_raw != ssha_fill, ssha_raw.astype(np.float64) * ssha_scale, np.nan)
    return ssha + correction
