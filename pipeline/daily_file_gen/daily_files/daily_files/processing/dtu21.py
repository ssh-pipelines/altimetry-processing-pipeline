"""Cached bilinear DTU21 MSS interpolator for AVISO L2P processing.

The bundled global grid at `ref_files/mss/DTU21MSS_1min_WGS84.nc` (DTU's
official WGS84 release) is loaded once per Lambda container;
`RegularGridInterpolator` is built over (lat, lon) — the same axis order as the
on-disk `mean_sea_surf_sol2[lat, lon]` layout, so no transpose is needed. The
grid MUST be on the WGS84 ellipsoid to match AVISO L2P's `mean_sea_surface`; an
earlier bundled grid on the TOPEX/Poseidon datum injected a ~0.71 m offset into
S3B ssha (see docs/findings/2026-08-06-s3b-mss-swap-ellipsoid-offset.md). xarray
decodes the int32+scale_factor encoding to float64 by default; we cast to
float32 to halve resident memory (~1.87 GB → ~933 MB) at no meaningful precision
cost for MSS use.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import xarray as xr
from scipy.interpolate import RegularGridInterpolator

_DTU21_PATH = (
    Path(__file__).parent.parent / "ref_files" / "mss" / "DTU21MSS_1min_WGS84.nc"
)

_interpolator: RegularGridInterpolator | None = None


def get_dtu21_interpolator() -> RegularGridInterpolator:
    """Return the module-cached DTU21 interpolator, building it on first call."""
    global _interpolator
    if _interpolator is None:
        with xr.open_dataset(_DTU21_PATH) as ds:
            lat = ds["lat"].values
            lon = ds["lon"].values
            mss = ds["mean_sea_surf_sol2"].values.astype(np.float32)
        _interpolator = RegularGridInterpolator(
            points=(lat, lon),
            values=mss,
            method="linear",
            bounds_error=False,
            fill_value=np.nan,
        )
    return _interpolator


def set_interpolator_for_test(interp: RegularGridInterpolator | None) -> None:
    """Inject a synthetic interpolator (or clear the cache with None). Tests
    use this to avoid loading the bundled ~1 GB grid in CI."""
    global _interpolator
    _interpolator = interp
