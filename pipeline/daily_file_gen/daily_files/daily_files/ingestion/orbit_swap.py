import logging
import os
import subprocess

import numpy as np

ORBIT_SWAP_EXECUTABLE = os.environ.get(
    "ORBIT_SWAP_EXECUTABLE", "/var/task/bin/interpPosGoaToNetCDFtimes.e"
)
_FILL_VALUE = 9999999999.0


def run_orbit_swap(netcdf_path: str, orbit_path: str) -> np.ndarray | None:
    """Run the C orbit-swap program and return orbit-swapped SSHA values.

    The C program prints one line per 1 Hz measurement with 11 space-separated
    columns. Column 11 (index 10) is the orbit-swapped SSHA in meters.
    Fill values (9999999999.0) are converted to NaN.

    Returns None if the executable fails or produces no data lines.
    """
    try:
        result = subprocess.run(
            [ORBIT_SWAP_EXECUTABLE, "-posgoa", orbit_path, "-netcdf", netcdf_path],
            capture_output=True,
            text=True,
            timeout=120,
        )
    except FileNotFoundError:
        logging.warning(f"Orbit swap executable not found: {ORBIT_SWAP_EXECUTABLE}")
        return None
    except subprocess.TimeoutExpired:
        logging.warning("Orbit swap executable timed out")
        return None

    if result.returncode != 0:
        logging.warning(f"Orbit swap failed (rc={result.returncode}): {result.stderr[:500]}")
        return None

    ssha_values = []
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) == 11:
            try:
                val = float(parts[10])
                ssha_values.append(np.nan if val == _FILL_VALUE else val)
            except ValueError:
                pass  # skip WARNING/SUCCESS lines or other non-data output

    if not ssha_values:
        logging.warning("Orbit swap produced no data rows in stdout")
        return None

    return np.array(ssha_values, dtype=np.float64)
