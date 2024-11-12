"""
Script to consist of encoding code for saving netcdfs across full pipeline
"""

import xarray as xr
import numpy as np

def daily_file_encoding(ds: xr.Dataset) -> dict:
    # TODO: move setting of coords out of encoding
    ds = ds.set_coords(["latitude", "longitude"])
    encoding = {"time": {"units": "seconds since 1990-01-01 00:00:00", "dtype": "float64", "_FillValue": None}}
    for var in ds.variables:
        if var not in ["latitude", "longitude", "time", "basin_names_table"]:
            encoding[var] = {"complevel": 5, "zlib": True}
        elif "lat" in var or "lon" in var:
            encoding[var] = {"complevel": 5, "zlib": True, "dtype": "float32", "_FillValue": None}

        if any(x in var for x in ["source_flag", "nasa_flag", "median_filter_flag"]):
            encoding[var]["dtype"] = "int8"
            encoding[var]["_FillValue"] = np.iinfo(np.int8).max
        if any(x in var for x in ["basin_flag", "pass", "cycle"]):
            encoding[var]["dtype"] = "int32"
            encoding[var]["_FillValue"] = np.iinfo(np.int32).max
        if any(x in var for x in ["ssh", "dac", "oer"]):
            encoding[var]["dtype"] = "float64"
            encoding[var]["_FillValue"] = np.finfo(np.float64).max
    return encoding

def simple_grid_encoding(ds: xr.Dataset) -> dict:
    # TODO: move setting of coords out of encoding
    ds = ds.set_coords(["latitude", "longitude"])
    encoding = {"time": {"units": "seconds since 1990-01-01 00:00:00", "dtype": "float64", "_FillValue": None}}
    for var in ds.variables:
        if var not in ["latitude", "longitude", "time", "basin_names_table"]:
            encoding[var] = {"complevel": 5, "zlib": True}
        elif "lat" in var or "lon" in var:
            encoding[var] = {"complevel": 5, "zlib": True, "dtype": "float32", "_FillValue": None}

        if any(x in var for x in ["basin_flag", "counts"]):
            encoding[var]["dtype"] = "int32"
            encoding[var]["_FillValue"] = np.iinfo(np.int32).max
        if any(x in var for x in ["SSHA"]):
            encoding[var]["dtype"] = "float64"
            encoding[var]["_FillValue"] = np.finfo(np.float64).max
    return encoding