import os

import geopandas as gpd
import numpy as np
import xarray as xr

from daily_files.config.paths import REF_FILES_DIR
from daily_files.config.source_config import SourceConfig
from daily_files.processing.daily_file import get_base_global_attrs, get_var_attrs

# Source-flag configuration per processor class name.
# Update these when flag column definitions change in the corresponding DailyFile subclass.
_SOURCE_FLAG_CONFIG: dict[str, dict] = {
    "GSFCDailyFile": {
        "src_flag_dim": 15,
        "nasa_flag_derivation": (
            "nasa_flag is 0 if: basin_flag is set to any valid, non-fill value & data passes an along-track "
            "median check, saved in the medain_filter_flag variable & the following source_flag values are set "
            "to 0: Radiometer_Observation_is_Suspect, Attitude_Out_of_Range, Sigma0_Ku_Band_Out_of_Range, "
            "Sea_Ice_Detected"
        ),
        "source_flag_attrs": {
            "standard_name": "quality_flag",
            "long_name": "Source data flag",
            "comment": "GSFC flags used to calculate nasa_flag. See documentation for more details.",
            "coverage_content_type": "auxiliaryInformation",
            "flag_column_1": "abs(SSH(cycle)-SSH(cycle +/-1))>50cm",
            "flag_column_2": "Radiometer_Observation_is_Suspect",
            "flag_column_3": "Attitude_Out_of_Range",
            "flag_column_4": "Sigma0_Ku_Band_Out_of_Range",
            "flag_column_5": "Possible_Rain_Contamination",
            "flag_column_6": "Sea_Ice_Detected",
            "flag_column_7": "Significant_Wave_Height>8m",
            "flag_column_8": "Cross_Track_slope>10cm/km",
            "flag_column_9": "Cross_Track_Distance>1km",
            "flag_column_10": "Any_Applied_SSH_Correction_Out_of_Limits",
            "flag_column_11": "Contiguous_1Hz_Data",
            "flag_column_12": "Sigma_H_of_fit>15cm",
            "flag_column_13": "Distance_to_Land<50km",
            "flag_column_14": "Water_Depth<200m",
            "flag_column_15": "Single_Frequency_Altimeter",
            "flag_values": np.array([0, 1], dtype=np.int8),
            "flag_meanings": "good bad",
        },
    },
    "S6DailyFile": {
        "src_flag_dim": 4,
        "nasa_flag_derivation": (
            "nasa_flag is set to 0 for data that should be retained, and 1 for data that should be "
            "removed. nasa_flag is 0 if: basin_flag is set to any valid, non-fill value & data passes "
            "an along-track median check, saved in the medain_filter_flag variable & the following "
            "source_flag values are set to 0: surface_classification_flag (0 or 2), rain_flag_nr, "
            "range_ocean_nr_qual, rad_water_vapor_qual, and derived standard deviation"
        ),
        "source_flag_attrs": {
            "standard_name": "quality_flag",
            "long_name": "Source data flag",
            "comment": "S6 flags used to calculate nasa_flag. See documentation for more details.",
            "flag_values": np.array([0, 1], dtype=np.int8),
            "flag_meanings": "good bad",
            "flag_column_1": "range_ocean_nr_qual",
            "flag_column_2": "surface_classification_flag",
            "flag_column_3": "rad_water_vapor_qual",
            "flag_column_4": "rain_flag_nr",
        },
    },
    "AvisoL2PDailyFile": {
        "src_flag_dim": 1,
        "nasa_flag_derivation": (
            "nasa_flag is set to 0 for data that should be retained, and 1 for data that "
            "should be removed. nasa_flag is 0 if: basin_flag is set to any valid, non-fill "
            "value & data passes an along-track median check, saved in the median_filter_flag "
            "variable & the following source_flag values are set to 0: validation_flag"
        ),
        "source_flag_attrs": {
            "standard_name": "quality_flag",
            "long_name": "Source data flag",
            "comment": "AVISO L2P validation flag. See documentation for more details.",
            "coverage_content_type": "auxiliaryInformation",
            "flag_column_1": "validation_flag",
            "flag_values": np.array([0, 1], dtype=np.int8),
            "flag_meanings": "good bad",
        },
    },
}

_MEDIAN_FILTER_FLAG_ATTRS = {
    "standard_name": "quality_flag",
    "long_name": "median filter flag",
    "comment": (
        "flag set to 0 for good data, 1 for data that fail a 5 standard deviation filter relative "
        "to a 15-point along-track median. See documentation for details."
    ),
    "flag_values": np.array([0, 1], dtype=np.int8),
    "flag_meanings": "good bad",
}


def _load_basin_data() -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Read the basin shapefile and return (basin_ids, names, basin_names_table array)."""
    poly_df = gpd.read_file(os.path.join(REF_FILES_DIR, "basin", "new_basin_lake_polygons.shp"))
    names = poly_df["name"].apply(lambda x: x.replace("'", " ").replace(",", " -")).values
    basin_ids = poly_df["feature_id"].astype(str).values
    basin_table = np.array([f"{basin},{name}" for basin, name in zip(basin_ids, names)])
    basin_table = np.insert(basin_table, 0, "0,Land", axis=0)
    return basin_ids, names, basin_table


def build_empty_dataset(source_config: SourceConfig, processor_cls: type) -> xr.Dataset:
    """
    Build an empty daily file dataset with the correct structure and metadata for the given source.
    Used by make_empty() when no source data is available for a given date.
    """
    processor_name = processor_cls.__name__
    if processor_name not in _SOURCE_FLAG_CONFIG:
        raise ValueError(
            f"No empty template config for processor '{processor_name}'. "
            f"Known processors: {list(_SOURCE_FLAG_CONFIG)}"
        )
    flag_cfg = _SOURCE_FLAG_CONFIG[processor_name]
    src_flag_dim = flag_cfg["src_flag_dim"]

    # high_latitude sources don't carry a target_mss in config — they normalize
    # to DTU21 by interpolation at processing time (see ADR 0002).
    target_mss = source_config.target_mss or "DTU21"

    basin_ids, names, basin_table = _load_basin_data()

    ds = xr.Dataset(
        data_vars={
            "ssha": (("time",), np.array([], dtype=np.float64)),
            "dac": (("time",), np.array([], dtype=np.float64)),
            "inv_bar_cor": (("time",), np.array([], dtype=np.float64)),
            "latitude": (("time",), np.array([], dtype=np.float32)),
            "longitude": (("time",), np.array([], dtype=np.float32)),
            "cycle": (("time",), np.array([], dtype=np.float64)),
            "pass": (("time",), np.array([], dtype=np.float64)),
            "basin_flag": (("time",), np.array([], dtype=np.float64)),
            "nasa_flag": (("time",), np.array([], dtype=np.float32)),
            "source_flag": (("time", "src_flag_dim"), np.empty((0, src_flag_dim), dtype=np.float32)),
            "median_filter_flag": (("time",), np.array([], dtype=np.float32)),
            "ssha_smoothed": (("time",), np.array([], dtype=np.float64)),
            "oer": (("time",), np.array([], dtype=np.float64)),
            "basin_names_table": (("basins",), basin_table.astype("unicode")),
        },
        coords={"time": np.array([], dtype="datetime64[ns]")},
    )

    ds["time"].encoding["units"] = "seconds since 1990-01-01"

    # Variable attrs shared across all sources
    for var, attrs in get_var_attrs(target_mss).items():
        for attr, value in attrs.items():
            ds[var].attrs[attr] = value

    # basin_flag flag_values / flag_meanings (from shapefile)
    ds["basin_flag"].attrs["flag_values"] = np.array(basin_ids, dtype=np.int32)
    ds["basin_flag"].attrs["flag_meanings"] = " ".join(
        [name.replace(": ", ":").replace(" ", "_").replace(":", "_") for name in names]
    )

    # oer attrs (set by the OER Lambda in normal processing; replicated here for structural completeness)
    ds["oer"].attrs = {
        "units": "m",
        "long_name": "Orbit error reduction",
        "comment": "Add this variable to ssh and ssh_smoothed to reduce orbit error",
        "coverage_content_type": "auxiliaryInformation",
        "valid_min": -1.0e100,
        "valid_max": 1.0e100,
    }

    # Source-specific flag attrs
    ds["nasa_flag"].attrs["flag_derivation"] = flag_cfg["nasa_flag_derivation"]
    for attr, value in flag_cfg["source_flag_attrs"].items():
        ds["source_flag"].attrs[attr] = value
    for attr, value in _MEDIAN_FILTER_FLAG_ATTRS.items():
        ds["median_filter_flag"].attrs[attr] = value

    # Global attrs
    ds.attrs.update(get_base_global_attrs())
    ds.attrs["mean_sea_surface"] = target_mss
    ds.attrs["granule_id"] = ""
    ds.attrs["flagged_passes"] = "N/A"

    return ds
