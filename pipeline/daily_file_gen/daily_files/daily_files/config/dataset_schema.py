"""Schema definition and validation for output NetCDF datasets.

The schema is derived from production GSFC and S6 sample outputs and defines
the required global attributes, required variables, and required per-variable
attributes that every output dataset must contain.
"""

import xarray as xr


# Global attrs that are allowed to be empty strings (e.g. when no source data is available)
ALLOW_EMPTY_GLOBAL_ATTRS = {
    "source",
    "source_url",
    "source_files",
    "references",
}

REQUIRED_GLOBAL_ATTRS = [
    "Conventions",
    "title",
    "summary",
    "institution",
    "source",
    "source_url",
    "source_files",
    "date_created",
    "history",
    "references",
    "mean_sea_surface",
    "standard_name_vocabulary",
    "id",
    "naming_authority",
    "project",
    "processing_level",
    "product_generation_step",
    "product_short_name",
    "acknowledgement",
    "license",
    "product_version",
    "keywords",
    "keywords_vocabulary",
    "cdm_data_type",
    "featureType",
    "platform",
    "instrument",
    "publisher_name",
    "publisher_url",
    "publisher_email",
    "creator_name",
    "creator_url",
    "creator_email",
    "geospatial_lat_min",
    "geospatial_lat_max",
    "geospatial_lon_min",
    "geospatial_lon_max",
    "time_coverage_start",
    "time_coverage_end",
]

REQUIRED_VARIABLES: dict[str, list[str]] = {
    "time": [
        "long_name",
        "standard_name",
        "coverage_content_type",
        "REFTime",
        "REFTime_comment",
    ],
    "latitude": [
        "long_name",
        "standard_name",
        "units",
        "coverage_content_type",
        "valid_min",
        "valid_max",
    ],
    "longitude": [
        "long_name",
        "standard_name",
        "units",
        "coverage_content_type",
        "valid_min",
        "valid_max",
    ],
    "ssha": [
        "long_name",
        "standard_name",
        "units",
        "coverage_content_type",
        "description",
        "mean_sea_surface",
        "valid_min",
        "valid_max",
    ],
    "ssha_smoothed": [
        "long_name",
        "standard_name",
        "units",
        "coverage_content_type",
        "description",
        "mean_sea_surface",
        "valid_min",
        "valid_max",
    ],
    "dac": [
        "long_name",
        "units",
        "coverage_content_type",
        "comment",
        "valid_min",
        "valid_max",
    ],
    "inv_bar_cor": [
        "long_name",
        "units",
        "coverage_content_type",
        "comment",
        "valid_min",
        "valid_max",
    ],
    "nasa_flag": [
        "long_name",
        "standard_name",
        "coverage_content_type",
        "description",
        "flag_values",
        "flag_meanings",
    ],
    "source_flag": [
        "long_name",
        "standard_name",
        "comment",
        "flag_values",
        "flag_meanings",
    ],
    "median_filter_flag": [
        "long_name",
        "standard_name",
        "comment",
        "flag_values",
        "flag_meanings",
    ],
    "basin_flag": ["long_name", "coverage_content_type", "comment", "reference"],
    "basin_names_table": [
        "long_name",
        "coverage_content_type",
        "description",
        "reference",
    ],
    "cycle": ["long_name", "coverage_content_type"],
    "pass": ["long_name", "coverage_content_type"],
}


def validate_dataset(ds: xr.Dataset) -> list[str]:
    """Validate a dataset against the output schema.

    Returns a list of error strings. An empty list means the dataset is valid.
    """
    errors = []

    for attr in REQUIRED_GLOBAL_ATTRS:
        if attr not in ds.attrs:
            errors.append(f"Missing global attribute: '{attr}'")
        elif (
            isinstance(ds.attrs[attr], str)
            and ds.attrs[attr] == ""
            and attr not in ALLOW_EMPTY_GLOBAL_ATTRS
        ):
            errors.append(f"Empty global attribute: '{attr}'")

    for var_name, required_attrs in REQUIRED_VARIABLES.items():
        if var_name not in ds:
            errors.append(f"Missing variable: '{var_name}'")
            continue
        for attr in required_attrs:
            if attr not in ds[var_name].attrs:
                errors.append(f"Variable '{var_name}' missing attribute: '{attr}'")

    return errors


def assert_valid_dataset(ds: xr.Dataset):
    """Raise ValueError if the dataset fails schema validation."""
    errors = validate_dataset(ds)
    if errors:
        raise ValueError(
            f"Dataset validation failed with {len(errors)} error(s):\n"
            + "\n".join(f"  - {e}" for e in errors)
        )
