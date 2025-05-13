from datetime import datetime, timedelta, timezone
from typing import Iterable
import xarray as xr
import hashlib
import json
from os.path import basename, getsize


def dt_to_dec(date: datetime) -> float:
    """
    Transforms datetime values to year decimal values.
    """
    day_of_year = date.timetuple().tm_yday
    days_in_year = date.replace(month=12, day=31).timetuple().tm_yday
    return date.year + day_of_year / days_in_year


def dec_to_dt(decimal_year: float) -> datetime:
    """Convert a single decimal year to a datetime object."""
    year = int(decimal_year)
    return datetime(year, 1, 1) + timedelta(days=(decimal_year - year) * 365.25)


def create_lines(ds: xr.Dataset, indicator_name: str) -> Iterable[str]:
    """
    Creates list of formatted strings for each indicator time step
    """
    lines = []
    for time in ds["time"]:
        time_slice = ds.sel(time=time)
        indicator_value = time_slice[indicator_name].values
        if indicator_name == "gmsl":
            smoothed_gmsl = time_slice["smoothed_gmsl"].values
            lines.append(f"{time:<12.7f} {indicator_value:>12f} {smoothed_gmsl:>12f}\n")
        else:
            lines.append(f"{time:<12.7f} {indicator_value:>12f}\n")
    return lines


def generate_txt(ds: xr.Dataset, indicator_name: str) -> str:
    filename = f"NASA_SSH_{indicator_name.upper()}_INDICATOR.txt"

    lines = create_lines(ds, indicator_name)

    with open(f"ref_files/txt_templates/{filename}", "r") as template:
        with open(f"/tmp/{filename}", "w") as f:
            template_header = template.readlines()
            template_header = [
                hdr.replace(
                    "PLACEHOLDER_CREATION_DATE", datetime.now().date().isoformat()
                )
                for hdr in template_header
            ]
            f.writelines(template_header)
            f.write("\n")
            f.writelines(lines)

    return filename


def generate_mp(start: int, end: int, filepath: str, shortname: str) -> str:
    # Get times in nanoseconds since epoch
    today = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    production_epoch = int(today.timestamp() * 1000)

    # Get md5 checksum of file at filepath
    with open(filepath, "rb") as f:
        checksum = hashlib.md5(f.read()).hexdigest()

    mp = {
        "granuleUR": basename(filepath),
        "localVersion": 1,
        "boundingBox": {
            "SouthernLatitude": -66.0,
            "NorthernLatitude": 66.0,
            "EasternLongitude": 180.0,
            "WesternLongitude": -180.0,
        },
        "productionDateTime": production_epoch,
        "beginningTime": start,
        "endingTime": end,
        "dataFormat": "ASCII",
        "dataSize": getsize(filepath),
        "collection": {"name": shortname, "version": "1"},
        "dayNightFlag": "UNSPECIFIED",
        "checksum": checksum,
    }

    mp_path = filepath.replace(".txt", ".mp")
    with open(mp_path, "w") as mpfile:
        json.dump(mp, mpfile)
    return mp_path
