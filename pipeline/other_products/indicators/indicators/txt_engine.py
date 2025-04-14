from datetime import datetime
from typing import Iterable
import xarray as xr


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


def generate_txt(ds: xr.Dataset, indicator_name: str):
    filename = f"NASA_SSH_{indicator_name.upper()}_INDICATOR.txt"
    
    lines = create_lines(ds, indicator_name)

    with open(f"ref_files/txt_templates/{filename}", "r") as template:
        with open(f"/tmp/{filename}.txt", "w") as f:
            template_header = template.readlines()
            template_header = [
                hdr.replace("PLACEHOLDER_CREATION_DATE", datetime.now().date().isoformat()) for hdr in template_header
            ]
            f.writelines(template_header)
            f.write("\n")
            f.writelines(lines)
