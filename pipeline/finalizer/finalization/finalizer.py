import logging
import os
from datetime import date, datetime

import numpy as np
import pandas as pd
import netCDF4 as nc

from utilities.aws_utils import aws_manager

GSFC_START = date(1992, 10, 13)
S6_START = date(2024, 1, 20)

S6_ABSOLUTE_OFFSET = 0.0232  # Offset from GSFC in meters


class Finalizer:
    def __init__(self, processing_date: date):
        self.bad_pass_df: pd.DataFrame = self._load_bad_passes()
        self.processing_date: date = processing_date
        self.source: str = "GSFC" if processing_date < S6_START else "S6"

    def _load_bad_passes(self) -> pd.DataFrame:
        stream = aws_manager.fs.open("s3://example-bucket/aux_files/bad_pass_list.csv")
        return pd.read_csv(stream)

    def get_daily_file(self, path) -> str:
        if aws_manager.fs.exists(path):
            local_path = os.path.join("/tmp", os.path.basename(path))
            aws_manager.fs.get(path, local_path)
            return local_path
        raise FileNotFoundError(f"{path} not found")

    def upload_df(self, local_path: str, dst_path: str):
        aws_manager.fs.upload(local_path, dst_path)

    def process(self):
        year = str(self.processing_date.year)
        filename = f'{self.source}-SSH_alt_ref_at_v1_{self.processing_date.strftime("%Y%m%d")}.nc'
        logging.info(f"Processing {filename}")
        src_s3_path = os.path.join("s3://example-bucket/daily_files/p2", self.source, year, filename)

        try:
            local_filepath = self.get_daily_file(src_s3_path)
        except Exception as e:
            logging.info(e)

        ds = nc.Dataset(local_filepath, "r+")

        ds.flagged_passes = "N/A"
        ds.pass_flag_notes = (
            "passes are flagged, with nasa_flag set to 1 whenever a pass contains differences that are too large relative to self crossovers, "
            "computed using data from a 20-day window.  To be flagged, there must be at least pass_flag_mean_num crossover points for a pass "
            "and the absolute value of its mean crossover difference is larger than pass_flag_mean_threshold (meters), or when it has at least pass_flag_rms_num "
            "crossover points with RMS larger than pass_flag_rms_threshold (meters). Passes that have been flagged are stored in the flagged_passes attribute "
            "as comma separated cycle/pass"
        )
        ds.pass_flag_mean_num = 15.0
        ds.pass_flag_rms_num = 25.0
        ds.pass_flag_mean_threshold = 0.1
        ds.pass_flag_rms_threshold = 0.27

        bad_pass_slice = self.bad_pass_df[
            (self.bad_pass_df["source"] == self.source) & (self.bad_pass_df["date"] == str(date))
        ]
        if not bad_pass_slice.empty:
            ds = apply_bad_pass(ds, bad_pass_slice)

        ds.product_generation_step = "3"
        ds.history = datetime.now().strftime("Created on %Y-%m-%dT%H:%M:%S")

        if self.source == "S6":
            # Remove any previously applied offset
            try:
                if "absolute_offset_applied" in ds.ncattrs():
                    ds.variables["ssha"][:] = ds.variables["ssha"][:] - float(ds.absolute_offset_applied)
                    ds.variables["ssha_smoothed"][:] = ds.variables["ssha_smoothed"][:] - float(
                        ds.absolute_offset_applied
                    )
            except AttributeError as e:
                logging.exception(f"Error finalizing {filename}: {e}")
                pass

            ds.variables["ssha"][:] = ds.variables["ssha"][:] + S6_ABSOLUTE_OFFSET
            ds.variables["ssha_smoothed"][:] = ds.variables["ssha_smoothed"][:] + S6_ABSOLUTE_OFFSET

            ds.absolute_offset_applied = S6_ABSOLUTE_OFFSET
        elif self.source == "GSFC":
            ds.absolute_offset_applied = 0

        dst_filename = filename.replace(self.source, "NASA")
        dst_s3_path = os.path.join("s3://example-bucket/daily_files/p3", year, dst_filename)

        ds.granule_id = dst_filename

        # Sort the global attributes by deleting / replacing
        sorted_attributes = sorted(ds.ncattrs(), key=lambda x: x.lower())
        attribute_data = {attr: ds.getncattr(attr) for attr in sorted_attributes}

        for attr in ds.ncattrs():
            ds.delncattr(attr)

        for attr, value in attribute_data.items():
            ds.setncattr(attr, value)

        ds.close()

        try:
            self.upload_df(local_filepath, dst_s3_path)
            os.remove(local_filepath)
        except Exception as e:
            logging.exception(e)
            return
        logging.info(f"Processing {filename} complete. ")


def apply_bad_pass(ds: nc.Dataset, df: pd.DataFrame) -> nc.Dataset:
    """
    Set nasa_flag values to 1 where there are identified bad passes
    """
    # Get cycle and pass variables from the dataset
    cycle_var = ds.variables["cycle"][:].astype(int)
    pass_var = ds.variables["pass"][:].astype(int)

    # Convert bad_pass_slice cycles and passes to numpy arrays for comparison
    bad_cycles = df["cycle"].astype(int).to_numpy()
    bad_passes = df["pass"].astype(int).to_numpy()

    # Mask where cycle and pass match those in the bad_pass_slice
    mask = np.isin(cycle_var, bad_cycles) & np.isin(pass_var, bad_passes)

    # Set nasa_flag to 1 where the mask is True
    ds.variables["nasa_flag"][mask] = 1

    # Update the 'flagged_passes' attribute in the dataset
    ds.flagged_passes = ", ".join(df[["cycle", "pass"]].apply(lambda x: "{}/{}".format(*x), axis=1))

    # Reapply nasa_flag to ssha_smoothed
    ssha_smoothed = ds.variables["ssha_smoothed"][:]
    nasa_flag = ds.variables["nasa_flag"][:]
    ssha_smoothed[nasa_flag == 1] = np.nan
    ds.variables["ssha_smoothed"][:] = ssha_smoothed

    return ds
