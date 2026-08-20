import json
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime

import netCDF4 as nc
import numpy as np
import pandas as pd
from finalization.config.source_config import get_source_config

from utilities.aws_utils import aws_manager
from utilities.pipeline_layout import (
    bad_pass_key,
    daily_file_filename,
    daily_file_key,
    s3_uri,
)
from utilities.provenance import append_to_nc, read_from_nc


@dataclass
class FinalizerResult:
    """What `Finalizer.process` produced for one date: the bucket-relative P3 key it
    wrote and the file's `processing_history` lineage read back for the Job outcome."""

    key: str
    processing_history: list[dict]


class Finalizer:
    def __init__(self, processing_date: date, source: str, bucket: str):
        self.processing_date: date = processing_date
        self.source: str = source
        self.config = get_source_config(source)
        self._validate_date()
        self.bad_pass_df: pd.DataFrame = self._load_bad_passes(bucket)

    def _validate_date(self):
        if self.processing_date < self.config.start_date:
            logging.warning(
                f"Processing date {self.processing_date} is before "
                f"{self.source} start date {self.config.start_date}"
            )
        if self.config.end_date and self.processing_date > self.config.end_date:
            logging.warning(
                f"Processing date {self.processing_date} is after "
                f"{self.source} end date {self.config.end_date}"
            )

    def _load_bad_passes(self, bucket: str) -> pd.DataFrame:
        s3_key = s3_uri(bucket, bad_pass_key(self.source, self.processing_date))
        if not aws_manager.fs.exists(s3_key):
            return pd.DataFrame(columns=["cycle", "pass"])

        logging.info(f"Bad pass file found at {s3_key}")
        with aws_manager.fs.open(s3_key, "r") as f:
            data = json.loads(f.read())

        bad_passes = data.get("bad_passes", [])
        if not bad_passes:
            return pd.DataFrame(columns=["cycle", "pass"])

        df = pd.DataFrame(bad_passes)
        df = df.rename(columns={"pass_num": "pass"})
        return df

    def get_daily_file(self, path) -> str:
        if aws_manager.fs.exists(path):
            local_path = os.path.join("/tmp", os.path.basename(path))
            aws_manager.fs.get(path, local_path)
            return local_path
        raise FileNotFoundError(f"{path} not found")

    def upload_df(self, local_path: str, dst_path: str):
        aws_manager.fs.upload(local_path, dst_path)

    def _build_filename(self) -> str:
        return daily_file_filename(self.config, self.processing_date)

    def _build_src_path(self, bucket: str) -> str:
        return s3_uri(bucket, daily_file_key(self.config, self.processing_date, "p2"))

    def _build_dst_path(self, bucket: str) -> str:
        return s3_uri(bucket, daily_file_key(self.config, self.processing_date, "p3"))

    def process(self, bucket):
        filename = self._build_filename()
        logging.info(f"Processing {filename}")
        src_s3_path = self._build_src_path(bucket)

        try:
            local_filepath = self.get_daily_file(src_s3_path)
        except Exception as e:
            logging.info(e)

        ds = nc.Dataset(local_filepath, "r+")

        pf = self.config.pass_flag
        ds.flagged_passes = "N/A"
        ds.pass_flag_notes = (
            "passes are flagged, with nasa_flag set to 1 whenever a pass contains differences that are "
            "too large relative to self crossovers, computed using data from a 20-day window.  To be "
            "flagged, there must be at least pass_flag_mean_num crossover points for a pass and the "
            "absolute value of its mean crossover difference is larger than pass_flag_mean_threshold "
            "(meters), or when it has at least pass_flag_rms_num crossover points with RMS larger than "
            "pass_flag_rms_threshold (meters). Passes that have been flagged are stored in the "
            "flagged_passes attribute as comma separated cycle/pass"
        )
        ds.pass_flag_mean_num = pf.mean_num
        ds.pass_flag_rms_num = pf.rms_num
        ds.pass_flag_mean_threshold = pf.mean_threshold
        ds.pass_flag_rms_threshold = pf.rms_threshold

        if not self.bad_pass_df.empty:
            ds = apply_bad_pass(ds, self.bad_pass_df)

        ds.product_generation_step = "3"
        ds.history = datetime.now().strftime("Created on %Y-%m-%dT%H:%M:%S")

        # Remove any previously applied offset
        if self.config.offset != 0.0:
            try:
                if "absolute_offset_applied" in ds.ncattrs():
                    prev_offset = float(ds.absolute_offset_applied)
                    if prev_offset != 0.0:
                        ds.variables["ssha"][:] = ds.variables["ssha"][:] - prev_offset
                        ds.variables["ssha_smoothed"][:] = (
                            ds.variables["ssha_smoothed"][:] - prev_offset
                        )
            except AttributeError as e:
                logging.exception(f"Error finalizing {filename}: {e}")

            ds.variables["ssha"][:] = ds.variables["ssha"][:] + self.config.offset
            ds.variables["ssha_smoothed"][:] = (
                ds.variables["ssha_smoothed"][:] + self.config.offset
            )

        ds.absolute_offset_applied = self.config.offset

        # Remove the constant high-lat-vs-reference offset from the absolute SSH
        # level. OER left this in place (it fits only orbit error); the finalizer
        # ties ssha to the reference datum. Kept separate from `offset` — the two
        # are distinct quantities (manual datum tie vs measured crossover median)
        # and each carries its own provenance attribute. No-op for reference
        # sources (intermission_bias defaults to 0.0).
        bias = self.config.intermission_bias
        if bias != 0.0:
            try:
                if "intermission_bias_applied" in ds.ncattrs():
                    prev_bias = float(ds.intermission_bias_applied)
                    if prev_bias != 0.0:
                        ds.variables["ssha"][:] = ds.variables["ssha"][:] + prev_bias
                        ds.variables["ssha_smoothed"][:] = (
                            ds.variables["ssha_smoothed"][:] + prev_bias
                        )
            except AttributeError as e:
                logging.exception(f"Error finalizing {filename}: {e}")

            ds.variables["ssha"][:] = ds.variables["ssha"][:] - bias
            ds.variables["ssha_smoothed"][:] = (
                ds.variables["ssha_smoothed"][:] - bias
            )

        ds.intermission_bias_applied = bias

        dst_s3_path = self._build_dst_path(bucket)

        ds.granule_id = filename

        append_to_nc(
            ds,
            stage="finalizer",
            generation_step=3,
            bad_passes_applied=not self.bad_pass_df.empty,
            absolute_offset_applied=self.config.offset,
            intermission_bias_applied=bias,
            bad_pass_source=bad_pass_key(self.source, self.processing_date),
        )
        processing_history = read_from_nc(ds)

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
            raise
        logging.info(f"Processing {filename} complete. ")

        return FinalizerResult(
            key=daily_file_key(self.config, self.processing_date, "p3"),
            processing_history=processing_history,
        )


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
    ds.flagged_passes = ", ".join(
        df[["cycle", "pass"]].apply(lambda x: "{}/{}".format(*x), axis=1)
    )

    # Reapply nasa_flag to ssha_smoothed
    ssha_smoothed = ds.variables["ssha_smoothed"][:]
    nasa_flag = ds.variables["nasa_flag"][:]
    ssha_smoothed[nasa_flag == 1] = np.nan
    ds.variables["ssha_smoothed"][:] = ssha_smoothed

    return ds
