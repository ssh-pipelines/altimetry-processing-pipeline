import logging
import os
from datetime import date, datetime

import numpy as np
import pandas as pd
import s3fs
import xarray as xr

GSFC_START = date(1992, 9, 25)
S6_START = date(2022, 3, 29)


class Finalizer:
    def __init__(self, start_date: date = S6_START, end_date: date = date.today()):
        _access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        _secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        _session_token = os.environ.get("AWS_SESSION_TOKEN")

        self.fs: s3fs.S3FileSystem = s3fs.S3FileSystem(
            anon=False, key=_access_key, secret=_secret_key, token=_session_token
        )

        self.bad_pass_df: pd.DataFrame = self._load_bad_passes()
        self.input_dates: dict = self._make_input_dates(start_date, end_date)

    def _load_bad_passes(self) -> pd.DataFrame:
        stream = self.fs.open("s3://example-bucket/aux_files/bad_pass_list.csv")
        return pd.read_csv(stream)

    @staticmethod
    def _make_input_dates(start_date: date, end_date: date) -> dict[np.datetime64, str]:
        """
        Generates a dictionary of dates mapped to their respective sources (GSFC or S6).
        Filters this dictionary to only include keys (dates) within the specified date range.
        """
        GSFC_dates = np.arange(GSFC_START, S6_START, dtype="datetime64[D]")
        S6_dates = np.arange(S6_START, "now", dtype="datetime64[D]")
        total_record = {d: "GSFC" for d in GSFC_dates} | {d: "S6" for d in S6_dates}

        # Filter for provided date range
        filtered_record = {
            k: total_record[k] for k in total_record.keys() & np.arange(start_date, end_date, dtype="datetime64[D]")
        }
        return filtered_record

    def stream_daily_file(self, path) -> xr.Dataset:
        if self.fs.exists(path):
            return self.fs.open(path)
        raise FileNotFoundError(f"{path} not found")

    def upload_df(self, local_path: str, dst_path: str):
        self.fs.upload(local_path, dst_path)

    def process(self):
        for df_date, source in self.input_dates.items():
            year = str(df_date.astype(object).year)
            filename = f'{source}-alt_ssh{str(df_date).replace("-","")}.nc'
            src_s3_path = os.path.join("s3://example-bucket/daily_files/p2", source, year, filename)

            try:
                stream_data = self.stream_daily_file(src_s3_path)
            except FileNotFoundError as e:
                logging.info(e)
                continue

            ds = xr.open_dataset(stream_data)

            ds.attrs["flagged_passes"] = "N/A"
            ds.attrs["pass_flag_notes"] = (
                "passes are flagged, with nasa_flag set to 1 whenever a pass contains differences that are too large relative to self crossovers, "
                "computed using data from a 20-day window.  To be flagged, there must be at least 'pass_flag_mean_num' crossover points for a pass "
                "and the absolute value of its mean crossover difference is larger than 'pass_flag_mean_threshold' (meters), or when it has at least 'pass_flag_rms_num' "
                "crossover points with RMS larger than 'pass_flag_rms_threshold' (meters). Passes that have been flagged are stored in the 'flagged_passes' attribute "
                "as comma separated cycle/pass"
            )
            ds.attrs["pass_flag_mean_num"] = 15
            ds.attrs["pass_flag_rms_num"] = 25
            ds.attrs["pass_flag_mean_threshold"] = 0.1
            ds.attrs["pass_flag_rms_threshold"] = 0.27

            bad_pass_slice = self.bad_pass_df[
                (self.bad_pass_df["source"] == source) & (self.bad_pass_df["date"] == str(date))
            ]
            if not bad_pass_slice.empty:
                ds = apply_bad_pass(ds, bad_pass_slice)

            ds.attrs["product_generation_step"] = "3"
            ds.attrs["history"] = datetime.now().strftime("Created on %Y-%m-%dT%H:%M:%S")

            dst_filename = filename.replace(source, "NASA")
            dst_s3_path = os.path.join("s3://example-bucket/daily_files/p3", year, dst_filename)
            local_path = os.path.join("/tmp", dst_filename)

            encoding = {"time": {"units": "seconds since 1990-01-01 00:00:00", "dtype": "float64"}}
            for var in ds.variables:
                if var not in ["latitude", "longitude", "time", "basin_names_table"]:
                    encoding[var] = {"complevel": 5, "zlib": True}
                elif "lat" in var or "lon" in var:
                    encoding[var] = {"complevel": 5, "zlib": True, "dtype": "float32"}
                elif "basin_names_table" in var:
                    encoding[var] = {"complevel": 5, "zlib": True, "char_dim_name": "basin_name_len", "dtype": "|S33"}

                if any(x in var for x in ["source_flag", "nasa_flag", "median_filter_flag"]):
                    encoding[var]["dtype"] = "int8"
                    encoding[var]["_FillValue"] = np.iinfo(np.int8).max
                if any(x in var for x in ["basin_flag", "pass", "cycle"]):
                    encoding[var]["dtype"] = "int32"
                    encoding[var]["_FillValue"] = np.iinfo(np.int32).max
                if any(x in var for x in ["ssh", "dac"]):
                    encoding[var]["dtype"] = "float64"
                    encoding[var]["_FillValue"] = np.finfo(np.float64).max

            ds.to_netcdf(local_path, encoding=encoding)
            self.upload_df(local_path, dst_s3_path)


def apply_bad_pass(ds: xr.Dataset, bad_pass_slice: pd.DataFrame) -> xr.Dataset:
    """
    Set nasa_flag values to 1 where there are identified bad passes
    """

    ds["nasa_flag"] = ds["nasa_flag"].where(
        ~(
            (ds["cycle"].isin(bad_pass_slice["cycle"].astype(int)))
            & (ds["pass"].isin(bad_pass_slice["pass"].astype(int)))
        ),
        1,
    )

    ds.attrs["flagged_passes"] = ", ".join(bad_pass_slice[["cycle", "pass"]].apply(lambda x: "{}/{}".format(*x), 1))
    return ds