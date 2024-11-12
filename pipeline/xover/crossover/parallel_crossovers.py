from dataclasses import asdict, dataclass, fields
from io import TextIOWrapper
import re
from typing import Iterable, Tuple
import numpy as np
import xarray as xr
import os
import logging
from datetime import datetime, UTC

from crossover.xover_ssh import xover_ssh
from utilities.aws_utils import aws_manager


EPOCH: np.datetime64 = np.datetime64("1990-01-01T00:00:00.000000")
WINDOW_SIZE: int = 10
WINDOW_PADDING: int = 2
CYCLE_LENGTH: float = 9.9156
ZERO_DIFF: np.timedelta64 = np.timedelta64(0, "ns")
MAX_DIFF: np.timedelta64 = np.timedelta64(int(CYCLE_LENGTH * 86400000000000), "ns")


@dataclass
class CrossoverData:
    time1: Iterable[np.datetime64]
    time2: Iterable[np.datetime64]
    lon: Iterable[float]
    lat: Iterable[float]
    ssh1: Iterable[float]
    ssh2: Iterable[float]
    cycle1: Iterable[int]
    pass1: Iterable[int]
    cycle2: Iterable[int]
    pass2: Iterable[int]

    @classmethod
    def init(cls) -> "CrossoverData":
        return cls(
            time1=[],
            time2=[],
            lon=[],
            lat=[],
            ssh1=[],
            ssh2=[],
            cycle1=[],
            pass1=[],
            cycle2=[],
            pass2=[],
        )

    def to_numpy(self):
        for field in fields(self):
            value = getattr(self, field.name)
            setattr(self, field.name, np.array(value))

    def filter_and_sort(self, next_day: np.datetime64):
        self.to_numpy()  # Convert lists to numpy arrays
        mask = self.time1 < next_day
        for field in fields(self):
            value = getattr(self, field.name)
            setattr(self, field.name, value[mask])

        sorted_indices = np.argsort(self.time1)
        for field in fields(self):
            value = getattr(self, field.name)
            setattr(self, field.name, value[sorted_indices])


class Crossover:
    time: np.ndarray
    longitude: np.ndarray
    latitude: np.ndarray
    ssh: np.ndarray
    trackids: np.ndarray
    unique_trackids: np.ndarray
    starts: np.ndarray

    def __init__(self, day: np.datetime64, source: str, df_version: str):
        self.day: np.datetime64 = day
        self.next_day: np.datetime64 = self.day + np.timedelta64(1, "D")
        self.source: str = source
        self.df_version: str = df_version
        self.window_start: np.datetime64 = day
        self.window_end: np.datetime64 = day + np.timedelta64(
            WINDOW_SIZE + WINDOW_PADDING, "D"
        )

    def _valid_date(self, filepath: str) -> bool:
        date = self._date_from_filename(os.path.basename(filepath))
        return self.window_start <= date <= self.window_end

    def stream_files(self) -> Iterable[TextIOWrapper]:
        start_year = str(self.window_start.astype("datetime64[Y]"))
        end_year = str(self.window_end.astype("datetime64[Y]"))

        all_keys = []

        for year in list({start_year, end_year}):
            glob_pattern = os.path.join(
                "s3://example-bucket/daily_files",
                self.df_version,
                self.source,
                year,
                "*.nc",
            )
            s3_keys = aws_manager.fs.glob(glob_pattern)
            for fp in s3_keys:
                if self._valid_date(fp):
                    all_keys.append(fp)

        streams = []
        for key in all_keys:
            if aws_manager.key_exists(key):
                streams.append(aws_manager.stream_obj(key))
            else:
                logging.warning(f"Unable to stream {key} as it does not exist")
        return streams

    def extract_and_set_data(self):
        window_ds = xr.open_mfdataset(
            self.streams,
            engine="h5netcdf",
            concat_dim="time",
            chunks={},
            parallel=True,
            combine="nested",
        )
        window_ds = window_ds.dropna("time", subset=["ssha_smoothed"]).sortby("time")

        self.time = window_ds["time"].values
        self.longitude = window_ds["longitude"].values.astype("float64")
        self.latitude = window_ds["latitude"].values.astype("float64")
        self.ssh = window_ds["ssha_smoothed"].values.astype("float64")

        self.trackids = (
            window_ds["cycle"].values.astype("int32") * 10000 + window_ds["pass"].values
        )
        self.unique_trackids = np.unique(self.trackids)
        self.starts = np.array(
            [
                np.min(self.time[self.trackids == track_id])
                for track_id in self.unique_trackids
            ],
            dtype="datetime64[ns]",
        )

    @staticmethod
    def _date_from_filename(filename: str) -> np.datetime64:
        match = re.compile(r"\d{8}").search(filename)
        date_str = match.group()
        date_obj = datetime.strptime(date_str, "%Y%m%d")
        return np.datetime64(date_obj)

    def search_day_for_crossovers(self):
        logging.info(f"Processing {np.datetime_as_string(self.day, unit='D')}")

        # Loop through unique track ids that start on day of interest
        for i, track_1 in enumerate(self.unique_trackids[self.starts < self.next_day]):
            time_1, lonlat_1, ssh_1 = self.get_track_data(track_1)
            if time_1.size <= 1:
                continue

            # Determine possible crossover tracks
            different_cycles = np.abs(track_1 - self.unique_trackids) > 1
            opposite_passes = (track_1 % 2) != (self.unique_trackids % 2)
            starts_diff = self.starts - self.starts[i]
            within_window = (starts_diff <= MAX_DIFF) & (starts_diff > ZERO_DIFF)
            possible_tracks = self.unique_trackids[
                different_cycles & opposite_passes & within_window
            ]

            for track_2 in possible_tracks:
                time_2, lonlat_2, ssh_2 = self.get_track_data(track_2)

                if time_2.size <= 1:
                    continue

                xcoords, xssh, xtime = xover_ssh(
                    lonlat_1, lonlat_2, ssh_1, ssh_2, time_1, time_2
                )

                if np.size(xcoords) == 0:
                    continue

                self.crossover_data.ssh1.append(xssh[0])
                self.crossover_data.ssh2.append(xssh[1])
                self.crossover_data.time1.append(
                    EPOCH + np.timedelta64(int(xtime[0]), "ns")
                )
                self.crossover_data.time2.append(
                    EPOCH + np.timedelta64(int(xtime[1]), "ns")
                )
                self.crossover_data.lon.append(xcoords[0])
                self.crossover_data.lat.append(xcoords[1])
                self.crossover_data.cycle1.append(track_1 // 10000)
                self.crossover_data.cycle2.append(track_2 // 10000)
                self.crossover_data.pass1.append(track_1 % 10000)
                self.crossover_data.pass2.append(track_2 % 10000)

        if len(self.crossover_data.time1) > 0:
            self.crossover_data.filter_and_sort(self.next_day)

    def get_track_data(
        self, track_id: int
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Masks time, lonlat, and ssh arrays by track_id
        """
        track_mask = self.trackids == track_id
        masked_time = (
            (self.time[track_mask] - EPOCH).astype("timedelta64[ns]").astype("float64")
        )
        masked_lonlat = np.column_stack(
            (self.longitude[track_mask], self.latitude[track_mask])
        )
        masked_ssh = self.ssh[track_mask]
        return masked_time, masked_lonlat, masked_ssh

    def create_dataset(self) -> xr.Dataset:
        """
        Creates xarray Dataset object from crossover data
        """
        ds = xr.Dataset(
            data_vars={
                k: ("time1", v)
                for k, v in asdict(self.crossover_data).items()
                if k != "time1"
            },
            coords={"time1": ("time1", self.crossover_data.time1)},
            attrs={
                "title": f"{self.source} self-crossovers {self.day}",
                "window_length": f"{(self.window_end - self.window_start).astype('int32')} days (nominal: {WINDOW_SIZE} days + {WINDOW_PADDING} days padding)",
                "created_on": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S"),
                "input_product_generation_steps": self.df_version[-1],
                "satellite_names": self.source,
            },
        )
        ds["time2"].attrs = {"long_name": "Time of crossover in later pass"}
        ds["lon"].attrs = {"units": "degrees", "long_name": "Crossover longitude"}
        ds["lat"].attrs = {"units": "degrees", "long_name": "Crossover latitude"}
        ds["ssh1"].attrs = {
            "units": "m",
            "long_name": "SSH at crossover in earlier pass",
        }
        ds["ssh2"].attrs = {"units": "m", "long_name": "SSH at crossover in later pass"}
        ds["cycle1"].attrs = {
            "units": "N/A",
            "long_name": "Cycle number of earlier pass",
        }
        ds["cycle2"].attrs = {"units": "N/A", "long_name": "Cycle number of later pass"}
        ds["pass1"].attrs = {"units": "N/A", "long_name": "Pass number of earlier pass"}
        ds["pass2"].attrs = {"units": "N/A", "long_name": "Pass number of later pass"}

        ds["time1"].encoding["units"] = f"seconds since {EPOCH}"
        ds["time2"].encoding["units"] = f"seconds since {EPOCH}"
        return ds

    def save_to_netcdf(self, ds: xr.Dataset) -> str:
        """
        Saves xarray Dataset object as local netcdf and returns local path
        """
        filename = f"xovers_{self.source}-{np.datetime_as_string(self.day)}.nc"
        local_output_path = os.path.join("/tmp", filename)
        logging.info(f"Saving netcdf to {local_output_path}")
        ds.to_netcdf(local_output_path, engine="h5netcdf")
        return local_output_path

    def upload_xover(self, local_path):
        """
        Uploads crossover netCDF to bucket
        """
        filename = os.path.basename(local_path)
        s3_output_path = os.path.join(
            "s3://example-bucket/crossovers",
            self.df_version,
            self.source,
            np.datetime_as_string(self.day, unit="Y"),
            filename,
        )
        aws_manager.upload_obj(local_path, s3_output_path)

    def run(self):
        logging.info(f"Looking for {self.source} {self.day} self-crossovers...")
        """
        1. Stream files in window
        2. Open stream via xarray
        3. Initialize arrays
        4. Big processing loop to find xovers
        5. Save and upload netcdf
        
        
        What's missing: handling daily files with no data or entire windows with no data. 
        Need to make empty crossover.
        """
        # Initialize empty data class
        self.crossover_data = CrossoverData.init()

        self.streams = self.stream_files()
        if len(self.streams) > 0:
            self.extract_and_set_data()
            self.search_day_for_crossovers()

        ds = self.create_dataset()
        local_path = self.save_to_netcdf(ds)
        self.upload_xover(local_path)
        logging.info(f"Processing {self.source} {self.day} complete")
