from dataclasses import asdict, dataclass, fields
from io import TextIOWrapper
from typing import Iterable, Tuple
import numpy as np
import xarray as xr
import os
import logging
from datetime import datetime, UTC

from crossover.xover_ssh import xover_ssh
from crossover.config.source_config import get_source_config
from utilities.aws_utils import aws_manager
from utilities.source_registry import daily_filename_prefix


EPOCH: np.datetime64 = np.datetime64("1990-01-01T00:00:00.000000")
ZERO_DIFF: np.timedelta64 = np.timedelta64(0, "ns")


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

    def append_result(self, xcoords, xssh, xtime, track_1: int, track_2: int):
        self.time1.append(EPOCH + np.timedelta64(int(xtime[0]), "ns"))
        self.time2.append(EPOCH + np.timedelta64(int(xtime[1]), "ns"))
        self.lon.append(xcoords[0])
        self.lat.append(xcoords[1])
        self.ssh1.append(xssh[0])
        self.ssh2.append(xssh[1])
        self.cycle1.append(track_1 // 10000)
        self.pass1.append(track_1 % 10000)
        self.cycle2.append(track_2 // 10000)
        self.pass2.append(track_2 % 10000)

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
        self.config = get_source_config(source)
        self.max_diff: np.timedelta64 = np.timedelta64(int(self.config.cycle_length * 86400000000000), "ns")
        self.window_start: np.datetime64 = day
        self.window_end: np.datetime64 = day + np.timedelta64(self.config.window_size + self.config.window_padding, "D")

    def stream_files(self, bucket: str) -> Iterable[TextIOWrapper]:
        streams = []
        date = self.window_start
        prefix = daily_filename_prefix(self.source)
        while date <= self.window_end:
            date_str = np.datetime_as_string(date, unit="D").replace("-", "")
            year = np.datetime_as_string(date, unit="Y")
            filename = f"{prefix}_{date_str}.nc"
            key = f"s3://{bucket}/daily_files/{self.df_version}/{self.source}/{year}/{filename}"

            if aws_manager.key_exists(key):
                streams.append(aws_manager.stream_obj(key))
            else:
                logging.info(f"No daily file for {date_str}, skipping")

            date += np.timedelta64(1, "D")

        return streams

    def extract_and_set_data(self):
        time_chunks = []
        lon_chunks = []
        lat_chunks = []
        ssh_chunks = []
        cycle_chunks = []
        pass_chunks = []

        for stream in self.streams:
            ds = xr.open_dataset(
                stream,
                engine="h5netcdf",
                drop_variables=["basin_flag", "median_filter_flag", "nasa_flag", "source_flag", "ssha", "dac"],
            )
            ssh = ds["ssha_smoothed"].values
            valid = ~np.isnan(ssh)
            time_chunks.append(ds["time"].values[valid])
            lon_chunks.append(ds["longitude"].values[valid])
            lat_chunks.append(ds["latitude"].values[valid])
            ssh_chunks.append(ssh[valid])
            cycle_chunks.append(ds["cycle"].values[valid])
            pass_chunks.append(ds["pass"].values[valid])
            ds.close()

        self.time = np.concatenate(time_chunks)
        self.longitude = np.concatenate(lon_chunks).astype(np.float64)
        self.latitude = np.concatenate(lat_chunks).astype(np.float64)
        self.ssh = np.concatenate(ssh_chunks).astype(np.float64)
        self.trackids = np.concatenate(cycle_chunks).astype("int32") * 10000 + np.concatenate(pass_chunks)

        sort_idx = np.lexsort((self.time.view("i8"), self.trackids))
        sorted_trackids = self.trackids[sort_idx]
        sorted_time = self.time[sort_idx]

        boundaries = np.diff(sorted_trackids) != 0
        start_indices = np.concatenate([[0], np.where(boundaries)[0] + 1])

        self.unique_trackids = sorted_trackids[start_indices]
        self.starts = sorted_time[start_indices]

        self._build_track_index()
        self._track_data_cache = {}

    def _build_track_index(self):
        self.track_index = {}
        sort_idx = np.argsort(self.trackids)
        sorted_trackids = self.trackids[sort_idx]

        changes = np.where(np.diff(sorted_trackids) != 0)[0] + 1
        starts = np.concatenate([[0], changes])
        ends = np.concatenate([changes, [len(sorted_trackids)]])

        for i, tid in enumerate(sorted_trackids[starts]):
            group_idx = sort_idx[starts[i] : ends[i]]
            # Sort within each group by time so get_track_data returns
            # time-ordered data and xover_ssh's internal argsorts are near-free.
            time_order = np.argsort(self.time[group_idx])
            self.track_index[tid] = group_idx[time_order]

    def _today_tracks(self):
        """Yield (track_id, start_time) for tracks starting on the processing day."""
        mask = self.starts < self.next_day
        for tid, start in zip(self.unique_trackids[mask], self.starts[mask]):
            yield tid, start

    def _candidate_tracks(self, track_1: int, track_1_start: np.datetime64) -> np.ndarray:
        """Return track IDs that could cross track_1 (different cycle, opposite direction, within window)."""
        different_cycles = np.abs(track_1 - self.unique_trackids) > 1
        opposite_passes = (track_1 % 2) != (self.unique_trackids % 2)
        starts_diff = self.starts - track_1_start
        within_window = (starts_diff <= self.max_diff) & (starts_diff > ZERO_DIFF)
        return self.unique_trackids[different_cycles & opposite_passes & within_window]

    def search_day_for_crossovers(self):
        logging.info(f"Processing {np.datetime_as_string(self.day, unit='D')}")

        for track_1, track_1_start in self._today_tracks():
            time_1, lonlat_1, ssh_1 = self.get_track_data(track_1)
            if time_1.size <= 1:
                continue

            for track_2 in self._candidate_tracks(track_1, track_1_start):
                time_2, lonlat_2, ssh_2 = self.get_track_data(track_2)
                if time_2.size <= 1:
                    continue

                xcoords, xssh, xtime = xover_ssh(lonlat_1, lonlat_2, ssh_1, ssh_2, time_1, time_2)
                if np.size(xcoords) == 0:
                    continue

                self.crossover_data.append_result(xcoords, xssh, xtime, track_1, track_2)

        if len(self.crossover_data.time1) > 0:
            self.crossover_data.filter_and_sort(self.next_day)

    def get_track_data(self, track_id: int) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Returns (time, lonlat, ssh) arrays for a track, cached after first call.
        """
        cached = self._track_data_cache.get(track_id)
        if cached is not None:
            return cached
        idx = self.track_index[track_id]
        masked_time = (self.time[idx] - EPOCH).astype("timedelta64[ns]").astype("float64")
        masked_lonlat = np.column_stack((self.longitude[idx], self.latitude[idx]))
        masked_ssh = self.ssh[idx]
        result = (masked_time, masked_lonlat, masked_ssh)
        self._track_data_cache[track_id] = result
        return result

    def create_dataset(self) -> xr.Dataset:
        """
        Creates xarray Dataset object from crossover data
        """
        ds = xr.Dataset(
            data_vars={k: ("time1", v) for k, v in asdict(self.crossover_data).items() if k != "time1"},
            coords={"time1": ("time1", self.crossover_data.time1)},
            attrs={
                "title": f"{self.source} self-crossovers {self.day}",
                "window_length": f"{(self.window_end - self.window_start).astype('int32')} days (nominal: {self.config.window_size} days + {self.config.window_padding} days padding)",
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

    def save_to_netcdf(self, ds: xr.Dataset, out_dir: str = "/tmp") -> str:
        """
        Saves xarray Dataset object as local netcdf and returns local path
        """
        filename = f"xovers_{self.source}-{np.datetime_as_string(self.day)}.nc"
        local_output_path = os.path.join(out_dir, filename)
        logging.info(f"Saving netcdf to {local_output_path}")
        ds.to_netcdf(local_output_path, engine="h5netcdf")
        return local_output_path

    def upload_xover(self, local_path: str, bucket: str):
        """
        Uploads crossover netCDF to bucket
        """
        filename = os.path.basename(local_path)
        s3_output_path = os.path.join(
            f"s3://{bucket}/crossovers",
            self.df_version,
            self.source,
            np.datetime_as_string(self.day, unit="Y"),
            filename,
        )
        aws_manager.upload_obj(local_path, s3_output_path)

    def run(self, bucket):
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

        self.streams = self.stream_files(bucket)
        if len(self.streams) > 0:
            self.extract_and_set_data()
            self.search_day_for_crossovers()

        ds = self.create_dataset()
        local_path = self.save_to_netcdf(ds)
        self.upload_xover(local_path, bucket)
        logging.info(f"Processing {self.source} {self.day} complete")
