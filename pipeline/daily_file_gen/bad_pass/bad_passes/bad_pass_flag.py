import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, Iterable, List

import netCDF4 as nc
import numpy as np

from bad_passes.config.source_config import get_source_config
from utilities.aws_utils import aws_manager
from utilities.pipeline_layout import bad_pass_key, crossover_key, s3_uri

# product_type ↔ crossover_type ↔ bad_pass path (documented 1:1; mirrors the OER
# stage's _CROSSOVER_TYPE_BY_PRODUCT_TYPE). A reference mission
# (product_type=reference) crosses against itself → "self" stacking; a
# high_latitude source crosses against the finalized reference mission →
# "reference" fixed-truth (no sign flip).
_CROSSOVER_TYPE_BY_PRODUCT_TYPE = {
    "reference": "self",
    "high_latitude": "reference",
}


class XoverProcessor:
    REF_EPOCH = datetime(1990, 1, 1)
    TRACKID_CYCLE_FACTOR = 10000
    MAX_MEAN = 0.1
    MAX_RMS = 0.27
    N_MEAN = 15
    N_RMS = 25
    TIME_PAD_SECS = 3600
    DAY_SECS = 86400

    def __init__(self, source: str, date: datetime):
        self.source = source
        self.date = date
        self.config = get_source_config(source)
        self.crossover_type = _CROSSOVER_TYPE_BY_PRODUCT_TYPE[self.config.product_type]
        self.windowlen = 10
        self.windowpad = 1
        if self.crossover_type == "reference":
            # Reference xover files are keyed by the high-lat crossover time and
            # are self-contained per day, so a small *centered* window covers the
            # neighboring days needed by the time-pad in identify_bad_passes.
            n = self.config.reference_window_size
            self.window_start = date - timedelta(days=n)
            self.window_end = date + timedelta(days=n)
        else:
            # self xover files "look forward" in time → backward-looking window.
            self.window_start = date - timedelta(days=self.windowlen) - timedelta(days=self.windowpad)
            self.window_end = date + timedelta(days=self.windowpad)

    def get_files(self, bucket: str) -> Iterable[str]:
        window_range = []
        cur_date = self.window_start
        while cur_date <= self.window_end:
            xover_path = s3_uri(bucket, crossover_key(self.source, cur_date, "p2"))
            if aws_manager.key_exists(xover_path):
                window_range.append(xover_path)
            else:
                logging.info(f"Key {xover_path} does not exist")
            cur_date = cur_date + timedelta(days=1)
        logging.info(f"Found {len(window_range)} within window")
        return window_range

    def open_file(self, file):
        if file.startswith("s3://"):
            return aws_manager.fs.open(file, "rb")
        return open(file, "rb")

    def load_all_data(self, files: Iterable[str]):
        logging.info("Loading all data...")

        ref_tstamp = self.REF_EPOCH.timestamp()

        # Accumulate per-file arrays, then concatenate once. Both branches share
        # the file-open loop and the time1 size==0 skip; the branch chosen by
        # self.crossover_type decides which variables are read and how dssh is
        # built (self stacks with a sign flip, reference does not).
        loader = self._load_reference if self.crossover_type == "reference" else self._load_self
        accum: dict[str, list] = {}

        for file in files:
            with self.open_file(file) as f:
                nc_file = nc.Dataset("dummy", memory=f.read())
                try:
                    size = len(nc_file["time1"])
                    if size == 0:
                        continue
                    loader(nc_file, ref_tstamp, accum)
                finally:
                    nc_file.close()

        if not accum.get("ssh1"):
            self.dssh = np.array([])
            self.psec = np.array([])
            self.trackid = np.array([])
            logging.info("Loading data complete (no data)")
            return

        if self.crossover_type == "reference":
            self._build_reference(accum)
        else:
            self._build_self(accum)
        logging.info("Loading data complete")

    @staticmethod
    def _load_self(nc_file, ref_tstamp: float, accum: dict) -> None:
        """Read one self-schema crossover file into the accumulator."""
        accum.setdefault("cycle1", []).append(nc_file["cycle1"][:])
        accum.setdefault("cycle2", []).append(nc_file["cycle2"][:])
        accum.setdefault("pass1", []).append(nc_file["pass1"][:])
        accum.setdefault("pass2", []).append(nc_file["pass2"][:])
        accum.setdefault("ssh1", []).append(nc_file["ssh1"][:])
        accum.setdefault("ssh2", []).append(nc_file["ssh2"][:])
        accum.setdefault("psec1", []).append(nc_file["time1"][:] + ref_tstamp)
        accum.setdefault("psec2", []).append(nc_file["time2"][:] + ref_tstamp)

    @staticmethod
    def _load_reference(nc_file, ref_tstamp: float, accum: dict) -> None:
        """Read one reference-schema crossover file into the accumulator.

        The reference schema has no ``time2``/``cycle2``; only the high-lat side
        vars and the interpolated reference ``ssh2`` are read here.
        """
        accum.setdefault("cycle1", []).append(nc_file["cycle1"][:])
        accum.setdefault("pass1", []).append(nc_file["pass1"][:])
        accum.setdefault("ssh1", []).append(nc_file["ssh1"][:])
        accum.setdefault("ssh2", []).append(nc_file["ssh2"][:])
        accum.setdefault("psec1", []).append(nc_file["time1"][:] + ref_tstamp)

    def _build_self(self, accum: dict) -> None:
        """Self path: each pair contributes twice with opposite sign (shared
        orbit error), keyed by both trackids — reproduces the original exactly."""
        cycle1 = np.concatenate(accum["cycle1"])
        cycle2 = np.concatenate(accum["cycle2"])
        pass1 = np.concatenate(accum["pass1"])
        pass2 = np.concatenate(accum["pass2"])
        psec1 = np.concatenate(accum["psec1"])
        psec2 = np.concatenate(accum["psec2"])
        ssh1 = np.concatenate(accum["ssh1"])
        ssh2 = np.concatenate(accum["ssh2"])

        dssh0 = ssh1 - ssh2
        self.dssh = np.concatenate((dssh0, -dssh0))
        self.psec = np.concatenate((psec1, psec2))
        self.trackid = np.concatenate(
            (cycle1 * self.TRACKID_CYCLE_FACTOR + pass1, cycle2 * self.TRACKID_CYCLE_FACTOR + pass2)
        )

    def _build_reference(self, accum: dict) -> None:
        """Reference path: side 2 is the fixed reference-mission truth, not a
        second observation, so there is no sign-flipped stacking. Each crossover
        contributes a single ``dssh = ssh1 - ssh2`` at the high-lat time,
        keyed by the high-lat trackid only (length == n, not 2n)."""
        cycle1 = np.concatenate(accum["cycle1"])
        pass1 = np.concatenate(accum["pass1"])
        ssh1 = np.concatenate(accum["ssh1"])
        ssh2 = np.concatenate(accum["ssh2"])

        self.dssh = ssh1 - ssh2
        self.psec = np.concatenate(accum["psec1"])
        self.trackid = cycle1 * self.TRACKID_CYCLE_FACTOR + pass1

    def identify_bad_passes(self, currentdate: float) -> List[Dict[str, str]]:
        bad_passes = []
        ii = np.where(
            (self.psec >= currentdate - self.TIME_PAD_SECS)
            & (self.psec <= currentdate + self.DAY_SECS + self.TIME_PAD_SECS)
        )[0]
        tid_list = np.unique(self.trackid[ii])
        min_points = min(self.N_MEAN, self.N_RMS)

        for tid in tid_list:
            jj = np.where(self.trackid == tid)[0]
            if len(jj) >= min_points:
                xmean = np.mean(self.dssh[jj])
                xrms = np.std(self.dssh[jj], ddof=1)
                check_mean = (len(jj) > self.N_MEAN) and (np.abs(xmean) > self.MAX_MEAN)
                check_rms = (len(jj) > self.N_RMS) and (xrms > self.MAX_RMS)
                if check_mean or check_rms:
                    cycle = str(int(tid // self.TRACKID_CYCLE_FACTOR))
                    pass_num = str(int(tid % self.TRACKID_CYCLE_FACTOR))
                    bad_passes.append({"cycle": cycle, "pass_num": pass_num})
        return bad_passes

    def write_results_to_s3(self, results: dict, bucket: str) -> None:
        """Write bad pass results JSON to s3://{bucket}/bad_passes/{source}/{date}.json"""
        source = results["source"]
        date = results["date"]
        s3_key = s3_uri(bucket, bad_pass_key(source, datetime.fromisoformat(date)))
        local_path = f"/tmp/{source}_{date}_bad_passes.json"
        with open(local_path, "w") as f:
            json.dump(results, f)
        aws_manager.upload_obj(local_path, s3_key)
        os.remove(local_path)
        logging.info(f"Wrote bad pass results to {s3_key}")

    def process(self, bucket: str):
        logging.info(f"Finding {self.source} bad passes for {self.date}")
        file_paths = self.get_files(bucket)
        self.load_all_data(file_paths)
        currentdate = datetime.timestamp(self.date)
        # Get list of (cycle, pass_num)
        bad_passes = self.identify_bad_passes(currentdate)
        logging.info(f"Found {len(bad_passes)} {self.source} bad passes for {self.date}")
        formatted_results = {
            "date": self.date.date().isoformat(),
            "source": self.source,
            "bad_passes": bad_passes,
        }
        if bad_passes:
            self.write_results_to_s3(formatted_results, bucket)
        return formatted_results
