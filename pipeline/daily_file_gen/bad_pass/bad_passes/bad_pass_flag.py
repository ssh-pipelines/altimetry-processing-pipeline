import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, Iterable, List

import netCDF4 as nc
import numpy as np

from utilities.aws_utils import aws_manager


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
        self.windowlen = 10
        self.windowpad = 1
        self.window_start = date - timedelta(days=self.windowlen) - timedelta(days=self.windowpad)
        self.window_end = date + timedelta(days=self.windowpad)

    def get_files(self, bucket: str) -> Iterable[str]:
        window_range = []
        cur_date = self.window_start
        while cur_date <= self.window_end:
            xover_filename = f"xovers_{self.source}-{cur_date.strftime('%Y-%m-%d')}.nc"
            xover_path = f"s3://{bucket}/crossovers/p2/{self.source}/{cur_date.year}/{xover_filename}"
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

        all_cycle1, all_cycle2 = [], []
        all_pass1, all_pass2 = [], []
        all_psec1, all_psec2 = [], []
        all_ssh1, all_ssh2 = [], []

        for file in files:
            with self.open_file(file) as f:
                nc_file = nc.Dataset("dummy", memory=f.read())
                try:
                    size = len(nc_file["time1"])
                    if size == 0:
                        continue
                    all_cycle1.append(nc_file["cycle1"][:])
                    all_cycle2.append(nc_file["cycle2"][:])
                    all_pass1.append(nc_file["pass1"][:])
                    all_pass2.append(nc_file["pass2"][:])
                    all_ssh1.append(nc_file["ssh1"][:])
                    all_ssh2.append(nc_file["ssh2"][:])
                    all_psec1.append(nc_file["time1"][:] + ref_tstamp)
                    all_psec2.append(nc_file["time2"][:] + ref_tstamp)
                finally:
                    nc_file.close()

        if not all_ssh1:
            self.dssh = np.array([])
            self.psec = np.array([])
            self.trackid = np.array([])
            logging.info("Loading data complete (no data)")
            return

        cycle1 = np.concatenate(all_cycle1)
        cycle2 = np.concatenate(all_cycle2)
        pass1 = np.concatenate(all_pass1)
        pass2 = np.concatenate(all_pass2)
        psec1 = np.concatenate(all_psec1)
        psec2 = np.concatenate(all_psec2)
        ssh1 = np.concatenate(all_ssh1)
        ssh2 = np.concatenate(all_ssh2)

        dssh0 = ssh1 - ssh2
        self.dssh = np.concatenate((dssh0, -dssh0))
        self.psec = np.concatenate((psec1, psec2))
        self.trackid = np.concatenate(
            (cycle1 * self.TRACKID_CYCLE_FACTOR + pass1, cycle2 * self.TRACKID_CYCLE_FACTOR + pass2)
        )
        logging.info("Loading data complete")

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
        s3_key = f"s3://{bucket}/bad_passes/{source}/{date}.json"
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
