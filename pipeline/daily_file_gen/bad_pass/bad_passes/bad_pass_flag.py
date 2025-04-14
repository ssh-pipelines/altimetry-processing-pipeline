import logging
from datetime import datetime, timedelta
from typing import Dict, Iterable, List

import netCDF4 as nc
import numpy as np

from utilities.aws_utils import aws_manager


class XoverProcessor:
    def __init__(self, source: str, date: datetime):
        self.source = source
        self.date = date
        self.windowlen = 10
        self.windowpad = 1
        self.window_start = date - timedelta(self.windowlen) - timedelta(self.windowpad)
        self.window_end = date + timedelta(self.windowpad)

    def get_files(self) -> Iterable[str]:
        window_range = []
        cur_date = self.window_start
        while cur_date <= self.window_end:
            xover_filename = f'xovers_{self.source}-{cur_date.strftime("%Y-%m-%d")}.nc'
            xover_path = f"s3://example-bucket/crossovers/p2/{self.source}/{cur_date.year}/{xover_filename}"
            if aws_manager.key_exists(xover_path):
                window_range.append(xover_path)
            else:
                logging.info(f"Key {xover_path} does not exist")
            cur_date = cur_date + timedelta(days=1)
        logging.info(f"Found {len(window_range)} within window")
        return window_range

    def open_file(self, file):
        if "s3" in file:
            return aws_manager.fs.open(file, "rb")
        return open(file, "rb")

    def load_all_data(self, files: Iterable[str]):
        logging.info("Loading all data...")

        ref_tstamp = datetime(1990, 1, 1).timestamp()

        # Init np arrays with sufficient size
        total_size = len(files) * 2000
        cycle1, cycle2, pass1, pass2, psec1, psec2, ssh1, ssh2 = (
            np.empty(total_size) for _ in range(8)
        )

        # Populate arrays
        index = 0
        for file in files:
            with self.open_file(file) as f:
                nc_file = nc.Dataset("dummy", memory=f.read())
                size = len(nc_file["time1"])
                if size == 0:
                    continue
                cycle1[index : index + size] = nc_file["cycle1"][:]
                cycle2[index : index + size] = nc_file["cycle2"][:]
                pass1[index : index + size] = nc_file["pass1"][:]
                pass2[index : index + size] = nc_file["pass2"][:]
                ssh1[index : index + size] = nc_file["ssh1"][:]
                ssh2[index : index + size] = nc_file["ssh2"][:]
                psec1[index : index + size] = nc_file["time1"][:] + ref_tstamp
                psec2[index : index + size] = nc_file["time2"][:] + ref_tstamp
            index += size

        # Crop arrays
        cycle1 = cycle1[:index]
        cycle2 = cycle2[:index]
        pass1 = pass1[:index]
        pass2 = pass2[:index]
        psec1 = psec1[:index]
        psec2 = psec2[:index]
        ssh1 = ssh1[:index]
        ssh2 = ssh2[:index]

        dssh0 = ssh1 - ssh2
        self.dssh = np.concatenate((dssh0, -dssh0))
        self.psec = np.concatenate((psec1, psec2))
        self.trackid = np.concatenate((cycle1 * 10000 + pass1, cycle2 * 10000 + pass2))
        logging.info("Loading data complete")

    def identify_bad_passes(self, currentdate: float) -> List[Dict[str, str]]:
        max_mean = 0.1
        max_rms = 0.27
        nmean = 15
        nrms = 25

        bad_passes = []
        ii = np.where(
            (self.psec >= currentdate - 3600)
            & (self.psec <= currentdate + 86400 + 3600)
        )[0]
        tid_list = np.unique(self.trackid[ii])

        for tid in tid_list:
            jj = np.where(self.trackid == tid)[0]
            if len(jj) >= min(nmean, nrms):
                xmean = np.mean(self.dssh[jj])
                xrms = np.std(self.dssh[jj], ddof=1)
                check_mean = (len(jj) > nmean) & (np.abs(xmean) > max_mean)
                check_rms = (len(jj) > nrms) & (xrms > max_rms)
                if check_mean | check_rms:
                    cycle = str(int(np.floor(tid / 10000)))
                    pass_num = str(int(tid % 10000))
                    bad_passes.append({"cycle": cycle, "pass_num": pass_num})
        return bad_passes

    def process(self):
        logging.info(f"Finding {self.source} bad passes for {self.date}")
        file_paths = self.get_files()
        self.load_all_data(file_paths)
        currentdate = datetime.timestamp(self.date)
        # Get list of (cycle, pass_num)
        bad_passes = self.identify_bad_passes(currentdate)
        logging.info(
            f"Found {len(bad_passes)} {self.source} bad passes for {self.date}"
        )
        formatted_results = {
            "date": self.date.date().isoformat(),
            "source": self.source,
            "bad_passes": bad_passes,
        }
        return formatted_results
