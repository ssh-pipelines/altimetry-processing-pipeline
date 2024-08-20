import logging
import os
import time
from datetime import datetime
from typing import Iterable, Tuple

import netCDF4 as nc
import numpy as np
import pandas as pd

from utilities.aws_utils import aws_manager


class XoverProcessor:
    def __init__(self, source: str, start_date: str = None, end_date: str = None):
        self.out_file = "s3://example-bucket/aux_files/bad_pass_list.csv"
        self.xover_pattern = "s3://example-bucket/crossovers/p2/"
        self.source = source
        self.ref_tstamp = datetime(1990, 1, 1).timestamp()
        self.windowlen = 10
        self.windowpad = 1
        self.max_mean = 0.1
        self.max_rms = 0.27
        self.nmean = 15
        self.nrms = 25
        self.bad_pass_df = self._load_bad_pass_csv()
        self.start_date = (
            datetime.strptime(start_date, "%Y-%m-%d") if start_date else None
        )
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d") if end_date else None

    def get_files(self) -> Iterable[str]:
        all_files = sorted(
            aws_manager.fs.glob(
                os.path.join(self.xover_pattern, self.source, "**", "*.nc")
            )
        )
        if not self.start_date and not self.end_date:
            logging.info(
                f"Found {len(all_files)} xover files between {self.start_date} and {self.end_date}"
            )
            return all_files

        filtered_files = []
        for file in all_files:
            file_date = self.parse_date_from_filename(file)
            if self.start_date and file_date < self.start_date:
                continue
            if self.end_date and file_date > self.end_date:
                continue
            filtered_files.append(file)
        logging.info(f"Found {len(filtered_files)} within window")
        return filtered_files

    def parse_date_from_filename(self, filename: str) -> datetime:
        tmpdate = filename.split("_")[-1].split(".")[0][-10:]
        return datetime.strptime(tmpdate, "%Y-%m-%d")

    def load_all_data(
        self, files: Iterable[str]
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        logging.info("Loading all data...")

        # Init np arrays with sufficient size
        total_size = len(files) * 2000
        cycle1, cycle2, pass1, pass2, psec1, psec2, ssh1, ssh2 = (
            np.empty(total_size) for _ in range(8)
        )

        # Populate arrays
        index = 0
        for file in files:
            with aws_manager.fs.open(file, "rb") as f:
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
                psec1[index : index + size] = nc_file["time1"][:] + self.ref_tstamp
                psec2[index : index + size] = nc_file["time2"][:] + self.ref_tstamp
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
        dssh = np.concatenate((dssh0, -dssh0))
        psec = np.concatenate((psec1, psec2))
        trackid = np.concatenate((cycle1 * 10000 + pass1, cycle2 * 10000 + pass2))

        logging.info("Loading data complete")
        return dssh, psec, trackid

    def identify_bad_passes(
        self,
        trackid: np.ndarray,
        dssh: np.ndarray,
        psec: np.ndarray,
        currentdate: float,
    ) -> Iterable[str]:
        bad_tid_list = []
        ii = np.where(
            (psec >= currentdate - 3600) & (psec <= currentdate + 86400 + 3600)
        )[0]
        tid_list = np.unique(trackid[ii])

        for tid in tid_list:
            jj = np.where(trackid == tid)[0]
            if len(jj) >= min(self.nmean, self.nrms):
                xmean = np.mean(dssh[jj])
                xrms = np.std(dssh[jj], ddof=1)
                if self.check_bad(len(jj), xmean, xrms):
                    cycle = str(int(np.floor(tid / 10000)))
                    pass_num = str(int(tid % 10000))
                    bad_tid_list.append((cycle, pass_num))
        return bad_tid_list

    def check_bad(self, len_jj, xmean, xrms) -> bool:
        check_mean = (len_jj > self.nmean) & (np.abs(xmean) > self.max_mean)
        check_rms = (len_jj > self.nrms) & (xrms > self.max_rms)
        return check_mean | check_rms

    def scan_source(self) -> pd.DataFrame:
        dates_to_drop = []
        bad_tid_list = []

        dssh, psec, trackid = self.load_all_data(self.files)

        starttime = time.time()

        for i, file in enumerate(self.files):
            tmpdate = self.parse_date_from_filename(file)
            currentdate = datetime.timestamp(tmpdate)
            cdate_str = datetime.strftime(tmpdate, "%Y-%m-%d")

            bad_passes = self.identify_bad_passes(trackid, dssh, psec, currentdate)
            if bad_passes:
                bad_tid_list.extend(
                    [
                        f"{self.source},{cdate_str},{cycle},{pass_num}"
                        for cycle, pass_num in bad_passes
                    ]
                )
            else:
                dates_to_drop.append(cdate_str)

            if (i % 100) == 0:
                logging.info(f"{i} of {len(self.files)} {time.time() - starttime}")

        mask = (self.bad_pass_df["date"].isin(dates_to_drop)) & (
            self.bad_pass_df["source"] == self.source
        )
        self.bad_pass_df = self.bad_pass_df.drop(index=self.bad_pass_df[mask].index)

        bad_tid_dfs = [
            pd.DataFrame([row.split(",")], columns=self.bad_pass_df.columns)
            for row in bad_tid_list
        ]
        self.bad_pass_df = (
            pd.concat([self.bad_pass_df] + bad_tid_dfs)
            .reset_index(drop=True)
            .drop_duplicates()
        )

        return self.bad_pass_df

    def _load_bad_pass_csv(self) -> pd.DataFrame:
        stream = aws_manager.fs.open(self.out_file)
        df = pd.read_csv(stream)
        return df

    def process(self):
        self.files = self.get_files()
        self.bad_pass_df = self.scan_source()


def update_bad_passes(gsfc_start: str, gsfc_end: str, s6_start: str, s6_end: str):
    gsfc_bad_pass_df = None
    s6_bad_pass_df = None

    # Example: Process GSFC files for a specific date range
    if gsfc_start and gsfc_end:
        logging.info(
            f"Checking GSFC between {gsfc_start} and {gsfc_end} for bad passes"
        )
        xover_processor = XoverProcessor(
            "GSFC", start_date=gsfc_start, end_date=gsfc_end
        )
        xover_processor.process()
        gsfc_bad_pass_df = xover_processor.bad_pass_df
        logging.info("GSFC check complete.")

    # Example: Process S6 files for a specific date range
    if s6_start and s6_end:
        logging.info(f"Checking S6 between {s6_start} and {s6_end} for bad passes")
        xover_processor = XoverProcessor("S6", start_date=s6_start, end_date=s6_end)
        xover_processor.process()
        s6_bad_pass_df = xover_processor.bad_pass_df
        logging.info("S6 check complete.")

    # Merge the results
    if gsfc_bad_pass_df is not None and s6_bad_pass_df is not None:
        combined_bad_pass_df = (
            pd.concat([gsfc_bad_pass_df, s6_bad_pass_df])
            .drop_duplicates()
            .reset_index(drop=True)
        )
    elif gsfc_bad_pass_df is not None:
        combined_bad_pass_df = gsfc_bad_pass_df.drop_duplicates().reset_index(drop=True)
    elif s6_bad_pass_df is not None:
        combined_bad_pass_df = s6_bad_pass_df.drop_duplicates().reset_index(drop=True)

    # Write combined results to CSV
    combined_bad_pass_df.to_csv("/tmp/bad_pass_list.csv", header=True, index=False)
    aws_manager.fs.upload("/tmp/bad_pass_list.csv", xover_processor.out_file)
    logging.info("Bad pass update complete.")
