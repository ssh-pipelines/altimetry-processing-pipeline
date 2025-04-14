from datetime import datetime
import logging
import os
import unittest
from glob import glob

from bad_passes.bad_pass_flag import XoverProcessor


class BadPassTestCase(unittest.TestCase):
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO",
        format="[%(levelname)s] %(asctime)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )

    @classmethod
    def setUpClass(cls) -> None:
        source = "GSFC"
        data_dir = "/Users/username/Desktop/manual_gsfc/xover/p2/"
        all_files = sorted(glob(f"{data_dir}*.nc"))

        all_bad_passes = []
        for xover_path in all_files:
            # xovers_GSFC-2023-12-23.nc
            xover_filename = os.path.basename(xover_path)
            proc_date = xover_filename[12:22]
            date = datetime.strptime(proc_date, "%Y-%m-%d")
            xover_processor = XoverProcessor(source, datetime.fromisoformat(proc_date))

            logging.info(f"Finding {source} bad passes for {date}")

            # REPLACE THIS WITH GETTING WINDOW FILE PATHS
            logging.info(
                f"Streaming crossover files between {xover_processor.window_start}, {xover_processor.window_end}..."
            )
            files = sorted(glob("/Users/username/Desktop/manual_gsfc/xover/p1/*.nc"))
            files = filter(
                lambda x: os.path.basename(x)
                >= f'xovers_{xover_processor.source}-{xover_processor.window_start.strftime("%Y-%m-%d")}.nc',
                files,
            )
            files = filter(
                lambda x: os.path.basename(x)
                <= f'xovers_{xover_processor.source}-{xover_processor.window_end.strftime("%Y-%m-%d")}.nc',
                files,
            )
            file_paths = sorted(list(files))

            # NEED TO OVERRIDE PULLING DATA FROM S3
            xover_processor.load_all_data(file_paths)

            currentdate = datetime.timestamp(date)
            # Get list of (cycle, pass_num)
            bad_passes = xover_processor.identify_bad_passes(currentdate)
            if len(bad_passes) > 0:
                logging.info(
                    f"Found {len(bad_passes)} {xover_processor.source} bad passes for {xover_processor.date}"
                )
                formatted_results = {
                    "date": xover_processor.date.date().isoformat(),
                    "source": xover_processor.source,
                    "bad_passes": bad_passes,
                }
                all_bad_passes.append(formatted_results)

        if len(all_bad_passes) > 0:
            for bad_pass in all_bad_passes:
                print(bad_pass.items())
        else:
            print("No bad passes found")

    def test_valid_length(self):
        pass
