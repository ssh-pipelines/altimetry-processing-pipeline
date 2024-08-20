from datetime import datetime
import logging

import unittest

from daily_files.fetching.s6_fetch import S6Fetch
from daily_files.daily_files.daily_file_job import DailyFileJob


class S6FetchTestCase(unittest.TestCase):
    daily_file_job: DailyFileJob

    @classmethod
    def setUpClass(cls) -> None:
        date = "2023-05-18"
        logging.info(f"Querying CMR for S6 data on {date}")
        cls.date = datetime.strptime(date, "%Y-%m-%d")
        fetcher = S6Fetch(cls.date)

        cls.priority_granules = fetcher.priority_granules
        cls.granules = fetcher.granules

    def test_priority(self):
        """
        Need to devise test for ensuring priority selection is working.
        Could be tricky as the collections are continuously shifting
        """
        print(len(self.granules))
        for granule in self.granules:
            print(granule.s3_url.split("/")[-1])

    def test_s3_path(self):
        s3_paths = [granule.s3_url for granule in self.granules]
        self.assertTrue(s3_paths[0].startswith("s3://"))
        self.assertIn("podaac-ops-cumulus-protected", s3_paths[0])
