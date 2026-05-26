import json
import unittest
from dataclasses import replace
from datetime import date, datetime
from unittest.mock import MagicMock, patch

from config.source_config import get_source_config
from enumeration.s3_bucket import S3BucketEnumerator


def _filename_regex_config():
    return replace(get_source_config("EXAMPLE_S3"), cycle_index_key=None)


def _make_paginator(pages):
    paginator = MagicMock()
    paginator.paginate.return_value = pages
    return paginator


def _mock_get_object(index_data):
    body = MagicMock()
    body.read.return_value = json.dumps(index_data).encode()
    return {"Body": body}


class TestEnumerateFilenameRegex(unittest.TestCase):
    @patch("enumeration.s3_bucket.boto3")
    def test_finds_matching_files_in_range(self, mock_boto3):
        cfg = _filename_regex_config()
        client = MagicMock()
        mock_boto3.client.return_value = client
        client.get_paginator.return_value = _make_paginator(
            [
                {
                    "Contents": [
                        {
                            "Key": "data/EXAMPLE_S3/2023/EXAMPLE_S3_20230715.nc",
                            "LastModified": datetime(2023, 8, 1, 12, 0, 0),
                        },
                        {
                            "Key": "data/EXAMPLE_S3/2023/EXAMPLE_S3_20230716.nc",
                            "LastModified": datetime(2023, 8, 2, 12, 0, 0),
                        },
                    ]
                }
            ]
        )

        enum = S3BucketEnumerator(cfg, "example-source-bucket")
        refs = enum.enumerate(date(2023, 7, 15), date(2023, 7, 15))

        self.assertEqual(len(refs), 1)
        self.assertEqual(
            refs[0].uri,
            "s3://example-source-bucket/data/EXAMPLE_S3/2023/EXAMPLE_S3_20230715.nc",
        )
        self.assertEqual(refs[0].date, date(2023, 7, 15))
        self.assertEqual(refs[0].mod_time, datetime(2023, 8, 1, 12, 0, 0))

    @patch("enumeration.s3_bucket.boto3")
    def test_returns_empty_for_no_contents(self, mock_boto3):
        cfg = _filename_regex_config()
        client = MagicMock()
        mock_boto3.client.return_value = client
        client.get_paginator.return_value = _make_paginator([{}])

        enum = S3BucketEnumerator(cfg, "example-source-bucket")
        refs = enum.enumerate(date(2023, 7, 15), date(2023, 7, 15))
        self.assertEqual(len(refs), 0)


CYCLE_INDEX = {
    "cycle_001.nc": {"start": "2023-07-01", "end": "2023-07-10"},
    "cycle_002.nc": {"start": "2023-07-11", "end": "2023-07-20"},
}


class TestEnumerateCycleIndex(unittest.TestCase):
    @patch("enumeration.s3_bucket.boto3")
    def test_emits_one_ref_per_covered_date(self, mock_boto3):
        cfg = get_source_config("EXAMPLE_S3")
        client = MagicMock()
        mock_boto3.client.return_value = client
        client.get_object.return_value = _mock_get_object(CYCLE_INDEX)
        client.get_paginator.return_value = _make_paginator(
            [
                {
                    "Contents": [
                        {
                            "Key": "data/EXAMPLE_S3/2023/cycle_001.nc",
                            "LastModified": datetime(2023, 8, 1, 12, 0, 0),
                        },
                        {
                            "Key": "data/EXAMPLE_S3/2023/cycle_002.nc",
                            "LastModified": datetime(2023, 8, 5, 12, 0, 0),
                        },
                    ]
                }
            ]
        )

        enum = S3BucketEnumerator(cfg, "example-source-bucket")
        refs = enum.enumerate(date(2023, 7, 5), date(2023, 7, 12))

        # Days 7/5..7/10 covered by cycle_001, days 7/11..7/12 covered by cycle_002
        dates = [r.date for r in refs]
        self.assertEqual(min(dates), date(2023, 7, 5))
        self.assertEqual(max(dates), date(2023, 7, 12))
        # Each covered date should produce exactly one ref
        self.assertEqual(len(refs), 8)

    @patch("enumeration.s3_bucket.boto3")
    def test_returns_empty_when_outside_cycle_coverage(self, mock_boto3):
        cfg = get_source_config("EXAMPLE_S3")
        client = MagicMock()
        mock_boto3.client.return_value = client
        client.get_object.return_value = _mock_get_object(CYCLE_INDEX)
        client.get_paginator.return_value = _make_paginator([{}])

        enum = S3BucketEnumerator(cfg, "example-source-bucket")
        refs = enum.enumerate(date(2023, 8, 15), date(2023, 8, 20))
        self.assertEqual(refs, [])
