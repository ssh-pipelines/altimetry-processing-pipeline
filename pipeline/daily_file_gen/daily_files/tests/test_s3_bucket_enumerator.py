import json
import unittest
from datetime import datetime
from unittest.mock import patch, MagicMock

from daily_files.config.source_config import SourceConfig, SmoothingConfig
from daily_files.fetching.s3_bucket_enumerator import S3BucketEnumerator


def _make_source_config(**overrides):
    defaults = dict(
        source="EXAMPLE_S3",
        product_type="reference",
        filename_template="{source}_alt_ref_at_v11_{date}.nc",
        s3_prefix="daily_files/p1",
        source_mss="DTU15",
        target_mss="DTU21",
        mss_diff_file="DTU15_minus_DTU21.nc",
        empty_template="gsfc_empty_template.nc",
        smoothing=SmoothingConfig(speed=5.745, sigma=15),
        discovery_type="s3_bucket",
        source_bucket="example-source-bucket",
        source_prefix_pattern="data/{source}/{year}",
        source_filename_pattern="{source}_{date8}.nc",
    )
    defaults.update(overrides)
    return SourceConfig(**defaults)


class TestBuildPrefix(unittest.TestCase):
    @patch("daily_files.fetching.s3_bucket_enumerator.boto3")
    def test_build_prefix_interpolates_source_and_year(self, mock_boto3):
        cfg = _make_source_config()
        enum = S3BucketEnumerator(datetime(2023, 7, 15), cfg)
        prefix = enum._build_prefix()
        self.assertEqual(prefix, "data/EXAMPLE_S3/2023")

    @patch("daily_files.fetching.s3_bucket_enumerator.boto3")
    def test_build_prefix_interpolates_month_day(self, mock_boto3):
        cfg = _make_source_config(source_prefix_pattern="data/{source}/{year}/{month}/{day}")
        enum = S3BucketEnumerator(datetime(2023, 1, 5), cfg)
        prefix = enum._build_prefix()
        self.assertEqual(prefix, "data/EXAMPLE_S3/2023/01/05")


class TestBuildFilenameRegex(unittest.TestCase):
    @patch("daily_files.fetching.s3_bucket_enumerator.boto3")
    def test_regex_matches_expected_filename(self, mock_boto3):
        import re

        cfg = _make_source_config()
        enum = S3BucketEnumerator(datetime(2023, 7, 15), cfg)
        regex = enum._build_filename_regex()
        match = re.search(regex, "EXAMPLE_S3_20230715.nc")
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "20230715")

    @patch("daily_files.fetching.s3_bucket_enumerator.boto3")
    def test_regex_does_not_match_wrong_source(self, mock_boto3):
        import re

        cfg = _make_source_config()
        enum = S3BucketEnumerator(datetime(2023, 7, 15), cfg)
        regex = enum._build_filename_regex()
        match = re.search(regex, "OTHER_20230715.nc")
        self.assertIsNone(match)


class TestEnumerate(unittest.TestCase):
    def _make_paginator(self, pages):
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = pages
        return mock_paginator

    @patch("daily_files.fetching.s3_bucket_enumerator.boto3")
    def test_enumerate_finds_matching_files(self, mock_boto3):
        cfg = _make_source_config()
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.get_paginator.return_value = self._make_paginator(
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

        enum = S3BucketEnumerator(datetime(2023, 7, 15), cfg)
        refs = enum.enumerate()

        self.assertEqual(len(refs), 1)
        self.assertEqual(refs[0].title, "EXAMPLE_S3_20230715.nc")
        self.assertEqual(
            refs[0].access_url,
            "s3://example-source-bucket/data/EXAMPLE_S3/2023/EXAMPLE_S3_20230715.nc",
        )
        self.assertEqual(refs[0].collection_id, "")

    @patch("daily_files.fetching.s3_bucket_enumerator.boto3")
    def test_enumerate_skips_other_dates(self, mock_boto3):
        cfg = _make_source_config()
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.get_paginator.return_value = self._make_paginator(
            [
                {
                    "Contents": [
                        {
                            "Key": "data/EXAMPLE_S3/2023/EXAMPLE_S3_20230716.nc",
                            "LastModified": datetime(2023, 8, 2, 12, 0, 0),
                        },
                    ]
                }
            ]
        )

        enum = S3BucketEnumerator(datetime(2023, 7, 15), cfg)
        refs = enum.enumerate()
        self.assertEqual(len(refs), 0)

    @patch("daily_files.fetching.s3_bucket_enumerator.boto3")
    def test_enumerate_returns_empty_for_no_contents(self, mock_boto3):
        cfg = _make_source_config()
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.get_paginator.return_value = self._make_paginator([{}])

        enum = S3BucketEnumerator(datetime(2023, 7, 15), cfg)
        refs = enum.enumerate()
        self.assertEqual(len(refs), 0)


CYCLE_INDEX = {
    "cycle_001.nc": {"start": "2023-07-01", "end": "2023-07-10"},
    "cycle_002.nc": {"start": "2023-07-11", "end": "2023-07-20"},
    "cycle_003.nc": {"start": "2023-07-21", "end": "2023-07-31"},
}


class TestEnumerateWithCycleIndex(unittest.TestCase):
    def _make_paginator(self, pages):
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = pages
        return mock_paginator

    def _mock_get_object(self, index_data):
        body = MagicMock()
        body.read.return_value = json.dumps(index_data).encode()
        return {"Body": body}

    @patch("daily_files.fetching.s3_bucket_enumerator.boto3")
    def test_returns_file_for_date_within_cycle(self, mock_boto3):
        cfg = _make_source_config(cycle_index_key="data/EXAMPLE_S3/cycle_index.json")
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.get_object.return_value = self._mock_get_object(CYCLE_INDEX)
        mock_client.get_paginator.return_value = self._make_paginator(
            [
                {
                    "Contents": [
                        {
                            "Key": "data/EXAMPLE_S3/2023/cycle_002.nc",
                            "LastModified": datetime(2023, 8, 1, 12, 0, 0),
                        },
                    ]
                }
            ]
        )

        enum = S3BucketEnumerator(datetime(2023, 7, 15), cfg)
        refs = enum.enumerate()

        self.assertEqual(len(refs), 1)
        self.assertEqual(refs[0].title, "cycle_002.nc")
        self.assertEqual(refs[0].access_url, "s3://example-source-bucket/data/EXAMPLE_S3/2023/cycle_002.nc")
        self.assertEqual(refs[0].time_start, "2023-07-11T00:00:00Z")
        self.assertEqual(refs[0].time_end, "2023-07-20T23:59:59Z")

    @patch("daily_files.fetching.s3_bucket_enumerator.boto3")
    def test_returns_both_files_when_date_spans_boundary(self, mock_boto3):
        # Date falls on the boundary of two cycles (end of cycle_001, start of cycle_002)
        index = {
            "cycle_001.nc": {"start": "2023-07-01", "end": "2023-07-15"},
            "cycle_002.nc": {"start": "2023-07-15", "end": "2023-07-25"},
        }
        cfg = _make_source_config(cycle_index_key="data/EXAMPLE_S3/cycle_index.json")
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.get_object.return_value = self._mock_get_object(index)
        mock_client.get_paginator.return_value = self._make_paginator(
            [
                {
                    "Contents": [
                        {
                            "Key": "data/EXAMPLE_S3/2023/cycle_001.nc",
                            "LastModified": datetime(2023, 8, 1, 12, 0, 0),
                        },
                        {
                            "Key": "data/EXAMPLE_S3/2023/cycle_002.nc",
                            "LastModified": datetime(2023, 8, 2, 12, 0, 0),
                        },
                    ]
                }
            ]
        )

        enum = S3BucketEnumerator(datetime(2023, 7, 15), cfg)
        refs = enum.enumerate()

        self.assertEqual(len(refs), 2)
        titles = {r.title for r in refs}
        self.assertEqual(titles, {"cycle_001.nc", "cycle_002.nc"})

    @patch("daily_files.fetching.s3_bucket_enumerator.boto3")
    def test_returns_empty_when_date_outside_all_cycles(self, mock_boto3):
        cfg = _make_source_config(cycle_index_key="data/EXAMPLE_S3/cycle_index.json")
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.get_object.return_value = self._mock_get_object(CYCLE_INDEX)
        mock_client.get_paginator.return_value = self._make_paginator([{}])

        enum = S3BucketEnumerator(datetime(2023, 8, 15), cfg)
        refs = enum.enumerate()

        self.assertEqual(len(refs), 0)

    @patch("daily_files.fetching.s3_bucket_enumerator.boto3")
    def test_dispatches_to_cycle_index_path_when_key_set(self, mock_boto3):
        cfg = _make_source_config(cycle_index_key="data/EXAMPLE_S3/cycle_index.json")
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.get_object.return_value = self._mock_get_object(CYCLE_INDEX)
        mock_client.get_paginator.return_value = self._make_paginator([{}])

        enum = S3BucketEnumerator(datetime(2023, 7, 5), cfg)
        enum.enumerate()

        # Should have called get_object for the cycle index
        mock_client.get_object.assert_called_once_with(
            Bucket="example-source-bucket",
            Key="data/EXAMPLE_S3/cycle_index.json",
        )

    @patch("daily_files.fetching.s3_bucket_enumerator.boto3")
    def test_does_not_use_cycle_index_when_key_not_set(self, mock_boto3):
        cfg = _make_source_config(cycle_index_key=None)
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.get_paginator.return_value = self._make_paginator([{}])

        enum = S3BucketEnumerator(datetime(2023, 7, 15), cfg)
        enum.enumerate()

        # Should NOT have called get_object (no cycle index)
        mock_client.get_object.assert_not_called()
