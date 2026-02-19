import unittest
from datetime import datetime
from unittest.mock import patch, MagicMock

from config.source_config import get_source_config
from app import query_source_bucket


class TestQuerySourceBucket(unittest.TestCase):
    def _make_paginator(self, pages):
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = pages
        return mock_paginator

    @patch("app.s3")
    def test_parses_listing_to_date_modtime_map(self, mock_s3):
        config = get_source_config("EXAMPLE_S3")
        mod_time = datetime(2023, 8, 1, 12, 0, 0)
        mock_s3.get_paginator.return_value = self._make_paginator(
            [
                {
                    "Contents": [
                        {
                            "Key": "data/EXAMPLE_S3/2023/EXAMPLE_S3_20230715.nc",
                            "LastModified": mod_time,
                        },
                        {
                            "Key": "data/EXAMPLE_S3/2023/EXAMPLE_S3_20230716.nc",
                            "LastModified": datetime(2023, 8, 2, 12, 0, 0),
                        },
                    ]
                }
            ]
        )

        start = datetime(2023, 7, 15)
        end = datetime(2023, 7, 16)
        result = query_source_bucket(start, end, config)

        self.assertIn(start.date(), result)
        self.assertIn(end.date(), result)
        self.assertEqual(result[start.date()], mod_time)

    @patch("app.s3")
    def test_returns_empty_when_no_files(self, mock_s3):
        config = get_source_config("EXAMPLE_S3")
        mock_s3.get_paginator.return_value = self._make_paginator([{}])

        start = datetime(2023, 7, 15)
        end = datetime(2023, 7, 15)
        result = query_source_bucket(start, end, config)

        self.assertEqual(result, {})

    @patch("app.s3")
    def test_filters_to_date_range(self, mock_s3):
        config = get_source_config("EXAMPLE_S3")
        mock_s3.get_paginator.return_value = self._make_paginator(
            [
                {
                    "Contents": [
                        {
                            "Key": "data/EXAMPLE_S3/2023/EXAMPLE_S3_20230710.nc",
                            "LastModified": datetime(2023, 8, 1),
                        },
                        {
                            "Key": "data/EXAMPLE_S3/2023/EXAMPLE_S3_20230715.nc",
                            "LastModified": datetime(2023, 8, 1),
                        },
                        {
                            "Key": "data/EXAMPLE_S3/2023/EXAMPLE_S3_20230720.nc",
                            "LastModified": datetime(2023, 8, 1),
                        },
                    ]
                }
            ]
        )

        start = datetime(2023, 7, 14)
        end = datetime(2023, 7, 16)
        result = query_source_bucket(start, end, config)

        self.assertIn(datetime(2023, 7, 15).date(), result)
        self.assertNotIn(datetime(2023, 7, 10).date(), result)
        self.assertNotIn(datetime(2023, 7, 20).date(), result)


class TestHandlerDispatch(unittest.TestCase):
    @patch("app.s3")
    @patch("app.query_source_bucket")
    @patch("app.query_cmr")
    def test_dispatches_to_query_source_bucket_for_s3_type(self, mock_query_cmr, mock_query_source_bucket, mock_s3):
        mock_query_source_bucket.return_value = {}
        mock_s3.get_paginator.return_value = MagicMock(paginate=MagicMock(return_value=[{}]))
        mock_s3.put_object.return_value = {}

        from app import handler

        event = {
            "bucket": "test-bucket",
            "source": "EXAMPLE_S3",
            "start": "2023-07-15",
            "end": "2023-07-15",
        }
        handler(event, None)

        mock_query_source_bucket.assert_called_once()
        mock_query_cmr.assert_not_called()

    @patch("app.s3")
    @patch("app.query_source_bucket")
    @patch("app.query_cmr")
    def test_dispatches_to_query_cmr_for_cmr_type(self, mock_query_cmr, mock_query_source_bucket, mock_s3):
        mock_query_cmr.return_value = {}
        mock_s3.get_paginator.return_value = MagicMock(paginate=MagicMock(return_value=[{}]))
        mock_s3.put_object.return_value = {}

        from app import handler

        event = {
            "bucket": "test-bucket",
            "source": "GSFC",
            "start": "2023-07-15",
            "end": "2023-07-15",
        }
        handler(event, None)

        mock_query_cmr.assert_called()
        mock_query_source_bucket.assert_not_called()
