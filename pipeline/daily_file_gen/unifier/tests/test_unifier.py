import unittest
from unittest.mock import MagicMock, patch

from config.source_config import (
    get_source_config,
    get_available_sources,
)


# ---------------------------------------------------------------------------
# Tests — source config loading
# ---------------------------------------------------------------------------

class TestSourceConfig(unittest.TestCase):

    def test_available_sources_include_gsfc_and_s6(self):
        sources = get_available_sources()
        self.assertIn("GSFC", sources)
        self.assertIn("S6", sources)

    def test_s6b_not_in_available_sources(self):
        sources = get_available_sources()
        self.assertNotIn("S6B", sources)

    def test_gsfc_config_loads(self):
        cfg = get_source_config("GSFC")
        self.assertEqual(cfg.source, "GSFC")
        self.assertEqual(cfg.product_type, "reference")

    def test_s6_config_loads(self):
        cfg = get_source_config("S6")
        self.assertEqual(cfg.source, "S6")

    def test_invalid_source_raises(self):
        with self.assertRaises(ValueError):
            get_source_config("INVALID")


# ---------------------------------------------------------------------------
# Tests — handler
# ---------------------------------------------------------------------------

class TestUnifierHandler(unittest.TestCase):

    @patch("app.boto3")
    def test_handler_copies_to_nasa_path(self, mock_boto3):
        from app import handler

        mock_s3 = MagicMock()
        mock_boto3.client.return_value = mock_s3

        event = {"bucket": "my-bucket", "date": "2020-03-15", "source": "GSFC"}
        result = handler(event, None)

        self.assertEqual(result["status"], "success")
        mock_s3.copy_object.assert_called_once_with(
            Bucket="my-bucket",
            CopySource={
                "Bucket": "my-bucket",
                "Key": "daily_files/p3/GSFC/2020/GSFC_alt_ref_at_v1_1_20200315.nc",
            },
            Key="daily_files/p3/NASA-SSH/2020/NASA-SSH_alt_ref_at_v1_1_20200315.nc",
        )

    @patch("app.boto3")
    def test_handler_s6_copies_to_nasa_path(self, mock_boto3):
        from app import handler

        mock_s3 = MagicMock()
        mock_boto3.client.return_value = mock_s3

        event = {"bucket": "my-bucket", "date": "2025-02-10", "source": "S6"}
        result = handler(event, None)

        self.assertEqual(result["status"], "success")
        mock_s3.copy_object.assert_called_once_with(
            Bucket="my-bucket",
            CopySource={
                "Bucket": "my-bucket",
                "Key": "daily_files/p3/S6/2025/S6_alt_ref_at_v1_1_20250210.nc",
            },
            Key="daily_files/p3/NASA-SSH/2025/NASA-SSH_alt_ref_at_v1_1_20250210.nc",
        )


class TestUnifierJobOutcome(unittest.TestCase):

    @patch("app.boto3")
    def test_handler_declares_nasa_ssh_output(self, mock_boto3):
        from app import handler

        mock_boto3.client.return_value = MagicMock()

        event = {"bucket": "my-bucket", "date": "2025-02-10", "source": "S6"}
        result = handler(event, None)

        self.assertEqual(result["stage"], "unifier")
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["date"], "2025-02-10")
        self.assertEqual(result["source"], "NASA-SSH")
        self.assertEqual(
            result["outputs"],
            [{
                "key": "daily_files/p3/NASA-SSH/2025/NASA-SSH_alt_ref_at_v1_1_20250210.nc",
                "kind": "nasa_ssh_p3",
            }],
        )
        self.assertEqual(
            result["metadata"]["copied_from"],
            "daily_files/p3/S6/2025/S6_alt_ref_at_v1_1_20250210.nc",
        )


class TestUnifierSkipsUnconfigured(unittest.TestCase):

    def test_unconfigured_source_raises(self):
        from app import handler

        event = {"bucket": "my-bucket", "date": "2025-06-01", "source": "S6B"}
        with self.assertRaises(ValueError) as ctx:
            handler(event, None)
        self.assertIn("not configured", str(ctx.exception))

    def test_missing_params_raises_valueerror(self):
        from app import handler

        event = {"bucket": "my-bucket", "date": "2025-06-01"}
        with self.assertRaises(ValueError):
            handler(event, None)


if __name__ == "__main__":
    unittest.main()
