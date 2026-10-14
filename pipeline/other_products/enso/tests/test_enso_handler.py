import unittest
from unittest.mock import patch


class TestEnsoHandlerOutcome(unittest.TestCase):
    """ENSO is keyed on a published product, not a Source: it derives from the
    reference-mission grid only, and that grid is composed from several missions."""

    @patch("app.enso_processing.start_job")
    def test_success_declares_enso_grid_output(self, mock_start_job):
        from app import handler

        mock_start_job.return_value = "enso_grids/NASA-SSH/ENSO_20250107.nc"

        event = {"bucket": "b", "date": "2025-01-07", "published_product": "NASA-SSH"}
        result = handler(event, None)

        self.assertEqual(result["stage"], "enso")
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["date"], "2025-01-07")
        self.assertEqual(result["source"], "NASA-SSH")
        self.assertEqual(
            result["outputs"],
            [{"key": "enso_grids/NASA-SSH/ENSO_20250107.nc", "kind": "enso_grid"}],
        )

    @patch("app.enso_processing.start_job")
    def test_processing_error_propagates(self, mock_start_job):
        from app import handler

        from utilities.errors import PipelineError

        mock_start_job.side_effect = RuntimeError("boom")

        event = {"bucket": "b", "date": "2025-01-07", "published_product": "NASA-SSH"}
        with self.assertRaises(PipelineError):
            handler(event, None)

    def test_missing_published_product_raises(self):
        from app import handler

        with self.assertRaises(ValueError):
            handler({"bucket": "b", "date": "2025-01-07"}, None)


if __name__ == "__main__":
    unittest.main()
