import unittest
from unittest.mock import patch


class TestEnsoHandlerOutcome(unittest.TestCase):

    @patch("app.enso_processing.start_job")
    def test_success_declares_enso_grid_output(self, mock_start_job):
        from app import handler

        mock_start_job.return_value = "enso_grids/S6/ENSO_20250107.nc"

        event = {"bucket": "b", "date": "2025-01-07", "source": "S6"}
        result = handler(event, None)

        self.assertEqual(result["stage"], "enso")
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["date"], "2025-01-07")
        self.assertEqual(result["source"], "S6")
        self.assertEqual(
            result["outputs"],
            [{"key": "enso_grids/S6/ENSO_20250107.nc", "kind": "enso_grid"}],
        )

    @patch("app.enso_processing.start_job")
    def test_processing_error_propagates(self, mock_start_job):
        from app import handler

        from utilities.errors import PipelineError

        mock_start_job.side_effect = RuntimeError("boom")

        event = {"bucket": "b", "date": "2025-01-07", "source": "S6"}
        with self.assertRaises(PipelineError):
            handler(event, None)


if __name__ == "__main__":
    unittest.main()
