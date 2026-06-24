import unittest
from unittest.mock import patch


class TestSimpleGridsHandlerOutcome(unittest.TestCase):

    @patch("app.start_job")
    def test_success_declares_simple_grid_output(self, mock_start_job):
        from app import handler

        mock_start_job.return_value = "simple_grids/S6/2025/S6_alt_ref_simple_grid_v1_1_20250107.nc"

        event = {"bucket": "b", "date": "2025-01-07", "source": "S6", "resolution": None}
        result = handler(event, None)

        self.assertEqual(result["stage"], "simple_grids")
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["date"], "2025-01-07")
        self.assertEqual(result["source"], "S6")
        self.assertEqual(
            result["outputs"],
            [{
                "key": "simple_grids/S6/2025/S6_alt_ref_simple_grid_v1_1_20250107.nc",
                "kind": "simple_grid",
            }],
        )

    @patch("app.start_job")
    def test_skip_returns_skipped_outcome(self, mock_start_job):
        from app import handler

        mock_start_job.return_value = None

        event = {"bucket": "b", "date": "2025-03-10", "source": "S6", "resolution": None}
        result = handler(event, None)

        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["outputs"], [])
        self.assertIn("skip_reason", result["metadata"])


class TestSimpleGridderKey(unittest.TestCase):

    def test_key_for_default_resolution(self):
        from simple_gridder.gridder import SimpleGridderJob

        job = SimpleGridderJob("2025-01-07", "b", "S6", None)
        self.assertEqual(
            job.key,
            "simple_grids/S6/2025/S6_alt_ref_simple_grid_v1_1_20250107.nc",
        )

    def test_quart_resolution_key_under_quart_deg(self):
        from simple_gridder.gridder import SimpleGridderJob

        job = SimpleGridderJob("2025-01-07", "b", "S6", "quart")
        self.assertTrue(job.key.startswith("simple_grids/quart_deg/S6/2025/"))
        # dst stays consistent with the key
        self.assertTrue(job.dst.endswith(job.key))


if __name__ == "__main__":
    unittest.main()
