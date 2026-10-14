import unittest
from datetime import date
from unittest.mock import MagicMock, patch

from app import handler, resolve_keys

# ---------------------------------------------------------------------------
# Tests — key resolution per family
# ---------------------------------------------------------------------------


class TestAlongTrackKeys(unittest.TestCase):
    """The along-track family copies one contributing source's P3 to the
    published address. Internal and published filenames differ deliberately, so a
    staging file and a released file are never mistaken for each other."""

    def test_reference_contributor_to_the_shared_line(self):
        src, dst = resolve_keys("along_track", "NASA-SSH", ["GSFC"], date(2020, 3, 15))
        self.assertEqual(src, "daily_files/p3/GSFC/2020/GSFC_alt_ref_at_v1_1_20200315.nc")
        self.assertEqual(
            dst,
            "products/NASA-SSH/along_track/2020/NASA-SSH_alt_ref_at_v1_1_20200315.nc",
        )

    def test_second_reference_contributor_resolves_the_same_destination(self):
        _, dst = resolve_keys("along_track", "NASA-SSH", ["S6"], date(2025, 2, 10))
        self.assertEqual(
            dst,
            "products/NASA-SSH/along_track/2025/NASA-SSH_alt_ref_at_v1_1_20250210.nc",
        )

    def test_high_latitude_contributor_to_its_own_line(self):
        src, dst = resolve_keys(
            "along_track", "NASA-SSH_S3B", ["S3B"], date(2025, 2, 10)
        )
        self.assertEqual(src, "daily_files/p3/S3B/2025/S3B_alt_hilat_at_v1_1_20250210.nc")
        self.assertEqual(
            dst,
            "products/NASA-SSH_S3B/along_track/2025/"
            "NASA-SSH_S3B_alt_hilat_at_v1_1_20250210.nc",
        )

    def test_internal_and_published_names_differ(self):
        src, dst = resolve_keys("along_track", "NASA-SSH", ["S6"], date(2025, 2, 10))
        self.assertNotEqual(src.rsplit("/", 1)[-1], dst.rsplit("/", 1)[-1])

    def test_no_contributors_raises(self):
        with self.assertRaises(ValueError):
            resolve_keys("along_track", "NASA-SSH", [], date(2025, 2, 10))

    def test_contested_date_refuses_rather_than_picking_one(self):
        """The runtime half of the contested-date guard. Letting execution order
        decide which mission's file ships is the failure this exists to stop."""
        with self.assertRaises(ValueError) as ctx:
            resolve_keys("along_track", "NASA-SSH", ["GSFC", "S6B"], date(2025, 12, 1))
        self.assertIn("contested", str(ctx.exception))

    def test_unknown_contributor_raises(self):
        with self.assertRaises(ValueError):
            resolve_keys("along_track", "NASA-SSH", ["NOPE"], date(2025, 2, 10))


class TestSimpleGridKeys(unittest.TestCase):
    def test_grid_promotion_reads_the_staged_copy(self):
        src, dst = resolve_keys("simple_grid", "NASA-SSH", [], date(2025, 1, 7))
        self.assertEqual(
            src,
            "simple_grids/NASA-SSH/2025/NASA-SSH_alt_ref_simple_grid_v1_1_20250107.nc",
        )
        self.assertEqual(
            dst,
            "products/NASA-SSH/simple_grid/2025/"
            "NASA-SSH_alt_ref_simple_grid_v1_1_20250107.nc",
        )

    def test_grid_promotion_needs_no_contributors(self):
        """A staged grid is keyed on its own output identity, so the contributor
        list that produced it is not needed to find it."""
        self.assertEqual(
            resolve_keys("simple_grid", "NASA-SSH", [], date(2025, 1, 7)),
            resolve_keys("simple_grid", "NASA-SSH", ["NASA-SSH"], date(2025, 1, 7)),
        )

    def test_line_publishing_no_grid_raises(self):
        with self.assertRaises(ValueError):
            resolve_keys("simple_grid", "NASA-SSH_S3B", [], date(2025, 1, 7))


class TestUnknownFamily(unittest.TestCase):
    def test_unknown_family_raises(self):
        with self.assertRaises(ValueError) as ctx:
            resolve_keys("ortho_maps", "NASA-SSH", [], date(2025, 1, 7))
        self.assertIn("Unknown product family", str(ctx.exception))


# ---------------------------------------------------------------------------
# Tests — handler
# ---------------------------------------------------------------------------


def _event(**overrides):
    event = {
        "bucket": "my-bucket",
        "date": "2025-02-10",
        "family": "along_track",
        "publish_as": "NASA-SSH",
        "contributors": ["S6"],
    }
    event.update(overrides)
    return event


class TestPublisherHandler(unittest.TestCase):

    @patch("app.boto3")
    def test_along_track_copies_to_the_published_address(self, mock_boto3):
        mock_s3 = MagicMock()
        mock_boto3.client.return_value = mock_s3

        result = handler(_event(), None)

        self.assertEqual(result["status"], "success")
        mock_s3.copy_object.assert_called_once_with(
            Bucket="my-bucket",
            CopySource={
                "Bucket": "my-bucket",
                "Key": "daily_files/p3/S6/2025/S6_alt_ref_at_v1_1_20250210.nc",
            },
            Key="products/NASA-SSH/along_track/2025/NASA-SSH_alt_ref_at_v1_1_20250210.nc",
        )

    @patch("app.boto3")
    def test_grid_promotion_copies_to_the_published_address(self, mock_boto3):
        mock_s3 = MagicMock()
        mock_boto3.client.return_value = mock_s3

        result = handler(
            _event(date="2025-01-07", family="simple_grid", contributors=["NASA-SSH"]),
            None,
        )

        self.assertEqual(result["status"], "success")
        mock_s3.copy_object.assert_called_once_with(
            Bucket="my-bucket",
            CopySource={
                "Bucket": "my-bucket",
                "Key": "simple_grids/NASA-SSH/2025/"
                "NASA-SSH_alt_ref_simple_grid_v1_1_20250107.nc",
            },
            Key="products/NASA-SSH/simple_grid/2025/"
            "NASA-SSH_alt_ref_simple_grid_v1_1_20250107.nc",
        )


class TestPublisherJobOutcome(unittest.TestCase):

    @patch("app.boto3")
    def test_along_track_outcome(self, mock_boto3):
        mock_boto3.client.return_value = MagicMock()

        result = handler(_event(), None)

        self.assertEqual(result["stage"], "publisher")
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["date"], "2025-02-10")
        # The outcome is keyed on the published product, not the contributor.
        self.assertEqual(result["source"], "NASA-SSH")
        self.assertEqual(
            result["outputs"],
            [{
                "key": "products/NASA-SSH/along_track/2025/"
                       "NASA-SSH_alt_ref_at_v1_1_20250210.nc",
                "kind": "published_along_track",
            }],
        )
        self.assertEqual(result["metadata"]["family"], "along_track")
        self.assertEqual(result["metadata"]["contributors"], ["S6"])
        self.assertEqual(
            result["metadata"]["copied_from"],
            "daily_files/p3/S6/2025/S6_alt_ref_at_v1_1_20250210.nc",
        )

    @patch("app.boto3")
    def test_the_two_families_declare_distinct_kinds(self, mock_boto3):
        """run_summary reconciles the two promotions separately, so they must not
        share a kind."""
        mock_boto3.client.return_value = MagicMock()

        at = handler(_event(), None)
        grid = handler(_event(date="2025-01-07", family="simple_grid"), None)
        self.assertNotEqual(
            at["outputs"][0]["kind"], grid["outputs"][0]["kind"]
        )
        self.assertEqual(grid["outputs"][0]["kind"], "published_simple_grid")

    @patch("app.boto3")
    def test_outcome_omits_provenance_complete(self, mock_boto3):
        """The publisher never opens the file; the internal artifact's own outcome
        carries the authoritative lineage flag, and run_summary reads absence as
        'unknown' rather than failing reconciliation."""
        mock_boto3.client.return_value = MagicMock()
        result = handler(_event(), None)
        self.assertNotIn("provenance_complete", result["metadata"])


class TestPublisherRejectsIncompleteEvents(unittest.TestCase):

    def test_missing_family_raises(self):
        event = _event()
        del event["family"]
        with self.assertRaises(ValueError):
            handler(event, None)

    def test_missing_publish_as_raises(self):
        event = _event()
        del event["publish_as"]
        with self.assertRaises(ValueError):
            handler(event, None)

    def test_missing_date_raises(self):
        event = _event()
        del event["date"]
        with self.assertRaises(ValueError):
            handler(event, None)


if __name__ == "__main__":
    unittest.main()
