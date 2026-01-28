import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
from daily_files.config.source_config import get_source_config
from daily_files.fetching.enumerator import FileRef


def _make_fake_file_ref(title, collection_id):
    """Create a FileRef with just the fields S6Enumerator needs."""
    return FileRef(
        id=f"id-{title}",
        title=title,
        access_url=f"s3://fake/{title}",
        time_start="2023-12-17T00:00:00Z",
        time_end="2023-12-17T23:59:59Z",
        modified_time="2023-12-17T12:00:00Z",
        collection_id=collection_id,
    )


class TestS6PrioritySelection(unittest.TestCase):
    """Test that S6Enumerator.enumerate picks the highest-priority
    (lowest priority number) granule per cycle_pass combination."""

    @patch("daily_files.fetching.cmr_enumerator._CMRQuery")
    def test_priority_selection_prefers_validated(self, mock_cmr_query_cls):
        from daily_files.fetching.cmr_enumerator import S6Enumerator

        source_config = get_source_config("S6")
        collections_sorted = sorted(source_config.collections, key=lambda c: c.priority)

        validated_refs = [
            _make_fake_file_ref("S6A_P4_2__LR_RED__NR_001_010_20231217.nc", collections_sorted[0].concept_id),
            _make_fake_file_ref("S6A_P4_2__LR_RED__NR_002_020_20231217.nc", collections_sorted[0].concept_id),
        ]
        unvalidated_refs = [
            _make_fake_file_ref("S6A_P4_2__LR_RED__NR_002_020_20231217_unval.nc", collections_sorted[1].concept_id),
            _make_fake_file_ref("S6A_P4_2__LR_RED__NR_003_030_20231217_unval.nc", collections_sorted[1].concept_id),
        ]

        def query_side_effect(concept_id, date):
            mock_query = MagicMock()
            if concept_id == collections_sorted[0].concept_id:
                mock_query.query.return_value = validated_refs
            elif concept_id == collections_sorted[1].concept_id:
                mock_query.query.return_value = unvalidated_refs
            else:
                mock_query.query.return_value = []
            return mock_query

        mock_cmr_query_cls.side_effect = query_side_effect

        enumerator = S6Enumerator(datetime(2023, 12, 17), source_config)
        file_refs = enumerator.enumerate()

        self.assertEqual(len(file_refs), 3)

        titles = [f.title for f in file_refs]
        self.assertIn("S6A_P4_2__LR_RED__NR_001_010_20231217.nc", titles)
        self.assertIn("S6A_P4_2__LR_RED__NR_002_020_20231217.nc", titles)
        self.assertNotIn("S6A_P4_2__LR_RED__NR_002_020_20231217_unval.nc", titles)
        self.assertIn("S6A_P4_2__LR_RED__NR_003_030_20231217_unval.nc", titles)

    @patch("daily_files.fetching.cmr_enumerator._CMRQuery")
    def test_empty_collections(self, mock_cmr_query_cls):
        from daily_files.fetching.cmr_enumerator import S6Enumerator

        mock_instance = MagicMock()
        mock_instance.query.return_value = []
        mock_cmr_query_cls.return_value = mock_instance

        source_config = get_source_config("S6")
        enumerator = S6Enumerator(datetime(2023, 12, 17), source_config)
        self.assertEqual(len(enumerator.enumerate()), 0)

    @patch("daily_files.fetching.cmr_enumerator._CMRQuery")
    def test_single_collection_only(self, mock_cmr_query_cls):
        from daily_files.fetching.cmr_enumerator import S6Enumerator

        source_config = get_source_config("S6")
        collections_sorted = sorted(source_config.collections, key=lambda c: c.priority)

        refs_p1 = [
            _make_fake_file_ref("S6A_P4_2__LR_RED__NR_001_010_20231217.nc", collections_sorted[0].concept_id),
            _make_fake_file_ref("S6A_P4_2__LR_RED__NR_002_020_20231217.nc", collections_sorted[0].concept_id),
        ]

        def query_side_effect(concept_id, date):
            mock_query = MagicMock()
            if concept_id == collections_sorted[0].concept_id:
                mock_query.query.return_value = refs_p1
            else:
                mock_query.query.return_value = []
            return mock_query

        mock_cmr_query_cls.side_effect = query_side_effect

        enumerator = S6Enumerator(datetime(2023, 12, 17), source_config)
        self.assertEqual(len(enumerator.enumerate()), 2)
