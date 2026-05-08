import unittest
from datetime import date, datetime
from unittest.mock import patch

from config.source_config import get_source_config
from enumeration.cmr import CMREnumerator


def _granule(
    title: str,
    concept_id: str,
    updated: str,
    s3_url: str,
    time_start: str = "2023-12-17T00:00:00Z",
    time_end: str = "2023-12-17T23:59:59Z",
):
    return {
        "id": f"id-{title}",
        "title": title,
        "updated": updated,
        "time_start": time_start,
        "time_end": time_end,
        "collection_concept_id": concept_id,
        "links": [
            {"rel": "http://esipfed.org/ns/fedsearch/1.1/s3#", "href": s3_url},
        ],
    }


class TestCMREnumeratorMultiCollection(unittest.TestCase):
    @patch("enumeration.cmr.GranuleQuery")
    def test_priority_resolution_picks_lower_priority_number(self, mock_query_cls):
        cfg = get_source_config("S6")
        cs = sorted(cfg.collections, key=lambda c: c.priority)

        # Two granules with same (cycle, pass) — different collections
        granules = [
            _granule(
                "S6A_P4_2__LR_RED__NR_001_010_x.nc",
                cs[0].concept_id,
                "2023-12-17T12:00:00",
                "s3://b/a.nc",
            ),
            _granule(
                "S6A_P4_2__LR_RED__NR_001_010_y.nc",
                cs[1].concept_id,
                "2023-12-17T13:00:00",
                "s3://b/b.nc",
            ),
        ]
        mock_api = mock_query_cls.return_value
        mock_api.concept_id.return_value = mock_api
        mock_api.provider.return_value = mock_api
        mock_api.temporal.return_value = mock_api
        mock_api.get_all.return_value = granules

        refs = CMREnumerator(cfg).enumerate(date(2023, 12, 17), date(2023, 12, 17))

        self.assertEqual(len(refs), 1)
        self.assertEqual(refs[0].uri, "s3://b/a.nc")  # priority-1 winner
        self.assertEqual(refs[0].sort_key, (1, 10))

    @patch("enumeration.cmr.GranuleQuery")
    def test_distinct_cycle_pass_both_kept(self, mock_query_cls):
        cfg = get_source_config("S6")
        cs = sorted(cfg.collections, key=lambda c: c.priority)

        granules = [
            _granule("S6A_NR_001_010_x.nc", cs[0].concept_id, "2023-12-17T12:00:00", "s3://b/a.nc"),
            _granule("S6A_NR_002_020_y.nc", cs[1].concept_id, "2023-12-17T13:00:00", "s3://b/b.nc"),
        ]
        mock_api = mock_query_cls.return_value
        mock_api.concept_id.return_value = mock_api
        mock_api.provider.return_value = mock_api
        mock_api.temporal.return_value = mock_api
        mock_api.get_all.return_value = granules

        refs = CMREnumerator(cfg).enumerate(date(2023, 12, 17), date(2023, 12, 17))
        sort_keys = [r.sort_key for r in refs]
        self.assertEqual(sort_keys, [(1, 10), (2, 20)])  # deterministic sort


class TestCMREnumeratorSingleCollection(unittest.TestCase):
    @patch("enumeration.cmr.GranuleQuery")
    def test_single_collection_emits_one_ref_per_granule(self, mock_query_cls):
        cfg = get_source_config("GSFC")
        cs = cfg.collections[0]

        granules = [
            _granule("GSFC_001.nc", cs.concept_id, "2023-12-17T01:00:00", "s3://b/g1.nc"),
            _granule("GSFC_002.nc", cs.concept_id, "2023-12-17T02:00:00", "s3://b/g2.nc"),
        ]
        mock_api = mock_query_cls.return_value
        mock_api.concept_id.return_value = mock_api
        mock_api.provider.return_value = mock_api
        mock_api.temporal.return_value = mock_api
        mock_api.get_all.return_value = granules

        refs = CMREnumerator(cfg).enumerate(date(2023, 12, 17), date(2023, 12, 17))
        self.assertEqual(len(refs), 2)
        uris = {r.uri for r in refs}
        self.assertEqual(uris, {"s3://b/g1.nc", "s3://b/g2.nc"})
        self.assertEqual(refs[0].mod_time, datetime(2023, 12, 17, 1, 0, 0))


class TestCMREnumeratorEmpty(unittest.TestCase):
    def test_empty_concept_ids_raises(self):
        from dataclasses import replace
        from utilities.source_profile import CollectionConfig

        cfg = get_source_config("GSFC")
        # build a copy with empty concept_ids
        empty_collections = [replace(c, concept_id="") for c in cfg.collections]
        cfg = replace(cfg, collections=empty_collections)
        with self.assertRaises(ValueError):
            CMREnumerator(cfg).enumerate(date(2023, 1, 1), date(2023, 1, 1))
