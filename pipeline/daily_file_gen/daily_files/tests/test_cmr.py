from typing import Iterable
import boto3
import os
import unittest
from unittest import mock
from datetime import datetime
from daily_files.fetching.cmr_query import CMRQuery, CMRGranule


class EndToEndCMRQueryTestCase(unittest.TestCase):
    granules: Iterable[CMRGranule]
    concept_id: str

    @classmethod
    def setUpClass(cls) -> None:
        gsfc_concept_id = "C2901523432-POCLOUD"
        date = datetime(2023, 1, 1)

        session = boto3.Session(profile_name="s6")
        credentials = session.get_credentials()
        aws_env_vars = {
            "AWS_ACCESS_KEY_ID": credentials.access_key,
            "AWS_SECRET_ACCESS_KEY": credentials.secret_key,
            "AWS_SESSION_TOKEN": credentials.token,
        }

        with mock.patch.dict(os.environ, aws_env_vars):
            cmr_query = CMRQuery(gsfc_concept_id, date)

        cls.concept_id = gsfc_concept_id
        cls.date = date
        cls.granules = cmr_query.query()

    def test_number_hits(self):
        self.assertEqual(2, len(self.granules))

    def test_correct_collection(self):
        for granule in self.granules:
            self.assertEqual(self.concept_id, granule.collection_id)

    def test_s3_url(self):
        for granule in self.granules:
            self.assertTrue(granule.s3_url.startswith("s3://"))
