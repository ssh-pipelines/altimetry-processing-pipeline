import base64
from datetime import datetime, timedelta
import logging
import time
from typing import Iterable
from cmr import GranuleQuery
from utilities.aws_utils import aws_manager
import requests


class S3NotFound(Exception):
    """Raise for S3 URL not available in CMR metadata exception"""


class CMRGranule:
    """
    Class for storing granule level metadata
    """

    def __init__(self, query_result: dict):
        self.granule_id: str = query_result.get("id")
        self.title: str = query_result.get("title")
        self.s3_url: str = self.extract_s3_url(query_result["links"])
        self.time_start: datetime = query_result.get("time_start")
        self.time_end: datetime = query_result.get("time_end")
        self.modified_time: datetime = query_result.get("updated")
        self.collection_id: str = query_result.get("collection_concept_id")

    def extract_s3_url(self, links: Iterable) -> str:
        for link in links:
            if (
                "rel" in link
                and link["rel"] == "http://esipfed.org/ns/fedsearch/1.1/s3#"
            ):
                return link["href"]
        raise S3NotFound()


class CMRQuery:
    """
    Class for querying CMR for granules for a given collection concept id and date
    """

    def __init__(self, concept_id: str, date: datetime):
        self.concept_id: str = concept_id
        self.start_date: datetime = date
        self.end_date: datetime = (
            self.start_date + timedelta(days=1) - timedelta(seconds=1)
        )
        self.token = self._get_edl_token()

    def _get_edl_token(self) -> str:
        edl_secret = aws_manager.get_secret("EDL_auth")
        username = edl_secret.get("user")
        password = edl_secret.get("password")
        encoded_auth = base64.b64encode(f"{username}:{password}".encode()).decode(
            "ascii"
        )

        resp = requests.post(
            "https://urs.earthdata.nasa.gov/api/users/find_or_create_token",
            headers={"Authorization": f"Basic {encoded_auth}"},
        )
        token = resp.json()["access_token"]
        return token

    def granule_query_with_wait(self):
        api = GranuleQuery()
        max_retries = 3
        attempt = 1
        while attempt <= max_retries:
            time.sleep(15)
            try:
                query_results = (
                    api.bearer_token(self.token)
                    .concept_id(self.concept_id)
                    .provider("POCLOUD")
                    .temporal(self.start_date, self.end_date)
                    .get_all()
                )
                return query_results
            except RuntimeError:
                attempt += 1
        logging.error("Unable to query CMR")
        raise RuntimeError("Unable to query CMR")

    def query(self) -> Iterable[CMRGranule]:
        api = GranuleQuery()
        try:
            query_results = (
                api.bearer_token(self.token)
                .concept_id(self.concept_id)
                .provider("POCLOUD")
                .temporal(self.start_date, self.end_date)
                .get_all()
            )
        except RuntimeError:
            query_results = self.granule_query_with_wait()

        cmr_granules = [CMRGranule(result) for result in query_results]
        logging.info(f"Found {len(cmr_granules)} granule(s) from CMR query")
        return cmr_granules
