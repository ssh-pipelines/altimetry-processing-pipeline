import base64
from datetime import datetime, timedelta
import logging
import re
import time
from typing import Iterable

from cmr import GranuleQuery
import requests

from daily_files.fetching.enumerator import Enumerator, FileRef
from utilities.aws_utils import aws_manager


class S3NotFound(Exception):
    """Raise for S3 URL not available in CMR metadata exception"""


def _extract_s3_url(links: Iterable) -> str:
    for link in links:
        if "rel" in link and link["rel"] == "http://esipfed.org/ns/fedsearch/1.1/s3#":
            return link["href"]
    raise S3NotFound()


def _make_file_ref(query_result: dict) -> FileRef:
    return FileRef(
        id=query_result.get("id"),
        title=query_result.get("title"),
        access_url=_extract_s3_url(query_result["links"]),
        time_start=query_result.get("time_start"),
        time_end=query_result.get("time_end"),
        modified_time=query_result.get("updated"),
        collection_id=query_result.get("collection_concept_id"),
    )


_cached_edl_token: str | None = None


def _get_edl_token() -> str:
    global _cached_edl_token
    if _cached_edl_token is not None:
        return _cached_edl_token
    edl_secret = aws_manager.get_secret("EDL_auth")
    username = edl_secret.get("user")
    password = edl_secret.get("password")
    encoded_auth = base64.b64encode(f"{username}:{password}".encode()).decode("ascii")

    resp = requests.post(
        "https://urs.earthdata.nasa.gov/api/users/find_or_create_token",
        headers={"Authorization": f"Basic {encoded_auth}"},
    )
    _cached_edl_token = resp.json()["access_token"]
    return _cached_edl_token


class _CMRQuery:
    """Queries CMR for granules for a given collection concept id and date."""

    def __init__(self, concept_id: str, date: datetime):
        self.concept_id: str = concept_id
        self.start_date: datetime = date
        self.end_date: datetime = (
            self.start_date + timedelta(days=1) - timedelta(seconds=1)
        )
        self.token = _get_edl_token()

    def _granule_query_with_wait(self):
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

    def query(self) -> list[FileRef]:
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
            query_results = self._granule_query_with_wait()

        file_refs = [_make_file_ref(result) for result in query_results]
        logging.info(f"Found {len(file_refs)} granule(s) from CMR query")
        return file_refs


class CMREnumerator(Enumerator):
    """Base enumerator that provides a CMR query helper."""

    def cmr_query(self, concept_id: str, date: datetime) -> list[FileRef]:
        return _CMRQuery(concept_id, date).query()


class GSFCEnumerator(CMREnumerator):
    def enumerate(self) -> list[FileRef]:
        concept_id = self.source_config.collections[0].concept_id
        return self.cmr_query(concept_id, self.date)


class S6Enumerator(CMREnumerator):
    """Enumerates Sentinel-6 granules across multiple collections,
    selecting the highest-priority granule per cycle_pass combination."""

    def enumerate(self) -> list[FileRef]:
        cycle_pass_pattern = r"_\d{3}_\d{3}_"
        priority_granules: dict[str, tuple[int, FileRef]] = {}

        for collection in sorted(
            self.source_config.collections, key=lambda c: c.priority
        ):
            logging.info(f"Querying for collection {collection.shortname}")
            granules = self.cmr_query(collection.concept_id, self.date)
            for granule in granules:
                cycle_pass = re.search(cycle_pass_pattern, granule.title).group(0)[1:-1]
                queue_status = priority_granules.get(cycle_pass, (100, None))
                if queue_status[0] > collection.priority:
                    priority_granules[cycle_pass] = (collection.priority, granule)

        return [granule for _, (_, granule) in sorted(priority_granules.items())]
