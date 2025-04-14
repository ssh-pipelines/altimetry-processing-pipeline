from datetime import datetime
from io import TextIOWrapper
import logging
import s3fs
from typing import Iterable
from daily_files.fetching.cmr_query import CMRGranule, CMRQuery
from daily_files.fetching.fetcher import Fetcher
from utilities.aws_utils import aws_manager


class PodaacS3Creds:
    def __init__(self, username: str, password: str):
        self.edl_auth: str = f"{username}:{password}"
        self.current_pds3_auth: dict = aws_manager.get_secret("podaac_direct_s3_auth")
        self.creds = self.get_creds()

    def get_creds(self):
        """
        Retrieve temporary Podaac S3 credentials. If credentials are outdated, need to run credential update Lambda
        which is intentionally handled external to this code in order to avoid race conditions.
        """
        curr_expiration = datetime.strptime(
            self.current_pds3_auth["expiration"], "%Y-%m-%d %H:%M:%S+00:00"
        )
        if curr_expiration < datetime.now():
            raise RuntimeError(
                f"Podaac creds expire at {curr_expiration} which is less than {datetime.now()}. Need to obtain new credentials..."
            )
        return self.current_pds3_auth


class PodaacS3Fetcher(Fetcher):
    """
    Requires obtaining temporary podaac s3 creds
    """

    granules: Iterable[CMRGranule]

    def __init__(self):
        edl_secret = aws_manager.get_secret("EDL_auth")
        self.ed_user = edl_secret.get("user")
        self.ed_pass = edl_secret.get("password")
        self.s3 = self.setup_s3()

    def cmr_query(self, concept_id: str, date: datetime) -> Iterable[CMRGranule]:
        return CMRQuery(concept_id, date).query()

    def setup_s3(self) -> s3fs.S3FileSystem:
        creds = PodaacS3Creds(self.ed_user, self.ed_pass).creds
        s3 = s3fs.S3FileSystem(
            anon=False,
            key=creds["accessKeyId"],
            secret=creds["secretAccessKey"],
            token=creds["sessionToken"],
        )
        return s3

    def fetch(self, src: str) -> TextIOWrapper:
        try:
            logging.debug(f"Loading {src} into memory")
            opened_s3 = self.s3.open(src)
        except Exception as e:
            logging.exception(f"Error opening {src}")
            raise e
        return opened_s3
