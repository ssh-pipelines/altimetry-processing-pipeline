from abc import ABC, abstractmethod
from datetime import datetime
from io import TextIOWrapper
import logging
from typing import Callable

import s3fs

from daily_files.fetching.enumerator import FileRef
from utilities.aws_utils import aws_manager


def get_podaac_s3_credentials() -> dict:
    """Retrieve temporary PODAAC S3 credentials via EDL auth stored in Secrets Manager."""
    current_auth: dict = aws_manager.get_secret("podaac_direct_s3_auth")
    expiration = datetime.strptime(current_auth["expiration"], "%Y-%m-%d %H:%M:%S+00:00")
    if expiration < datetime.now():
        raise RuntimeError(
            f"Podaac creds expire at {expiration} which is less than {datetime.now()}. "
            "Need to obtain new credentials..."
        )
    return current_auth


class Downloader(ABC):
    @abstractmethod
    def download(self, access_url: str) -> TextIOWrapper:
        pass

    def download_all(self, file_refs: list[FileRef]) -> list:
        return [self.download(f.access_url) for f in file_refs]


class S3Downloader(Downloader):
    def __init__(self, credentials_fn: Callable[[], dict] | None = None):
        if credentials_fn is not None:
            creds = credentials_fn()
            self.s3 = s3fs.S3FileSystem(
                anon=False,
                key=creds["accessKeyId"],
                secret=creds["secretAccessKey"],
                token=creds["sessionToken"],
            )
        else:
            self.s3 = s3fs.S3FileSystem(anon=False)

    def download(self, access_url: str) -> TextIOWrapper:
        try:
            logging.debug(f"Loading {access_url} into memory")
            return self.s3.open(access_url)
        except Exception as e:
            logging.exception(f"Error opening {access_url}")
            raise e


class HttpDownloader(Downloader):
    def __init__(self, auth: dict | None = None):
        self.auth = auth

    def download(self, access_url: str) -> TextIOWrapper:
        raise NotImplementedError("HttpDownloader is a placeholder for future use.")
