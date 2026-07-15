import gzip
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from io import BytesIO
from typing import Callable

import requests
import s3fs

from utilities.aws_utils import aws_manager


def get_podaac_s3_credentials() -> dict:
    """Retrieve temporary PODAAC S3 credentials via EDL auth stored in Secrets Manager."""
    current_auth: dict = aws_manager.get_secret("podaac_direct_s3_auth")
    expiration = datetime.strptime(
        current_auth["expiration"], "%Y-%m-%d %H:%M:%S+00:00"
    )
    if expiration < datetime.now():
        raise RuntimeError(
            f"Podaac creds expire at {expiration} which is less than {datetime.now()}. "
            "Need to obtain new credentials..."
        )
    return current_auth


class Downloader(ABC):
    @abstractmethod
    def download(self, uri: str):
        ...

    def download_all(self, uris: list[str]) -> list:
        return [self.download(uri) for uri in uris]


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

    def download(self, uri: str):
        try:
            logging.debug(f"Loading {uri} into memory")
            return self.s3.open(uri)
        except Exception as e:
            logging.exception(f"Error opening {uri}")
            raise e


class HttpDownloader(Downloader):
    """Downloads granules over HTTP, transparently decompressing .nc.gz to .nc.

    Returns a BytesIO containing raw NetCDF bytes that downstream ingestors can
    pass directly to xr.open_dataset / netCDF4.Dataset(memory=...).
    """

    def __init__(self, session_fn: Callable[[], requests.Session]):
        self.session = session_fn()

    def download(self, uri: str) -> BytesIO:
        try:
            logging.debug(f"Downloading {uri}")
            with self.session.get(uri, stream=True, timeout=120) as resp:
                resp.raise_for_status()
                raw = resp.raw
                raw.decode_content = True
                if uri.endswith(".gz"):
                    return BytesIO(gzip.decompress(raw.read()))
                return BytesIO(raw.read())
        except Exception as e:
            logging.exception(f"Error downloading {uri}")
            raise e
