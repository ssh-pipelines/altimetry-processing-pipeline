import gzip
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from io import BytesIO
from typing import Callable

import requests
import s3fs

from utilities.aws_utils import aws_manager

# Transient errors worth retrying at the download level. The mounted urllib3
# adapter already retries connect/read/5xx *before* the response is returned;
# these cover the streamed body read + gzip.decompress phase, which happens
# after the 200 and so is invisible to the adapter.
_RETRYABLE = (requests.exceptions.RequestException, gzip.BadGzipFile, OSError)


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
        """Download every URI, failing closed on the first unrecoverable one.

        This intentionally does NOT skip failed granules and return a partial
        result: a partial daily file would be written with fewer granules than
        exist upstream, and planning (plan_jobs) only replans a date when an
        upstream granule is *newer* than the existing product — so the gap would
        never heal. Raising instead leaves no p1 for the date, which DOES replan
        on the next run. Per-granule transient errors are handled by the
        downloader's own retry (see HttpDownloader.download); only genuinely
        unrecoverable granules reach here and abort the job.
        """
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

    def __init__(
        self,
        session_fn: Callable[[], requests.Session],
        max_attempts: int = 5,
        backoff_base: float = 1.0,
        backoff_max: float = 30.0,
    ):
        self.session = session_fn()
        self.max_attempts = max_attempts
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max

    def _fetch(self, uri: str) -> BytesIO:
        with self.session.get(uri, stream=True, timeout=120) as resp:
            resp.raise_for_status()
            raw = resp.raw
            raw.decode_content = True
            if uri.endswith(".gz"):
                return BytesIO(gzip.decompress(raw.read()))
            return BytesIO(raw.read())

    def download(self, uri: str) -> BytesIO:
        """Download a single granule, retrying transient errors with backoff.

        Retries cover the streamed body read + decompress phase (see _RETRYABLE);
        the mounted adapter handles connect/read/5xx within each attempt. After
        max_attempts the last error is re-raised so the job fails closed.
        """
        logging.debug(f"Downloading {uri}")
        for attempt in range(self.max_attempts):
            try:
                return self._fetch(uri)
            except _RETRYABLE as e:
                if attempt + 1 >= self.max_attempts:
                    logging.exception(
                        f"Error downloading {uri} after {self.max_attempts} attempts"
                    )
                    raise
                delay = min(self.backoff_base * 2**attempt, self.backoff_max)
                logging.warning(
                    f"Transient error downloading {uri} "
                    f"(attempt {attempt + 1}/{self.max_attempts}): {e}. "
                    f"Retrying in {delay:.1f}s"
                )
                time.sleep(delay)
