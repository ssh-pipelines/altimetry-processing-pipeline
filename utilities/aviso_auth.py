import logging
import netrc
import os

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utilities.aws_utils import aws_manager

_AVISO_HOST = "aviso.altimetry.fr"
_SECRET_NAME = "AVISO_auth"

_cached_session: requests.Session | None = None


def _build_retry_adapter() -> HTTPAdapter:
    """HTTPAdapter that retries transient connect/read errors and 5xx/429 responses
    with exponential backoff + jitter.

    AVISO's THREDDS server intermittently drops connections (ConnectTimeout) and
    returns transient 5xx. Retrying at the adapter level means both the granule
    downloader and the catalog enumerator inherit recovery via the shared session.
    raise_on_status=False so callers' own resp.raise_for_status() governs the
    final (non-retryable) status.
    """
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.0,
        backoff_max=60,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD", "OPTIONS"}),
        raise_on_status=False,
    )
    return HTTPAdapter(max_retries=retry)


def _resolve_credentials() -> tuple[str | None, str | None]:
    try:
        secret = aws_manager.get_secret(_SECRET_NAME)
        user = secret.get("user") or secret.get("username")
        pwd = secret.get("password")
        if user and pwd:
            return user, pwd
    except Exception as e:
        logging.warning(f"AVISO_auth secret unavailable: {e}")

    user = os.environ.get("AVISO_USER")
    pwd = os.environ.get("AVISO_PASS")
    if user and pwd:
        return user, pwd

    try:
        creds = netrc.netrc().authenticators(_AVISO_HOST)
        if creds:
            return creds[0], creds[2]
    except (FileNotFoundError, netrc.NetrcParseError):
        pass

    return None, None


def build_aviso_session() -> requests.Session:
    global _cached_session
    if _cached_session is not None:
        return _cached_session

    user, pwd = _resolve_credentials()
    session = requests.Session()
    if user and pwd:
        session.auth = (user, pwd)
    else:
        logging.warning("No AVISO credentials resolved — requests will likely fail.")

    session.headers.update({"Accept-Encoding": "gzip, deflate"})

    adapter = _build_retry_adapter()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    _cached_session = session
    return session
