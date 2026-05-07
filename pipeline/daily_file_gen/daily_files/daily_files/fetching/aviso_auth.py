import logging
import netrc
import os

import requests

from utilities.aws_utils import aws_manager


_AVISO_HOST = "aviso.altimetry.fr"
_SECRET_NAME = "AVISO_auth"

_cached_session: requests.Session | None = None


def _resolve_credentials() -> tuple[str | None, str | None]:
    try:
        secret = aws_manager.get_secret(_SECRET_NAME)
        user = secret.get("user") or secret.get("username")
        pwd = secret.get("password")
        if user and pwd:
            return user, pwd
    except Exception as e:
        logging.debug(f"AVISO_auth secret unavailable: {e}")

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
    _cached_session = session
    return session
