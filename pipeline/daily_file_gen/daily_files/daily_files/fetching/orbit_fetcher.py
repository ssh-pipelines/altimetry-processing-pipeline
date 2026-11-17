import gzip
import logging
import re
import time
import zlib

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# (connect, read). Orbit files are ~1.7 MB gzipped. The adapter makes up to 5
# attempts with 0, 2, 4, 8 s (+ up to 1 s jitter) between them, so a host that
# stalls every read costs about 2.8 min before the job fails.
_TIMEOUT = (10, 30)

# The adapter returns once headers arrive, so a body cut off mid-transfer or a
# corrupt gzip is invisible to it and is retried here instead.
_BODY_ERRORS = (
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ContentDecodingError,
    gzip.BadGzipFile,
    EOFError,
    zlib.error,
)
_BODY_ATTEMPTS = 3


def _build_session() -> requests.Session:
    """Session that retries connect/read errors and 429/5xx with exponential backoff.

    404 is not retried: it means JPL has not published the orbit for that date
    yet, which no amount of waiting inside one invocation fixes.
    """
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=1.0,
        backoff_max=30,
        backoff_jitter=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


class OrbitFetcher:
    POE_BASE_URL = "https://sideshow.jpl.nasa.gov/pub/usrs/sentinel6/jplpoeposgoa"
    MOE_BASE_URL = "https://sideshow.jpl.nasa.gov/pub/usrs/sentinel6/jplmoeposgoa"

    def __init__(self):
        self._cache: dict[str, str] = {}  # "{date}_{poe|moe}" -> local /tmp path
        self._session = _build_session()

    def fetch(self, filename: str) -> str:
        """Download the POE (NTC) or MOE (STC) orbit file for a granule.

        Returns the local /tmp path of the downloaded file; download errors
        propagate once retries are spent. Files are cached by date+type so
        multiple passes on the same day reuse a single download.
        """
        date_str = self._extract_date(filename)
        is_ntc = "__NT_" in filename
        orbit_type = "poe" if is_ntc else "moe"
        cache_key = f"{date_str}_{orbit_type}"

        if cache_key in self._cache:
            logging.debug(f"Reusing cached orbit file for {cache_key}")
            return self._cache[cache_key]

        url, orbit_filename = self._build_url(date_str, is_ntc)
        local_path = f"/tmp/{orbit_filename}"

        for attempt in range(1, _BODY_ATTEMPTS + 1):
            logging.info(f"Downloading orbit file from {url}")
            resp = self._session.get(url, timeout=_TIMEOUT)
            resp.raise_for_status()
            try:
                content = gzip.decompress(resp.content)
                break
            except _BODY_ERRORS as e:
                if attempt == _BODY_ATTEMPTS:
                    raise
                delay = 2.0**attempt
                logging.warning(
                    f"Truncated orbit file from {url} (attempt {attempt}/{_BODY_ATTEMPTS}): "
                    f"{e}. Retrying in {delay:.0f}s"
                )
                time.sleep(delay)
        with open(local_path, "wb") as f:
            f.write(content)
        self._cache[cache_key] = local_path
        logging.info(f"Orbit file saved to {local_path}")
        return local_path

    def _extract_date(self, filename: str) -> str:
        """Extract YYYY-MM-DD from the pass start timestamp in a granule filename."""
        match = re.search(r"(\d{4})(\d{2})(\d{2})T\d{6}", filename)
        if not match:
            raise ValueError(f"Cannot extract date from granule filename: {filename}")
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"

    def _build_url(self, date_str: str, is_ntc: bool) -> tuple[str, str]:
        orbit_type = "jplpoe" if is_ntc else "jplmoe"
        base = self.POE_BASE_URL if is_ntc else self.MOE_BASE_URL
        filename = f"{date_str}_s6an_{orbit_type}.pos"
        return f"{base}/{filename}.gz", filename
