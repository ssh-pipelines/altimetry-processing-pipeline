import logging
import re

import requests

from daily_files.fetching.enumerator import FileRef


class OrbitFetcher:
    POE_BASE_URL = "https://sideshow.jpl.nasa.gov/pub/usrs/sentinel6/jplpoeposgoa"
    MOE_BASE_URL = "https://sideshow.jpl.nasa.gov/pub/usrs/sentinel6/jplmoeposgoa"

    def __init__(self):
        self._cache: dict[str, str] = {}  # "{date}_{poe|moe}" -> local /tmp path

    def fetch(self, file_ref: FileRef) -> str | None:
        """Download the POE (NTC) or MOE (STC) orbit file for a granule.

        Returns the local /tmp path of the downloaded file, or None on failure.
        Files are cached by date+type so multiple passes on the same day reuse
        a single download.
        """
        date_str = self._extract_date(file_ref.title)
        is_ntc = "__NT_" in file_ref.title
        orbit_type = "poe" if is_ntc else "moe"
        cache_key = f"{date_str}_{orbit_type}"

        if cache_key in self._cache:
            logging.debug(f"Reusing cached orbit file for {cache_key}")
            return self._cache[cache_key]

        url, filename = self._build_url(date_str, is_ntc)
        local_path = f"/tmp/{filename}"

        try:
            logging.info(f"Downloading orbit file from {url}")
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            with open(local_path, "wb") as f:
                f.write(resp.content)
            self._cache[cache_key] = local_path
            logging.info(f"Orbit file saved to {local_path}")
            return local_path
        except Exception as e:
            logging.warning(f"Failed to download orbit file {url}: {e}")
            return None

    def _extract_date(self, title: str) -> str:
        """Extract YYYY-MM-DD from the pass start timestamp in a granule title.

        Granule titles contain a start datetime like '20260325T235012'.
        """
        match = re.search(r"(\d{4})(\d{2})(\d{2})T\d{6}", title)
        if not match:
            raise ValueError(f"Cannot extract date from granule title: {title}")
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"

    def _build_url(self, date_str: str, is_ntc: bool) -> tuple[str, str]:
        orbit_type = "jplpoe" if is_ntc else "jplmoe"
        base = self.POE_BASE_URL if is_ntc else self.MOE_BASE_URL
        filename = f"{date_str}_s6an_{orbit_type}.pos"
        return f"{base}/{filename}", filename
