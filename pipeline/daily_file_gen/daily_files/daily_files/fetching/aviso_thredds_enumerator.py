"""
AVISO ODATIS THREDDS catalog crawler used as a daily-files Enumerator.

Layout of the AVISO catalog:
    <base>/<collection>/<version>/<cycle_NNNN>/catalog.xml
                                               └── pass files (.nc.gz)

Filename convention encodes both the data window and the processing date:
    global_sla_l2p_ntc_e1_C0050_P0003_19960115T000000_19960115T003519_20240305T133008.nc.gz
                                      ^cycle ^pass  ^data_start         ^data_end           ^processed
"""

from __future__ import annotations

import logging
import re
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import requests

from daily_files.config.source_config import CollectionConfig
from daily_files.fetching.aviso_auth import build_aviso_session
from daily_files.fetching.enumerator import Enumerator, FileRef


TDS_BASE = "https://tds-odatis.aviso.altimetry.fr/thredds/"

NS = {
    "cat": "http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0",
    "xlink": "http://www.w3.org/1999/xlink",
}

_PROCESSED_RE = re.compile(r"_(\d{8}T\d{6})\.nc(?:\.gz)?$")
_CYCLE_PASS_RE = re.compile(r"_C(\d+)_P(\d+)_")

_DEFAULT_MAX_WORKERS = 16


@dataclass
class _Granule:
    granule_id: str
    cycle: int
    pass_number: int
    data_start: str  # ISO-8601
    data_end: str  # ISO-8601
    processed_at: str  # ISO-8601
    download_url: str
    size_bytes: int | None


def _parse_filename(name: str) -> dict | None:
    cp = _CYCLE_PASS_RE.search(name)
    pr = _PROCESSED_RE.search(name)
    if not cp or not pr:
        return None

    inner = name[cp.end():]
    parts = inner.split("_")
    if len(parts) < 3:
        return None

    def _to_iso(s: str) -> str:
        try:
            dt = datetime.strptime(s, "%Y%m%dT%H%M%S")
            return dt.replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            return s

    return {
        "cycle": int(cp.group(1)),
        "pass_number": int(cp.group(2)),
        "data_start": _to_iso(parts[0]),
        "data_end": _to_iso(parts[1]),
        "processed_at": _to_iso(pr.group(1)),
    }


def _collection_catalog_url(collection: str, version: str) -> str:
    return f"{TDS_BASE}catalog/{collection}/{version}/catalog.xml"


def _cycle_catalog_url(collection: str, version: str, cycle: int) -> str:
    return f"{TDS_BASE}catalog/{collection}/{version}/cycle_{cycle:04d}/catalog.xml"


def _fileserver_url(url_path: str) -> str:
    return f"{TDS_BASE}fileServer/{url_path}"


def _fetch_xml(url: str, session: requests.Session, retries: int = 3) -> ET.Element | None:
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 404:
                logging.debug(f"404: {url}")
                return None
            resp.raise_for_status()
            return ET.fromstring(resp.content)
        except requests.HTTPError as exc:
            logging.warning(f"HTTP {exc.response.status_code} — {url}")
            return None
        except (requests.RequestException, ET.ParseError) as exc:
            wait = 2 ** attempt
            if attempt < retries - 1:
                logging.warning(f"Attempt {attempt + 1} failed ({exc}), retrying in {wait}s")
                time.sleep(wait)
            else:
                logging.error(f"Giving up on {url}: {exc}")
                return None
    return None


def _list_cycles(collection: str, version: str, session: requests.Session) -> list[int]:
    url = _collection_catalog_url(collection, version)
    root = _fetch_xml(url, session)
    if root is None:
        raise RuntimeError(f"Could not fetch collection catalog: {url}")

    cycles: list[int] = []
    for ref in root.findall(".//cat:catalogRef", NS):
        href = ref.get("{http://www.w3.org/1999/xlink}href", "")
        m = re.match(r"cycle_(\d+)/catalog\.xml", href, re.IGNORECASE)
        if m:
            cycles.append(int(m.group(1)))

    cycles.sort()
    return cycles


def _parse_cycle_catalog(
    collection: str,
    version: str,
    cycle: int,
    session: requests.Session,
) -> list[_Granule]:
    url = _cycle_catalog_url(collection, version, cycle)
    root = _fetch_xml(url, session)
    if root is None:
        return []

    granules: list[_Granule] = []
    for ds in root.findall(".//cat:dataset[@urlPath]", NS):
        name = ds.get("name", "")
        url_path = ds.get("urlPath", "")
        if not name or not url_path:
            continue
        if not (name.endswith(".nc") or name.endswith(".nc.gz")):
            continue

        parsed = _parse_filename(name)
        if parsed is None:
            continue

        size: int | None = None
        size_el = ds.find("cat:dataSize", NS)
        if size_el is not None and size_el.text:
            try:
                val = float(size_el.text)
                units = (size_el.get("units") or "bytes").strip().lower()
                mult = {"bytes": 1, "kbytes": 1024, "mbytes": 1024 ** 2, "gbytes": 1024 ** 3}
                size = int(val * mult.get(units, 1))
            except ValueError:
                pass

        granules.append(
            _Granule(
                granule_id=name,
                cycle=parsed["cycle"],
                pass_number=parsed["pass_number"],
                data_start=parsed["data_start"],
                data_end=parsed["data_end"],
                processed_at=parsed["processed_at"],
                download_url=_fileserver_url(url_path),
                size_bytes=size,
            )
        )

    return granules


def _crawl_collection(
    collection: str,
    version: str,
    session: requests.Session,
    max_workers: int = _DEFAULT_MAX_WORKERS,
) -> list[_Granule]:
    cycles = _list_cycles(collection, version, session)
    logging.info(f"AVISO {collection}/{version}: {len(cycles)} cycles to crawl")

    all_granules: list[_Granule] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_parse_cycle_catalog, collection, version, c, session): c
            for c in cycles
        }
        for future in as_completed(futures):
            cycle = futures[future]
            try:
                all_granules.extend(future.result())
            except Exception as exc:
                logging.error(f"Cycle {cycle:04d} failed: {exc}")

    return all_granules


def _overlaps_day(granule: _Granule, day_start: datetime, day_end: datetime) -> bool:
    """A granule overlaps the target day if its data window intersects [day_start, day_end)."""
    g_start = datetime.fromisoformat(granule.data_start)
    g_end = datetime.fromisoformat(granule.data_end)
    return g_start < day_end and g_end >= day_start


class ThreddsEnumerator(Enumerator):
    """Enumerates AVISO L2P granules for a single date across one or more
    AVISO collections. When multiple collections produce overlapping (cycle,
    pass) entries, the lower-numbered priority wins (mirrors S6Enumerator).
    """

    def enumerate(self) -> list[FileRef]:
        session = build_aviso_session()

        day_start = self.date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        day_end = day_start + timedelta(days=1)

        priority_granules: dict[tuple[int, int], tuple[int, _Granule, CollectionConfig]] = {}

        for coll in sorted(self.source_config.collections, key=lambda c: c.priority):
            if not coll.thredds_collection or not coll.thredds_version:
                logging.warning(
                    f"Skipping collection '{coll.shortname}' — missing thredds_collection/thredds_version"
                )
                continue

            logging.info(
                f"Crawling AVISO {coll.thredds_collection}/{coll.thredds_version} for {self.date.date()}"
            )
            granules = _crawl_collection(coll.thredds_collection, coll.thredds_version, session)

            day_granules = [g for g in granules if _overlaps_day(g, day_start, day_end)]
            logging.info(f"  {len(day_granules)} granule(s) overlap {self.date.date()}")

            for g in day_granules:
                key = (g.cycle, g.pass_number)
                existing = priority_granules.get(key)
                if existing is None or existing[0] > coll.priority:
                    priority_granules[key] = (coll.priority, g, coll)

        file_refs: list[FileRef] = []
        for _, (_, g, coll) in sorted(priority_granules.items()):
            file_refs.append(
                FileRef(
                    id=g.granule_id,
                    title=g.granule_id,
                    access_url=g.download_url,
                    time_start=g.data_start,
                    time_end=g.data_end,
                    modified_time=g.processed_at,
                    collection_id=coll.thredds_collection or "",
                )
            )

        logging.info(f"Total AVISO granules selected for {self.date.date()}: {len(file_refs)}")
        return file_refs
