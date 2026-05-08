"""AVISO ODATIS THREDDS catalog crawler used as a pipeline_init Enumerator.

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
from datetime import date, datetime, timedelta, timezone

import requests

from config.source_config import CollectionConfig, PipelineInitSourceConfig
from enumeration.aviso_auth import build_aviso_session
from enumeration.base import GranuleRef


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
    data_start: datetime
    data_end: datetime
    processed_at: datetime
    download_url: str


def _to_utc(s: str) -> datetime | None:
    try:
        dt = datetime.strptime(s, "%Y%m%dT%H%M%S")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _parse_filename(name: str) -> dict | None:
    cp = _CYCLE_PASS_RE.search(name)
    pr = _PROCESSED_RE.search(name)
    if not cp or not pr:
        return None

    inner = name[cp.end():]
    parts = inner.split("_")
    if len(parts) < 3:
        return None

    data_start = _to_utc(parts[0])
    data_end = _to_utc(parts[1])
    processed_at = _to_utc(pr.group(1))
    if data_start is None or data_end is None or processed_at is None:
        return None

    return {
        "cycle": int(cp.group(1)),
        "pass_number": int(cp.group(2)),
        "data_start": data_start,
        "data_end": data_end,
        "processed_at": processed_at,
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

        granules.append(
            _Granule(
                granule_id=name,
                cycle=parsed["cycle"],
                pass_number=parsed["pass_number"],
                data_start=parsed["data_start"],
                data_end=parsed["data_end"],
                processed_at=parsed["processed_at"],
                download_url=_fileserver_url(url_path),
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


def _granule_dates(g: _Granule, range_start: date, range_end: date) -> list[date]:
    """Return the list of dates in [range_start, range_end] that this granule contributes to."""
    dates: list[date] = []
    g_start_day = g.data_start.date()
    g_end_day = g.data_end.date()
    cur = max(g_start_day, range_start)
    end = min(g_end_day, range_end)
    while cur <= end:
        dates.append(cur)
        cur = cur + timedelta(days=1)
    return dates


class ThreddsEnumerator:
    """Enumerates AVISO L2P granules across a date range. When multiple
    collections produce overlapping (cycle, pass) entries, the lower-numbered
    priority wins."""

    def __init__(self, source_config: PipelineInitSourceConfig):
        self.source_config = source_config

    def enumerate(self, start: date, end: date) -> list[GranuleRef]:
        session = build_aviso_session()

        # Per-collection priority resolution per (cycle, pass)
        winners: dict[tuple[int, int], tuple[int, _Granule, CollectionConfig]] = {}

        for coll in sorted(self.source_config.collections, key=lambda c: c.priority):
            if not coll.thredds_collection or not coll.thredds_version:
                logging.warning(
                    f"Skipping collection — missing thredds_collection/thredds_version"
                )
                continue

            logging.info(
                f"Crawling AVISO {coll.thredds_collection}/{coll.thredds_version} "
                f"for {start} to {end}"
            )
            granules = _crawl_collection(coll.thredds_collection, coll.thredds_version, session)
            in_range = [g for g in granules if g.data_end.date() >= start and g.data_start.date() <= end]
            logging.info(f"  {len(in_range)} granule(s) overlap [{start}, {end}]")

            for g in in_range:
                key = (g.cycle, g.pass_number)
                existing = winners.get(key)
                if existing is None or existing[0] > coll.priority:
                    winners[key] = (coll.priority, g, coll)

        refs: list[GranuleRef] = []
        for (cycle, pass_no), (_, g, _coll) in winners.items():
            for d in _granule_dates(g, start, end):
                refs.append(
                    GranuleRef(
                        date=d,
                        uri=g.download_url,
                        mod_time=g.processed_at,
                        sort_key=(cycle, pass_no),
                    )
                )

        refs.sort(key=lambda r: (r.date, r.sort_key, r.uri))
        logging.info(f"Total AVISO granule refs in [{start}, {end}]: {len(refs)}")
        return refs
