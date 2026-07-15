import logging
import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Iterable

from cmr import GranuleQuery
from config.source_config import PipelineInitSourceConfig

from enumeration.base import GranuleRef

_CYCLE_PASS_RE = re.compile(r"_(\d{3})_(\d{3})_")


def _extract_s3_url(links: Iterable[dict]) -> str | None:
    for link in links or []:
        if link.get("rel") == "http://esipfed.org/ns/fedsearch/1.1/s3#":
            return link.get("href")
    return None


def _extract_https_url(links: Iterable[dict]) -> str | None:
    for link in links or []:
        rel = link.get("rel", "")
        href = link.get("href", "")
        if rel.endswith("/data#") and href.startswith("http"):
            return href
    return None


def _granule_uri(granule: dict) -> str | None:
    return _extract_s3_url(granule.get("links")) or _extract_https_url(granule.get("links"))


def _parse_cycle_pass(title: str) -> tuple[int, int] | None:
    m = _CYCLE_PASS_RE.search(title or "")
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


class CMREnumerator:
    """Enumerates CMR granules across a date range. Handles both single
    collections and multi-collection priority resolution per (cycle, pass)."""

    def __init__(self, source_config: PipelineInitSourceConfig):
        if not source_config.collections:
            raise ValueError(
                f"CMREnumerator requires at least one collection for source "
                f"'{source_config.source}'"
            )
        self.source_config = source_config

    def enumerate(self, start: date, end: date) -> list[GranuleRef]:
        concept_ids = [c.concept_id for c in self.source_config.collections if c.concept_id]
        if not concept_ids:
            raise ValueError(
                f"CMREnumerator: no concept_ids configured for source "
                f"'{self.source_config.source}'"
            )

        start_dt = datetime.combine(start, datetime.min.time())
        end_dt = datetime.combine(end, datetime.max.time().replace(microsecond=0))

        logging.info(
            f"Querying CMR for {self.source_config.source} granules from "
            f"{start} to {end} across {len(concept_ids)} collection(s)"
        )
        api = (
            GranuleQuery()
            .concept_id(concept_ids)
            .provider("POCLOUD")
            .temporal(start_dt, end_dt)
        )
        results = api.get_all()

        # Group by date the granule contributes to.
        granules_by_date: dict[date, list[dict]] = defaultdict(list)
        for granule in results:
            t_start = granule.get("time_start")
            t_end = granule.get("time_end")
            if not t_start or not t_end:
                continue
            g_start = datetime.fromisoformat(t_start.replace("Z", ""))
            g_end = datetime.fromisoformat(t_end.replace("Z", ""))

            for d_offset in range((end - start).days + 1):
                d = start + timedelta(days=d_offset)
                day_start = datetime.combine(d, datetime.min.time())
                day_end = day_start + timedelta(days=1)
                if g_end > day_start and g_start < day_end:
                    granules_by_date[d].append(granule)

        priority_map = {c.concept_id: c.priority for c in self.source_config.collections if c.concept_id}
        use_priority = len(self.source_config.collections) > 1

        refs: list[GranuleRef] = []
        for d, granules in granules_by_date.items():
            if use_priority:
                refs.extend(self._select_priority_winners(d, granules, priority_map))
            else:
                refs.extend(self._all_granules(d, granules))

        refs.sort(key=lambda g: (g.date, g.sort_key, g.uri))
        return refs

    def _select_priority_winners(
        self,
        d: date,
        granules: list[dict],
        priority_map: dict[str, int],
    ) -> list[GranuleRef]:
        """For multi-collection sources, pick the lower-priority-number granule
        per (cycle, pass)."""
        winners: dict[tuple[int, int], tuple[int, dict]] = {}
        for granule in granules:
            cp = _parse_cycle_pass(granule.get("title", ""))
            if cp is None:
                continue
            concept_id = granule.get("collection_concept_id")
            priority = priority_map.get(concept_id, 100)
            existing = winners.get(cp)
            if existing is None or existing[0] > priority:
                winners[cp] = (priority, granule)

        refs: list[GranuleRef] = []
        for cp in sorted(winners):
            _, granule = winners[cp]
            ref = self._make_ref(d, granule, cp)
            if ref is not None:
                refs.append(ref)
        return refs

    def _all_granules(self, d: date, granules: list[dict]) -> list[GranuleRef]:
        refs: list[GranuleRef] = []
        for granule in granules:
            cp = _parse_cycle_pass(granule.get("title", "")) or ()
            ref = self._make_ref(d, granule, cp)
            if ref is not None:
                refs.append(ref)
        return refs

    def _make_ref(self, d: date, granule: dict, sort_key: tuple) -> GranuleRef | None:
        uri = _granule_uri(granule)
        updated = granule.get("updated")
        if not uri or not updated:
            logging.warning(
                f"Skipping granule {granule.get('title')!r}: missing uri or mod_time"
            )
            return None
        return GranuleRef(
            date=d,
            uri=uri,
            mod_time=datetime.fromisoformat(updated.replace("Z", "")),
            sort_key=sort_key,
        )
