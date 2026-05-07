import os
from dataclasses import dataclass
from datetime import date

import yaml


@dataclass
class SourceRegistryEntry:
    source: str
    product_type: str
    unify: bool
    start_date: date
    end_date: date | None = None
    discovery_type: str = "cmr"


def _load_registry() -> dict[str, SourceRegistryEntry]:
    config_path = os.path.join(os.path.dirname(__file__), "sources.yaml")
    with open(config_path) as f:
        raw = yaml.safe_load(f)

    entries = {}
    for source_key, cfg in raw["sources"].items():
        start_date = cfg["start_date"]
        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)

        end_date = cfg.get("end_date")
        if isinstance(end_date, str):
            end_date = date.fromisoformat(end_date)

        entries[source_key] = SourceRegistryEntry(
            source=source_key,
            product_type=cfg["product_type"],
            unify=cfg.get("unify", False),
            start_date=start_date,
            end_date=end_date,
            discovery_type=cfg.get("discovery_type", "cmr"),
        )
    return entries


_REGISTRY: dict[str, SourceRegistryEntry] = {}


def get_source_entry(source: str) -> SourceRegistryEntry:
    global _REGISTRY
    if not _REGISTRY:
        _REGISTRY = _load_registry()
    if source not in _REGISTRY:
        raise ValueError(
            f"Source '{source}' is not in the shared registry. "
            f"Available sources: {list(_REGISTRY.keys())}"
        )
    return _REGISTRY[source]


def daily_filename_prefix(source: str) -> str:
    """Return the filename prefix segment for a source's daily files.

    reference     → '{source}_alt_ref_at_v1_1'
    high_latitude → '{source}_alt_hilat_at_v1_1'
    """
    entry = get_source_entry(source)
    if entry.product_type == "reference":
        return f"{source}_alt_ref_at_v1_1"
    if entry.product_type == "high_latitude":
        return f"{source}_alt_hilat_at_v1_1"
    raise ValueError(
        f"Unknown product_type '{entry.product_type}' for source '{source}'."
    )


def get_registered_sources() -> list[str]:
    global _REGISTRY
    if not _REGISTRY:
        _REGISTRY = _load_registry()
    return list(_REGISTRY.keys())
