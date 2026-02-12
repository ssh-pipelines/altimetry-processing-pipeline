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


def _load_registry() -> dict[str, SourceRegistryEntry]:
    config_path = os.path.join(os.path.dirname(__file__), "sources.yaml")
    with open(config_path) as f:
        raw = yaml.safe_load(f)

    entries = {}
    for source_key, cfg in raw["sources"].items():
        start_date = cfg["start_date"]
        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)

        entries[source_key] = SourceRegistryEntry(
            source=source_key,
            product_type=cfg["product_type"],
            unify=cfg.get("unify", False),
            start_date=start_date,
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


def get_registered_sources() -> list[str]:
    global _REGISTRY
    if not _REGISTRY:
        _REGISTRY = _load_registry()
    return list(_REGISTRY.keys())
