import os
from dataclasses import dataclass, field
from datetime import date

import yaml


@dataclass
class CollectionConfig:
    concept_id: str
    priority: int = 1


@dataclass
class PipelineInitSourceConfig:
    source: str
    satellite: str
    start_date: date
    s3_prefix: str
    filename_pattern: str
    collections: list[CollectionConfig] = field(default_factory=list)


def _load_sources() -> dict[str, PipelineInitSourceConfig]:
    config_path = os.path.join(os.path.dirname(__file__), "sources.yaml")
    with open(config_path) as f:
        raw = yaml.safe_load(f)

    configs = {}
    for source_key, cfg in raw["sources"].items():
        collections = [CollectionConfig(**c) for c in cfg.get("collections", [])]

        start_date = cfg["start_date"]
        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)

        configs[source_key] = PipelineInitSourceConfig(
            source=source_key,
            satellite=cfg["satellite"],
            start_date=start_date,
            s3_prefix=cfg["s3_prefix"],
            filename_pattern=cfg["filename_pattern"],
            collections=collections,
        )
    return configs


_SOURCE_CONFIGS: dict[str, PipelineInitSourceConfig] = {}


def get_source_config(source: str) -> PipelineInitSourceConfig:
    global _SOURCE_CONFIGS
    if not _SOURCE_CONFIGS:
        _SOURCE_CONFIGS = _load_sources()
    if source not in _SOURCE_CONFIGS:
        raise ValueError(
            f"Source '{source}' is not configured. "
            f"Available sources: {list(_SOURCE_CONFIGS.keys())}"
        )
    return _SOURCE_CONFIGS[source]


def get_available_sources() -> list[str]:
    global _SOURCE_CONFIGS
    if not _SOURCE_CONFIGS:
        _SOURCE_CONFIGS = _load_sources()
    return list(_SOURCE_CONFIGS.keys())
