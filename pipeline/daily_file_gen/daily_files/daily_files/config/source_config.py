import os
from dataclasses import dataclass, field

import yaml


@dataclass
class SmoothingConfig:
    speed: float
    sigma: float


@dataclass
class CollectionConfig:
    shortname: str
    concept_id: str
    priority: int = 1
    source_label: str = ""
    source_url: str = ""
    reference: str = ""


@dataclass
class SourceConfig:
    source: str
    product_type: str
    filename_template: str
    s3_prefix: str
    source_mss: str
    target_mss: str
    mss_diff_file: str
    empty_template: str
    smoothing: SmoothingConfig
    collections: list[CollectionConfig] = field(default_factory=list)


def _load_sources() -> dict[str, SourceConfig]:
    config_path = os.path.join(os.path.dirname(__file__), "sources.yaml")
    with open(config_path) as f:
        raw = yaml.safe_load(f)

    configs = {}
    for source_key, cfg in raw["sources"].items():
        smoothing = SmoothingConfig(**cfg["smoothing"])

        collections = [CollectionConfig(**c) for c in cfg.get("collections", [])]

        configs[source_key] = SourceConfig(
            source=source_key,
            product_type=cfg["product_type"],
            filename_template=cfg["filename_template"],
            s3_prefix=cfg["s3_prefix"],
            source_mss=cfg["source_mss"],
            target_mss=cfg["target_mss"],
            mss_diff_file=cfg["mss_diff_file"],
            empty_template=cfg["empty_template"],
            smoothing=smoothing,
            collections=collections,
        )
    return configs


_SOURCE_CONFIGS: dict[str, SourceConfig] = {}


def get_source_config(source: str) -> SourceConfig:
    global _SOURCE_CONFIGS
    if not _SOURCE_CONFIGS:
        _SOURCE_CONFIGS = _load_sources()
    if source not in _SOURCE_CONFIGS:
        raise ValueError(
            f"Source '{source}' is not configured. Available sources: {list(_SOURCE_CONFIGS.keys())}"
        )
    return _SOURCE_CONFIGS[source]


def get_available_sources() -> list[str]:
    global _SOURCE_CONFIGS
    if not _SOURCE_CONFIGS:
        _SOURCE_CONFIGS = _load_sources()
    return list(_SOURCE_CONFIGS.keys())
