import os
from dataclasses import dataclass

import yaml


@dataclass
class UnifierSourceConfig:
    source: str
    src_filename_template: str
    dst_filename_template: str
    dst_prefix: str


def _load_sources() -> dict[str, UnifierSourceConfig]:
    config_path = os.path.join(os.path.dirname(__file__), "sources.yaml")
    with open(config_path) as f:
        raw = yaml.safe_load(f)

    configs = {}
    for source_key, cfg in raw["sources"].items():
        configs[source_key] = UnifierSourceConfig(
            source=source_key,
            src_filename_template=cfg["src_filename_template"],
            dst_filename_template=cfg["dst_filename_template"],
            dst_prefix=cfg["dst_prefix"],
        )
    return configs


_SOURCE_CONFIGS: dict[str, UnifierSourceConfig] = {}


def get_source_config(source: str) -> UnifierSourceConfig:
    global _SOURCE_CONFIGS
    if not _SOURCE_CONFIGS:
        _SOURCE_CONFIGS = _load_sources()
    if source not in _SOURCE_CONFIGS:
        raise ValueError(
            f"Source '{source}' is not configured for unification. "
            f"Available sources: {list(_SOURCE_CONFIGS.keys())}"
        )
    return _SOURCE_CONFIGS[source]


def get_available_sources() -> list[str]:
    global _SOURCE_CONFIGS
    if not _SOURCE_CONFIGS:
        _SOURCE_CONFIGS = _load_sources()
    return list(_SOURCE_CONFIGS.keys())
