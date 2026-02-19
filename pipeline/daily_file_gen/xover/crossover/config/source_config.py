import os
from dataclasses import dataclass

import yaml


@dataclass
class SourceConfig:
    source: str
    satellite: str
    crossover_type: str
    cycle_length: float
    window_size: int
    window_padding: int
    max_pass_number: int


def _load_sources() -> dict[str, SourceConfig]:
    config_path = os.path.join(os.path.dirname(__file__), "sources.yaml")
    with open(config_path) as f:
        raw = yaml.safe_load(f)

    configs = {}
    for source_key, cfg in raw["sources"].items():
        configs[source_key] = SourceConfig(
            source=source_key,
            satellite=cfg["satellite"],
            crossover_type=cfg["crossover_type"],
            cycle_length=cfg["cycle_length"],
            window_size=cfg["window_size"],
            window_padding=cfg["window_padding"],
            max_pass_number=cfg["max_pass_number"],
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
