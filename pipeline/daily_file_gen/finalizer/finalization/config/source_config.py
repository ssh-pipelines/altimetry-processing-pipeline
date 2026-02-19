import os
from dataclasses import dataclass
from datetime import date

import yaml

from utilities.source_registry import get_source_entry


@dataclass
class PassFlagConfig:
    mean_num: float
    rms_num: float
    mean_threshold: float
    rms_threshold: float


@dataclass
class FinalizerSourceConfig:
    source: str
    product_type: str
    unify: bool
    offset: float
    start_date: date
    end_date: date | None
    pass_flag: PassFlagConfig


def _load_sources() -> dict[str, FinalizerSourceConfig]:
    config_path = os.path.join(os.path.dirname(__file__), "sources.yaml")
    with open(config_path) as f:
        raw = yaml.safe_load(f)

    configs = {}
    for source_key, cfg in raw["sources"].items():
        pass_flag = PassFlagConfig(**cfg["pass_flag"])
        registry = get_source_entry(source_key)

        end_date = cfg.get("end_date")
        if isinstance(end_date, str):
            end_date = date.fromisoformat(end_date)

        configs[source_key] = FinalizerSourceConfig(
            source=source_key,
            product_type=registry.product_type,
            unify=registry.unify,
            offset=cfg["offset"],
            start_date=registry.start_date,
            end_date=end_date,
            pass_flag=pass_flag,
        )
    return configs


_SOURCE_CONFIGS: dict[str, FinalizerSourceConfig] = {}


def get_source_config(source: str) -> FinalizerSourceConfig:
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
