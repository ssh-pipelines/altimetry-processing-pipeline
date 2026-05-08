from dataclasses import dataclass, field

from utilities.source_profile import (
    CollectionConfig,
    SourceCommon,
    list_sources_for_stage,
    load_source_config,
)


@dataclass(kw_only=True, frozen=True)
class SmoothingConfig:
    speed: float
    sigma: float


@dataclass(kw_only=True, frozen=True)
class SourceConfig(SourceCommon):
    filename_template: str
    s3_prefix: str
    source_mss: str
    target_mss: str
    mss_diff_file: str
    smoothing: SmoothingConfig
    bad_points: dict | None = None

    def __post_init__(self):
        # Allow YAML-loaded smoothing dict to be coerced to SmoothingConfig.
        if isinstance(self.smoothing, dict):
            object.__setattr__(self, "smoothing", SmoothingConfig(**self.smoothing))


def get_source_config(source: str) -> SourceConfig:
    return load_source_config(SourceConfig, "daily_files", source)


def get_available_sources() -> list[str]:
    return list_sources_for_stage("daily_files")


__all__ = [
    "SourceConfig",
    "SmoothingConfig",
    "CollectionConfig",
    "get_source_config",
    "get_available_sources",
]
