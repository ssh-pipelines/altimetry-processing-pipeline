from dataclasses import dataclass
from typing import Optional

from utilities.source_profile import (
    CollectionConfig,
    SourceCommon,
    list_sources_for_stage,
    load_source_config,
)

_MSS_FIELDS = ("source_mss", "target_mss", "mss_diff_file")


@dataclass(kw_only=True, frozen=True)
class SmoothingConfig:
    speed: float
    sigma: float


@dataclass(kw_only=True, frozen=True)
class SourceConfig(SourceCommon):
    source_mss: Optional[str] = None
    target_mss: Optional[str] = None
    mss_diff_file: Optional[str] = None
    smoothing: SmoothingConfig
    bad_points: dict | None = None

    def __post_init__(self):
        # Allow YAML-loaded smoothing dict to be coerced to SmoothingConfig.
        if isinstance(self.smoothing, dict):
            object.__setattr__(self, "smoothing", SmoothingConfig(**self.smoothing))

        if self.product_type == "reference":
            for f in _MSS_FIELDS:
                if getattr(self, f) is None:
                    raise ValueError(
                        f"Source '{self.source}' (product_type=reference) is missing "
                        f"required MSS field '{f}'."
                    )
        elif self.product_type == "high_latitude":
            for f in _MSS_FIELDS:
                if getattr(self, f) is not None:
                    raise ValueError(
                        f"Source '{self.source}' (product_type=high_latitude) must not "
                        f"set '{f}' — high-latitude processors interpolate DTU21 directly "
                        f"(see docs/adr/0002-aviso-l2p-mss-handling.md)."
                    )


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
