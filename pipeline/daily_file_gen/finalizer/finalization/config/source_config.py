from dataclasses import dataclass

from utilities.source_profile import (
    SourceCommon,
    list_sources_for_stage,
    load_source_config,
)


@dataclass(kw_only=True, frozen=True)
class PassFlagConfig:
    mean_num: float
    rms_num: float
    mean_threshold: float
    rms_threshold: float


@dataclass(kw_only=True, frozen=True)
class FinalizerSourceConfig(SourceCommon):
    offset: float
    pass_flag: PassFlagConfig

    def __post_init__(self):
        if isinstance(self.pass_flag, dict):
            object.__setattr__(self, "pass_flag", PassFlagConfig(**self.pass_flag))


def get_source_config(source: str) -> FinalizerSourceConfig:
    return load_source_config(FinalizerSourceConfig, "finalizer", source)


def get_available_sources() -> list[str]:
    return list_sources_for_stage("finalizer")
