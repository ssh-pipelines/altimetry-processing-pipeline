from dataclasses import dataclass

from utilities.source_profile import (
    SourceCommon,
    list_sources_for_stage,
    load_source_config,
)


@dataclass(kw_only=True, frozen=True)
class SourceConfig(SourceCommon):
    crossover_type: str
    cycle_length: float
    window_size: int
    window_padding: int
    max_pass_number: int


def get_source_config(source: str) -> SourceConfig:
    return load_source_config(SourceConfig, "xover", source)


def get_available_sources() -> list[str]:
    return list_sources_for_stage("xover")
