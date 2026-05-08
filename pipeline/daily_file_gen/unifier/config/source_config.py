from dataclasses import dataclass

from utilities.source_profile import (
    SourceCommon,
    list_sources_for_stage,
    load_source_config,
)


@dataclass(kw_only=True, frozen=True)
class UnifierSourceConfig(SourceCommon):
    src_filename_template: str
    dst_filename_template: str
    dst_prefix: str


def get_source_config(source: str) -> UnifierSourceConfig:
    return load_source_config(UnifierSourceConfig, "unifier", source)


def get_available_sources() -> list[str]:
    return list_sources_for_stage("unifier")
