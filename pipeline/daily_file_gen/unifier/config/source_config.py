from utilities.source_profile import (
    SourceCommon,
    list_sources_for_stage,
    load_source_config,
)


# Unifier has no stage-specific fields after the layout refactor —
# src/dst keys are derived from `utilities.pipeline_layout`.
UnifierSourceConfig = SourceCommon


def get_source_config(source: str) -> UnifierSourceConfig:
    return load_source_config(UnifierSourceConfig, "unifier", source)


def get_available_sources() -> list[str]:
    return list_sources_for_stage("unifier")
