from utilities.source_profile import (
    SourceCommon,
    list_sources_for_stage,
    load_source_config,
)

# Pipeline_init has no stage-specific fields after the layout refactor —
# all paths and filenames are derived from `utilities.pipeline_layout`.
# The alias is kept so existing type annotations continue to read naturally.
PipelineInitSourceConfig = SourceCommon


def get_source_config(source: str) -> PipelineInitSourceConfig:
    return load_source_config(PipelineInitSourceConfig, "pipeline_init", source)


def get_available_sources() -> list[str]:
    return list_sources_for_stage("pipeline_init")
