from dataclasses import dataclass

from utilities.source_profile import (
    SourceCommon,
    list_sources_for_stage,
    load_source_config,
)


@dataclass(kw_only=True, frozen=True)
class PipelineInitSourceConfig(SourceCommon):
    s3_prefix: str
    filename_template: str


def get_source_config(source: str) -> PipelineInitSourceConfig:
    return load_source_config(PipelineInitSourceConfig, "pipeline_init", source)


def get_available_sources() -> list[str]:
    return list_sources_for_stage("pipeline_init")
