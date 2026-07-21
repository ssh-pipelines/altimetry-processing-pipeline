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
    # Only used by crossover_type == "reference": the reference mission a
    # high_latitude source is crossed against, and the (always finalized) daily
    # file version to load for it (see ADR-0006). Optional on the shared config,
    # required + validated below when the type is "reference".
    reference_source: str | None = None
    reference_version: str | None = None

    def __post_init__(self):
        if self.crossover_type == "reference":
            missing = [
                name
                for name in ("reference_source", "reference_version")
                if getattr(self, name) is None
            ]
            if missing:
                raise ValueError(
                    f"crossover_type 'reference' (source '{self.source}') requires "
                    f"{missing} in its xover config."
                )


def get_source_config(source: str) -> SourceConfig:
    return load_source_config(SourceConfig, "xover", source)


def get_available_sources() -> list[str]:
    return list_sources_for_stage("xover")
