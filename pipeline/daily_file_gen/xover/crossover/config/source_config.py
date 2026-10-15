from dataclasses import dataclass

from utilities.source_profile import (
    SourceCommon,
    list_sources_for_stage,
    load_source_config,
)

# The only published along-track line a high_latitude source can be crossed
# against today. Declared as the default so adding a high-lat source needs no
# reference wiring, and kept configurable so a future second reference line
# (or a dev-cluster stand-in) does not need a code change.
REFERENCE_PRODUCT = "NASA-SSH"


@dataclass(kw_only=True, frozen=True)
class SourceConfig(SourceCommon):
    crossover_type: str
    cycle_length: float
    window_size: int
    window_padding: int
    max_pass_number: int
    # Only used by crossover_type == "reference": the **published product** a
    # high_latitude source is crossed against (see ADR-0006). A published
    # product, not a source — it is composed from several missions and has no
    # internal representation. There is no version to choose: one published
    # along-track line per identity, always finalized.
    reference_product: str = REFERENCE_PRODUCT

    def __post_init__(self):
        if self.crossover_type == "reference" and not self.reference_product:
            raise ValueError(
                f"crossover_type 'reference' (source '{self.source}') requires a "
                f"reference_product in its xover config."
            )


def get_source_config(source: str) -> SourceConfig:
    return load_source_config(SourceConfig, "xover", source)


def get_available_sources() -> list[str]:
    return list_sources_for_stage("xover")
