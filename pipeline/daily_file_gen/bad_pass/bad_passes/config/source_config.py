from dataclasses import dataclass

from utilities.source_profile import (
    SourceCommon,
    list_sources_for_stage,
    load_source_config,
)


@dataclass(kw_only=True, frozen=True)
class BadPassConfig(SourceCommon):
    """bad_pass stage config.

    The self/reference dispatch uses the inherited ``product_type``
    (``reference`` → self stacking, ``high_latitude`` → reference fixed-truth),
    mirroring the OER stage. bad_pass does no spline fitting, so unlike
    ``OerConfig`` it needs no ``ground_speed``; only the inherited
    ``product_type`` (dispatch) and the reference-path window half-width matter.

    ``reference_window_size`` is the half-width (in days) of the *centered*
    crossover-fetch window used only by the reference path; the self path keeps
    its historical backward-looking window.
    """

    reference_window_size: int = 2


def get_source_config(source: str) -> BadPassConfig:
    return load_source_config(BadPassConfig, "bad_pass", source)


def get_available_sources() -> list[str]:
    return list_sources_for_stage("bad_pass")
