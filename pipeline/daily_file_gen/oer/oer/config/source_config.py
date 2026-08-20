from dataclasses import dataclass

from utilities.source_profile import (
    SourceCommon,
    list_sources_for_stage,
    load_source_config,
)


@dataclass(kw_only=True, frozen=True)
class OerConfig(SourceCommon):
    """OER stage config.

    ``ground_speed`` is inherited from ``SourceCommon`` (the single canonical
    per-source value shared with daily-file smoothing) — OER does not carry its
    own. The self/reference dispatch also uses the inherited ``product_type``
    (``reference`` → self OER, ``high_latitude`` → reference OER), so there is no
    ``crossover_type`` field here either.

    ``intermission_bias`` is likewise inherited from ``SourceCommon`` (a single
    canonical per-source constant). On the reference OER path it is subtracted
    from the crossover differences before the spline fit so the spline captures
    pure orbit error; the same constant is applied to ``ssha`` downstream in the
    finalizer. It defaults to ``0.0`` and is a no-op on the self path.

    ``reference_window_size`` is the half-width (in days) of the *centered*
    crossover-fetch window used only by the reference path; the self path keeps
    its historical backward-looking window (see ``OerCorrection.make_polygon``).
    """

    reference_window_size: int = 2


def get_source_config(source: str) -> OerConfig:
    return load_source_config(OerConfig, "oer", source)


def get_available_sources() -> list[str]:
    return list_sources_for_stage("oer")
