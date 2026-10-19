"""The EventBridge schedules that trigger pipeline runs.

A mirror of hand-maintained AWS state, not a deployment source — see the header
comment in `schedules.yaml`. Loaded here so a test can pair it against the
Source registry; nothing at runtime reads it.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import yaml

SCHEDULES_PATH = Path(__file__).parent / "schedules.yaml"

# The cron placeholder for a rule that does not exist in AWS yet. Same role as
# `id: "TBD"` in products.yaml: an obviously-unfinished value, so it cannot be
# mistaken for a recorded fact.
UNSCHEDULED = "TBD"


@dataclass(frozen=True)
class Schedule:
    """One EventBridge rule. `input` is the state-machine input verbatim, so the
    trigger shape is reviewable: it carries `published_product` and nothing a
    nominal scheduled run should not have (no `force_update`, no date window)."""

    name: str
    cron: str
    enabled: bool
    input: dict

    @property
    def published_product(self) -> str:
        try:
            return self.input["published_product"]
        except KeyError:
            raise ValueError(
                f"Schedule '{self.name}' declares no published_product in its "
                f"input; the trigger is product-keyed."
            ) from None

    @property
    def created(self) -> bool:
        """Whether the rule exists in AWS. A `TBD` cron means staged in config only."""
        return self.cron != UNSCHEDULED


@lru_cache(maxsize=None)
def list_schedules() -> tuple[Schedule, ...]:
    with open(SCHEDULES_PATH) as f:
        raw = yaml.safe_load(f) or {}
    declared = raw.get("schedules") or {}
    return tuple(
        Schedule(
            name=name,
            cron=facts.get("cron", UNSCHEDULED),
            enabled=bool(facts.get("enabled", False)),
            input=dict(facts.get("input") or {}),
        )
        for name, facts in sorted(declared.items())
    )


def clear_cache() -> None:
    """Test helper."""
    list_schedules.cache_clear()
