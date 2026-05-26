from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Protocol


@dataclass(frozen=True)
class GranuleRef:
    """Pipeline_init's internal record of a discovered granule.

    Only `uri` is serialized into the manifest; `mod_time` is consumed
    during planning to diff against existing P3 mod-times.
    """

    date: date
    uri: str
    mod_time: datetime
    sort_key: tuple = field(default_factory=tuple)


class Enumerator(Protocol):
    """Find upstream granules for a source over a date range."""

    def enumerate(self, start: date, end: date) -> list[GranuleRef]:
        ...
