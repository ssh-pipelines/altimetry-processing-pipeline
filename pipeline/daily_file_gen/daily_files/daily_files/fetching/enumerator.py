from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

from daily_files.config.source_config import SourceConfig


@dataclass
class FileRef:
    id: str
    title: str
    access_url: str
    time_start: str
    time_end: str
    modified_time: str
    collection_id: str


class Enumerator(ABC):
    def __init__(self, date: datetime, source_config: SourceConfig):
        self.date = date
        self.source_config = source_config

    @abstractmethod
    def enumerate(self) -> list[FileRef]:
        pass
