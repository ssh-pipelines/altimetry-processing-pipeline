from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable, TextIO

import numpy as np


@dataclass
class IngestedData:
    """Normalized data structure produced by all ingestors."""

    ssha: np.ndarray
    lat: np.ndarray
    lon: np.ndarray
    time: np.ndarray
    cycles: np.ndarray
    passes: np.ndarray
    dac: np.ndarray
    inv_bar_cor: np.ndarray
    source_specific: dict  # Additional source-specific arrays/objects needed for processing


class Ingestor(ABC):
    @abstractmethod
    def ingest(self, file_objs: Iterable[TextIO], **kwargs) -> IngestedData:
        """Open raw files and extract/normalize arrays into IngestedData."""
        raise NotImplementedError
