from abc import ABC, abstractmethod


class Fetcher(ABC):
    
    def __init__(self):
        pass
    
    @abstractmethod
    def setup_s3(self):
        pass
    
    @abstractmethod
    def fetch(self):
        pass