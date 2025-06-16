"""
The pysetl.storage.repository package. Provides high-level, type-safe data
access layers (repositories) over connectors.
"""
from .base_repository import BaseRepository
from .sparkrepository import SparkRepository
from .sparkrepository_builder import SparkRepositoryBuilder


__all__ = [
    "BaseRepository",
    "SparkRepository",
    "SparkRepositoryBuilder"
]
