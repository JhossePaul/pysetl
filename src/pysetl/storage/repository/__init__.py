"""
Repository subpackage for PySetl.

Provides high-level, type-safe data access layers (repositories) over
connectors.

Exposes BaseRepository, SparkRepository, and SparkRepositoryBuilder for use
throughout the framework.
"""
from .base_repository import BaseRepository
from .sparkrepository import SparkRepository
from .sparkrepository_builder import SparkRepositoryBuilder


__all__ = ["BaseRepository", "SparkRepository", "SparkRepositoryBuilder"]
