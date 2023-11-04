"""Repository module."""
from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from typedspark import DataSet, Schema
from typing_extensions import Self


T = TypeVar("T", bound=Schema)


class BaseRepository(Generic[T], ABC):
    """A Repository is an abstract high-level type-safe layer over a
    BaseConnector."""
    @abstractmethod
    def load(self: Self) -> DataSet[T]:
        """Read all data from data storage"""

    @abstractmethod
    def save(self: Self, data: DataSet[T]) -> Self:
        """Save data."""
