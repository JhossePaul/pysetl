"""
Abstract base repository for PySetl.

Defines the abstract base class for repositories in the repository subpackage.
All repositories provide a high-level, type-safe interface over connectors.
"""
from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from typedspark import DataSet, Schema
from typing_extensions import Self


T = TypeVar("T", bound=Schema)


class BaseRepository(Generic[T], ABC):
    """
    Abstract base class for type-safe repositories over connectors.

    All repositories provide a high-level, type-safe interface for loading and saving data.
    This class cannot be instantiated directly.
    """

    @abstractmethod
    def load(self: Self) -> DataSet[T]:
        """
        Abstract method to read all data from data storage.

        Returns:
            DataSet[T]: The loaded dataset.

        Raises:
            NotImplementedError: If not implemented in a subclass.
        """

    @abstractmethod
    def save(self: Self, data: DataSet[T]) -> Self:
        """
        Abstract method to save data to storage.

        Args:
            data (DataSet[T]): The dataset to save.

        Returns:
            Self: The repository instance (for chaining).

        Raises:
            NotImplementedError: If not implemented in a subclass.
        """
