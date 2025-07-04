"""
CanPartition mixin for PySetl.

Provides an abstract mixin to add partition listing functionality to connectors.
"""
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from typing_extensions import Self


class CanPartition(ABC):
    """
    Abstract mixin to add partition listing functionality to a connector.

    This class cannot be instantiated directly.
    """

    @abstractmethod
    def list_partitions(self: Self) -> list[str]:
        """
        Abstract method to list all partitions in the storage.

        Returns:
            list[str]: List of partition directory paths.

        Raises:
            NotImplementedError: If not implemented in a subclass.
        """

    @abstractmethod
    def read_partitions(self, partitions: list[str]) -> DataFrame:
        """Read partitions from data source."""
