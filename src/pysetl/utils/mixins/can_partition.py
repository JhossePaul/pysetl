"""
CanPartition module.

Connectors that inherit CanPartition should be able to partition the output by
the given columns on the file system
"""
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from typing_extensions import Self


class CanPartition(ABC):
    """CanPartition mixin class."""

    @abstractmethod
    def list_partitions(self: Self) -> list[str]:
        """List all available partitions."""

    @abstractmethod
    def read_partitions(self, partitions: list[str]) -> DataFrame:
        """Read partitions from data source."""
