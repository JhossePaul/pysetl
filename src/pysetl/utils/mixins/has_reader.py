"""HasReader module."""
from abc import abstractmethod
from pyspark.sql import DataFrameReader


class HasReader:
    """HasReader mixin."""

    @property
    @abstractmethod
    def _reader(self) -> DataFrameReader:
        """Returns a DataFrameReader."""
