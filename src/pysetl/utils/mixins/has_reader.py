"""HasReader module."""
from abc import abstractmethod, ABC
from pyspark.sql import DataFrameReader


class HasReader(ABC):
    """HasReader mixin."""

    @property
    @abstractmethod
    def _reader(self) -> DataFrameReader:
        """Returns a DataFrameReader."""
