"""
HasReader mixin for PySetl.

Provides an abstract mixin to add reader functionality to classes.
"""
from abc import abstractmethod, ABC
from pyspark.sql import DataFrameReader


class HasReader(ABC):
    """
    Abstract mixin to add reader functionality to a class.

    This class cannot be instantiated directly.
    """

    @property
    @abstractmethod
    def _reader(self) -> DataFrameReader:
        """Returns a DataFrameReader."""
