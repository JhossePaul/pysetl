"""
HasWriter mixin for PySetl.

Provides an abstract mixin to add writer functionality to classes.
"""
from abc import abstractmethod, ABC
from pyspark.sql import DataFrame, DataFrameWriter


class HasWriter(ABC):
    """
    Abstract mixin to add writer functionality to a class.

    This class cannot be instantiated directly.
    """

    @abstractmethod
    def _writer(self, data: DataFrame) -> DataFrameWriter:
        """Return a DataFrameWriter."""
