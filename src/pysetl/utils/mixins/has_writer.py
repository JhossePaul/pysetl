"""HasWriter module."""
from abc import abstractmethod
from pyspark.sql import DataFrame, DataFrameWriter


class HasWriter:
    """HasWrite Mixin."""

    @abstractmethod
    def _writer(self, data: DataFrame) -> DataFrameWriter:
        """Return a DataFrameWriter."""
