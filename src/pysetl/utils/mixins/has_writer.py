"""HasWriter module."""
from abc import abstractmethod, ABC
from pyspark.sql import DataFrame, DataFrameWriter


class HasWriter(ABC):
    """HasWrite Mixin."""

    @abstractmethod
    def _writer(self, data: DataFrame) -> DataFrameWriter:
        """Return a DataFrameWriter."""
