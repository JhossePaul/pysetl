"""
HasWriterMixin module
"""
from abc import abstractmethod
from pyspark.sql import DataFrame, DataFrameWriter


class HasWriterMixin:
    @abstractmethod
    def _writer(self, data: DataFrame) -> DataFrameWriter:
        pass
