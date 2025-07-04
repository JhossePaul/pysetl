"""
Abstract base connector for PySetl.

Defines the abstract base class for all connectors in the connector subpackage.
All connectors should implement read and write operations for Spark DataFrames.
"""
from abc import ABCMeta, abstractmethod
from pyspark.sql import DataFrame
from typing_extensions import Self


class BaseConnector(metaclass=ABCMeta):
    """
    Abstract base class for all data connectors.

    All connectors should implement read and write operations for Spark DataFrames.
    This class cannot be instantiated directly.
    """

    @abstractmethod
    def read(self: Self) -> DataFrame:
        """
        Abstract method to read data into a Spark DataFrame.

        Returns:
            DataFrame: The loaded Spark DataFrame.

        Raises:
            NotImplementedError: If not implemented in a subclass.
        """

    @abstractmethod
    def write(self: Self, data: DataFrame) -> None:
        """
        Abstract method to write data from a Spark DataFrame.

        Args:
            data (DataFrame): The Spark DataFrame to write.

        Raises:
            NotImplementedError: If not implemented in a subclass.
        """
