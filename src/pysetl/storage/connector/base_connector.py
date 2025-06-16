"""
Defines the abstract base class for all connectors in the pysetl.connector
package.
"""
from abc import ABCMeta, abstractmethod
from pyspark.sql import DataFrame
from typing_extensions import Self


class BaseConnector(metaclass=ABCMeta):
    """
    Abstract Connector.

    A connector should implement read/write operations. These implementations
    should be architecture and object-type specific
    """

    @abstractmethod
    def read(self: Self) -> DataFrame:
        """
        Read data into a DataFrame.

        Read method should implement specific cases for spark connections in
        order to read data
        """

    @abstractmethod
    def write(self: Self, data: DataFrame) -> None:
        """
        Write data into a DataFrame.

        Write method should implement specific cases for spark
        connections in order to write data
        """
