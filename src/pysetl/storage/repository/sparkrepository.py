"""
SparkRepository module for PySetl.

Provides the SparkRepository, a high-level, type-safe data access layer using a
connector.
"""
from typing import TypeVar, get_args, _GenericAlias  # type: ignore

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from typedspark import DataSet, Schema
from typing_extensions import Self

from pysetl.utils import pretty
from pysetl.utils.mixins import HasLogger, HasSparkSession, CanPartition, CanDrop
from pysetl.storage.connector.base_connector import BaseConnector
from pysetl.utils.exceptions import InvalidConnectorException
from .base_repository import BaseRepository


T = TypeVar("T", bound=Schema)


class SparkRepository(BaseRepository[T], HasLogger, HasSparkSession):
    """
    High-level, type-safe data access layer for Spark using a connector.

    Guarantees read-after-write consistency and links an output type T to its
    connector and config.

    Attributes:
        connector (BaseConnector): The underlying data connector.
        cache (bool): Whether to cache the loaded DataFrame.
        storage_level (StorageLevel): The Spark storage level for caching.
    """

    def __init__(
        self,
        connector: BaseConnector,
        cache: bool = False,
        flush: bool = False,
        storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK,
    ) -> None:
        """
        Initialize the SparkRepository.

        Args:
            connector (BaseConnector): The underlying data connector.
            cache (bool): Whether to cache the loaded DataFrame.
            flush (bool): Whether to flush the cache on next load.
            storage_level (StorageLevel): The Spark storage level for caching.
        """
        self.__connector = connector
        self.__cache: bool = cache
        self.__flush: bool = flush
        self.__storage_level: StorageLevel = storage_level
        self.__cached: DataFrame = self.spark.createDataFrame([], StructType())

    @property
    def type_annotation(self: Self) -> _GenericAlias:
        """
        Return the type annotation for the repository's output type.

        Returns:
            _GenericAlias: The type annotation for the output type.
        """
        return getattr(self, "__orig_class__")

    def __str__(self: Self) -> str:
        """
        Return a string representation of the repository's type annotation.

        Returns:
            str: The pretty-printed type annotation.
        """
        return pretty(self.type_annotation)  # pylint: disable=E1101

    def __repr__(self: Self) -> str:
        """
        Return the string representation for debugging.

        Returns:
            str: The pretty-printed type annotation.
        """
        return str(self)

    @property
    def cache(self: Self) -> bool:
        """
        Whether the DataFrame is cached.

        Returns:
            bool: True if caching is enabled, False otherwise.
        """
        return self.__cache

    @cache.setter
    def cache(self: Self, __value: bool) -> None:
        """
        Set the cache property.

        Args:
            __value (bool): Whether to enable caching.
        """
        self.__cache = __value

    def set_cache(self: Self, __value: bool) -> Self:
        """
        Set the cache property (for chaining).

        Args:
            __value (bool): Whether to enable caching.

        Returns:
            Self: The repository instance.
        """
        self.__cache = __value
        return self

    @property
    def connector(self: Self) -> BaseConnector:
        """
        Expose the repository's underlying connector.

        Returns:
            BaseConnector: The underlying data connector.
        """
        return self.__connector

    def load(self: Self) -> DataSet[T]:
        """
        Read all data from data storage.

        Returns:
            DataSet[T]: The loaded dataset.
        """
        __type, *_ = get_args(self.type_annotation)

        if self.__cache and (self.__flush or not self.__cached.is_cached):
            self.__cached = self.connector.read().persist(self.__storage_level)
            data = self.__cached
        elif self.__cache and not self.__flush and self.__cached.is_cached:
            data = self.__cached
        else:
            data = self.connector.read()

        self.__flush = False

        return DataSet[__type](data)  # type: ignore

    def save(self: Self, data: DataSet[T]) -> Self:
        """
        Save data to storage.

        Args:
            data (DataSet[T]): The dataset to save.

        Returns:
            Self: The repository instance (for chaining).
        """
        self.__flush = True
        self.connector.write(data)

        return self

    def list_partitions(self: Self) -> list[str]:
        """
        List all current available partitions in the storage.

        Returns:
            list[str]: List of partition directory paths.

        Raises:
            InvalidConnectorException: If the connector does not support partitions.
        """
        if not isinstance(self.connector, CanPartition):
            raise InvalidConnectorException(
                "Current connector doesn't support partitions"
            )

        return self.connector.list_partitions()

    def load_partitions(self: Self, partitions: list[str]) -> DataSet[T]:
        """
        Retrieve a subset of partitions from the data.

        Args:
            partitions (list[str]): List of partition paths to load.

        Returns:
            DataSet[T]: The loaded dataset for the specified partitions.

        Raises:
            InvalidConnectorException: If the connector does not support partitions.
        """
        if not isinstance(self.connector, CanPartition):
            raise InvalidConnectorException(
                "Current connector doesn't support partitions"
            )

        dataframe = self.connector.read_partitions(partitions)
        [__tpe, *_] = get_args(self.type_annotation)

        return DataSet[__tpe](dataframe)  # type: ignore

    def drop(self: Self) -> Self:
        """
        Drop the entire data (drop table or remove file system directory).

        Returns:
            Self: The repository instance (for chaining).

        Raises:
            InvalidConnectorException: If the connector does not support drop.
        """
        if not isinstance(self.connector, CanDrop):
            raise InvalidConnectorException("Current connector doesn't support drop")

        self.connector.drop()

        return self
