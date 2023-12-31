"""SparkRepository module."""
from typing import TypeVar, get_args, _GenericAlias  # type: ignore
from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from typing_extensions import Self
from typedspark import DataSet, Schema
from pysetl.utils import pretty
from pysetl.utils.mixins import (
    HasLogger, HasSparkSession, CanPartition, CanDrop
)
from pysetl.storage.connector import Connector
from pysetl.utils.exceptions import InvalidConnectorException
from .base_repository import BaseRepository


T = TypeVar("T", bound=Schema)


class SparkRepository(BaseRepository[T], HasLogger, HasSparkSession):
    """
    SparkRepository class.

    A SparkRepository garantees read-after-write consistency and
    links a output type T to its connector and config
    """

    def __init__(
        self,
        connector: Connector,
        cache: bool = False,
        flush: bool = False,
        storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK
    ) -> None:
        """Initialize SparkRepository."""
        self.__connector = connector
        self.__cache: bool = cache
        self.__flush: bool = flush
        self.__storage_level: StorageLevel = storage_level
        self.__cached: DataFrame = self.spark.createDataFrame(
            [],
            StructType()
        )

    @property
    def type_annotation(self: Self) -> _GenericAlias:
        """Return type annotation."""
        return getattr(self, "__orig_class__")

    def __str__(self: Self) -> str:
        """Customize str method."""
        return pretty(self.type_annotation)  # pylint: disable=E1101

    def __repr__(self: Self) -> str:
        """Customize repr method."""
        return str(self)

    @property
    def cache(self: Self) -> bool:
        """Whether the dataframe is cached."""
        return self.__cache

    @cache.setter
    def cache(self: Self, __value: bool) -> None:
        """Setter for cache property."""
        self.__cache = __value

    def set_cache(self: Self, __value: bool) -> Self:
        """Setter for cache_property."""
        self.__cache = __value
        return self

    @property
    def connector(self: Self) -> Connector:
        """Exposes repository connector."""
        return self.__connector

    def load(self: Self) -> DataSet[T]:
        """Read all data from data storage."""
        [__type, *_] = get_args(self.type_annotation)

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
        """Save data."""
        self.__flush = True
        self.connector.write(data)

        return self

    def list_partitions(self: Self) -> list[str]:
        """List current all current available partitions in the storage."""
        if not isinstance(self.connector, CanPartition):
            raise InvalidConnectorException(
                "Current connector doesn't support partitions"
            )

        return self.connector.list_partitions()

    def load_partitions(self: Self, partitions: list[str]) -> DataSet[T]:
        """Retrive a subset of partitions from the data."""
        if not isinstance(self.connector, CanPartition):
            raise InvalidConnectorException(
                "Current connector doesn't support partitions"
            )

        dataframe = self.connector.read_partitions(partitions)
        [__tpe, *_] = get_args(self.type_annotation)

        return DataSet[__tpe](dataframe)  # type: ignore

    def drop(self: Self) -> Self:
        """Drop the entire data. Drop table o remove FS directory."""
        if not isinstance(self.connector, CanDrop):
            raise InvalidConnectorException(
                "Current connector doesn't support drop"
            )

        self.connector.drop()

        return self
