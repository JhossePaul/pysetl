"""
SparkRepositoryBuilder module for PySetl.

Provides the SparkRepositoryBuilder for constructing SparkRepository instances
based on configuration and schema type.
"""
from typing import TypeVar, Generic, Optional, get_args
from typing_extensions import Self
from typedspark import Schema
from pysetl.utils import Builder
from pysetl.utils.exceptions import BuilderException
from pysetl.config import FileConfig
from pysetl.storage.connector import Connector, ConnectorBuilder
from .sparkrepository import SparkRepository


T = TypeVar("T", bound=Schema)


class SparkRepositoryBuilder(Generic[T], Builder[SparkRepository[T]]):
    """
    Builder for constructing SparkRepository instances.

    Builds a SparkRepository according to the given schema type and storage configuration.

    Attributes:
        config (FileConfig): The file configuration for the repository.
        cache (bool): Whether to cache the loaded DataFrame in the repository.
    """

    def __init__(self: Self, config: FileConfig, cache: bool = False) -> None:
        """
        Initialize the SparkRepositoryBuilder.

        Args:
            config (FileConfig): The file configuration for the repository.
            cache (bool): Whether to cache the loaded DataFrame in the repository.
        """
        self.config = config
        self.cache = cache
        self.__sparkrepository: Optional[SparkRepository[T]] = None

    def __create_connector(self: Self) -> Connector:
        """
        Create the connector for the repository.

        Returns:
            Connector: The constructed connector instance.
        """
        return ConnectorBuilder(self.config).getOrCreate()

    def build(self: Self) -> Self:
        """
        Build the SparkRepository from the configuration and schema type.

        Returns:
            Self: The builder instance with the repository set.
        """
        connector = self.__create_connector()
        orig_class = getattr(self, "__orig_class__")

        [__type, *_] = get_args(orig_class)  # pylint: disable=E1101
        self.__sparkrepository = SparkRepository[__type](  # type: ignore
            connector, self.cache
        )

        return self

    def get(self: Self) -> SparkRepository[T]:
        """
        Return the built SparkRepository instance.

        Returns:
            SparkRepository[T]: The constructed SparkRepository instance.

        Raises:
            BuilderException: If the repository has not been built yet.
        """
        if not self.__sparkrepository:
            raise BuilderException("No SparkRepository built")
        return self.__sparkrepository
