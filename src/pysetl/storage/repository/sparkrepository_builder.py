"""SparkRepositoryBuilder module."""
from typing import TypeVar, Generic, Optional, get_args
from typing_extensions import Self
from typedspark import Schema
from pysetl.utils import Builder
from pysetl.config.config import Config
from pysetl.storage.connector import BaseConnector, ConnectorBuilder
from .sparkrepository import SparkRepository


T = TypeVar("T", bound=Schema)


class SparkRepositoryBuilder(Generic[T], Builder[SparkRepository[T]]):
    """
    Builder for SparkRepository.

    The SparkRepositoryBuilder will build a SparkRepository according to the
    given DataType and Storage
    @param storage type of storage
    @param A Config object
    """

    def __init__(
            self: Self,
            config: Config,
            cache: bool = False) -> None:
        """Initialize builder."""
        self.config = config
        self.cache = cache
        self.__sparkrepository: Optional[SparkRepository[T]] = None

    def __create_connector(self: Self) -> BaseConnector:
        """Create the connector for the repository."""
        return ConnectorBuilder(self.config).getOrCreate()

    def build(self: Self) -> Self:
        """Build repository from config."""
        connector = self.__create_connector()

        __type = get_args(self.__orig_class__)[0]  # pylint: disable=E1101
        self.__sparkrepository = SparkRepository[__type](
            connector,
            self.cache
        )

        return self

    def get(self: Self) -> Optional[SparkRepository[T]]:
        """Return built repository."""
        return self.__sparkrepository
