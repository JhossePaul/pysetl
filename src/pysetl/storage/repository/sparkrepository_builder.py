"""
Provides the SparkRepositoryBuilder for constructing SparkRepository instances
within the pysetl.storage.repository package.
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
    Builder for SparkRepository.

    The SparkRepositoryBuilder will build a SparkRepository according to the
    given DataType and Storage
    @param storage type of storage
    @param A Config object
    """

    def __init__(
            self: Self,
            config: FileConfig,
            cache: bool = False) -> None:
        """Initialize builder."""
        self.config = config
        self.cache = cache
        self.__sparkrepository: Optional[SparkRepository[T]] = None

    def __create_connector(self: Self) -> Connector:
        """Create the connector for the repository."""
        return ConnectorBuilder(self.config).getOrCreate()

    def build(self: Self) -> Self:
        """Build repository from config."""
        connector = self.__create_connector()
        orig_class = getattr(self, "__orig_class__")

        [__type, *_] = get_args(orig_class)  # pylint: disable=E1101
        self.__sparkrepository = SparkRepository[__type](  # type: ignore
            connector,
            self.cache
        )

        return self

    def get(self: Self) -> SparkRepository[T]:
        """Return built repository."""
        if not self.__sparkrepository:
            raise BuilderException("No SparkRepository built")
        return self.__sparkrepository
