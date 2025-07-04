"""
ConnectorBuilder module for PySetl.

Provides the ConnectorBuilder for constructing specific connector instances
based on configuration.
"""
from typing import Optional
from typing_extensions import Self
from pysetl.utils import Builder
from pysetl.enums import FileStorage
from pysetl.config import FileConfig
from pysetl.utils.exceptions import BuilderException
from .csv_connector import CsvConnector
from .json_connector import JsonConnector
from .parquet_connector import ParquetConnector
from .connector import Connector


class ConnectorBuilder(Builder[Connector]):
    """
    Builder for constructing connector instances based on configuration.

    Attributes:
        config (FileConfig): The file configuration for the connector.
        connector (Optional[Connector]): The constructed connector instance.
    """

    def __init__(self: Self, config: FileConfig):
        """
        Initialize the ConnectorBuilder.

        Args:
            config (FileConfig): The file configuration for the connector.
        """
        self.config = config
        self.connector: Optional[Connector] = None

    def build(self) -> Self:
        """
        Create a connector instance based on the declared storage type in the configuration.

        Returns:
            Self: The builder instance with the connector set.
        """
        storage = self.config.config.storage

        if storage == FileStorage.CSV:
            self.connector = CsvConnector(self.config)
        elif storage == FileStorage.PARQUET:
            self.connector = ParquetConnector(self.config)
        else:
            self.connector = JsonConnector(self.config)

        return self

    def get(self: Self) -> Connector:
        """
        Return the constructed connector instance based on the configuration.

        Returns:
            Connector: The constructed connector instance.

        Raises:
            BuilderException: If the connector has not been built yet.
        """
        if not self.connector:
            raise BuilderException("Connector is not defined")

        return self.connector
