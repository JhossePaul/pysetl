"""Connector builder module."""
from typing import Optional
from typing_extensions import Self
from pysetl.config.config import Config
from pysetl.utils import Builder
from pysetl.enums import FileStorage
from pysetl.utils.exceptions import InvalidConnectorException
from .csv_connector import CsvConnector
from .json_connector import JsonConnector
from .parquet_connector import ParquetConnector
from .base_connector import BaseConnector


class ConnectorBuilder(Builder[BaseConnector]):
    """Build a connector based only on the configuration."""

    def __init__(self: Self, config: Config):
        """Initialize ConnectorBuilder."""
        self.config = config
        self.connector: Optional[BaseConnector] = None

    def build(self) -> Self:
        """Create a connector based on the declared storage."""
        storage = self.config.config.storage

        if storage == FileStorage.CSV:
            self.connector = CsvConnector(self.config)
        elif storage == FileStorage.PARQUET:
            self.connector = ParquetConnector(self.config)
        else:
            self.connector = JsonConnector(self.config)

        return self

    def get(self: Self) -> Optional[BaseConnector]:
        """Return the right connector based on the configutration."""
        if not self.connector:
            raise InvalidConnectorException("Connector is not defined")

        return self.connector
