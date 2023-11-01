"""Connector builder module."""
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
    """Build a connector based only on the configuration."""

    def __init__(self: Self, config: FileConfig):
        """Initialize ConnectorBuilder."""
        self.config = config
        self.connector: Optional[Connector] = None

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

    def get(self: Self) -> Connector:
        """Return the right connector based on the configutration."""
        if not self.connector:
            raise BuilderException("Connector is not defined")

        return self.connector
