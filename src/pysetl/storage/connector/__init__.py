"""
The pysetl.connector package. Provides connectors for various data sources and
formats.
"""
from .connector import Connector
from .csv_connector import CsvConnector
from .file_connector import FileConnector
from .json_connector import JsonConnector
from .parquet_connector import ParquetConnector
from .base_connector import BaseConnector
from .connector_builder import ConnectorBuilder


__all__ = [
    "BaseConnector",
    "Connector",
    "FileConnector",
    "CsvConnector",
    "JsonConnector",
    "ParquetConnector",
    "ConnectorBuilder"
]
