"""
ParquetConnector module for PySetl.

Provides the ParquetConnector for reading and writing Parquet files using Spark.
"""
from typing import Union
from typing_extensions import Self
from pysetl.config import ParquetConfig, FileConfig
from pysetl.utils.exceptions import InvalidConfigException
from pysetl.enums import FileStorage
from .file_connector import FileConnector


class ParquetConnector(FileConnector):
    """
    Connector for reading and writing Parquet files using Spark.

    Inherits from FileConnector and validates that the storage type is PARQUET.
    """

    def __init__(self: Self, options: Union[FileConfig, ParquetConfig]) -> None:
        """
        Initialize the ParquetConnector.

        Args:
            options (Union[FileConfig, ParquetConfig]): The file configuration for the connector.

        Raises:
            InvalidConfigException: If the storage type is not PARQUET.
        """
        if options.config.storage != FileStorage.PARQUET:
            raise InvalidConfigException("Not a ParquetConfig for a ParquetConnector")

        super().__init__(options)
