"""
CsvConnector module for PySetl.

Provides the CsvConnector for reading and writing CSV files using Spark.
"""
from typing import Union
from typing_extensions import Self
from pysetl.config import CsvConfig, FileConfig
from pysetl.utils.exceptions import InvalidConfigException
from pysetl.enums import FileStorage
from .file_connector import FileConnector


class CsvConnector(FileConnector):
    """
    Connector for reading and writing CSV files using Spark.

    Inherits from FileConnector and validates that the storage type is CSV.
    """

    def __init__(self: Self, options: Union[FileConfig, CsvConfig]) -> None:
        """
        Initialize the CsvConnector.

        Args:
            options (Union[FileConfig, CsvConfig]): The file configuration for the connector.

        Raises:
            InvalidConfigException: If the storage type is not CSV.
        """
        if options.config.storage != FileStorage.CSV:
            raise InvalidConfigException("Not a CsvConfig for a CsvConnector")

        super().__init__(options)
