"""
JsonConnector module for PySetl.

Provides the JsonConnector for reading and writing JSON files using Spark.
"""
from typing import Union
from typing_extensions import Self
from pysetl.config import JsonConfig, FileConfig
from pysetl.utils.exceptions import InvalidConfigException
from pysetl.enums import FileStorage
from .file_connector import FileConnector


class JsonConnector(FileConnector):
    """
    Connector for reading and writing JSON files using Spark.

    Inherits from FileConnector and validates that the storage type is JSON.
    """

    def __init__(self: Self, options: Union[FileConfig, JsonConfig]) -> None:
        """
        Initialize the JsonConnector.

        Args:
            options (Union[FileConfig, JsonConfig]): The file configuration for the connector.

        Raises:
            InvalidConfigException: If the storage type is not JSON.
        """
        if options.config.storage != FileStorage.JSON:
            raise InvalidConfigException("Not a JsonConfig for a JsonConnector")

        super().__init__(options)
