"""
Provides the JsonConnector for reading and writing JSON files within the
pysetl.connector package.
"""
from typing import Union
from typing_extensions import Self
from pysetl.config import JsonConfig, FileConfig
from pysetl.utils.exceptions import InvalidConfigException
from pysetl.enums import FileStorage
from .file_connector import FileConnector


class JsonConnector(FileConnector):
    """JsonConnector allows to read JSON files."""

    def __init__(self: Self, options: Union[FileConfig, JsonConfig]) -> None:
        """Initialize JsonConnector."""
        if options.config.storage != FileStorage.JSON:
            raise InvalidConfigException(
                "Not a JsonConfig for a JsonConnector"
            )

        super().__init__(options)
