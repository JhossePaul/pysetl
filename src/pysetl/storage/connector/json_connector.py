"""JsonConnector module."""
from typing import Union
from typing_extensions import Self
from pysetl.config import JsonConfig, FileConfig
from pysetl.utils.exceptions import BadConfiguration
from pysetl.enums import FileStorage
from .file_connector import FileConnector


class JsonConnector(FileConnector):
    """JsonConnector allows to read JSON files."""

    def __init__(self: Self, options: Union[FileConfig, JsonConfig]) -> None:
        """Initialize JsonConnector."""
        if options.config.storage != FileStorage.JSON:
            raise BadConfiguration("Not a JsonConfig for a JsonConnector")

        super().__init__(options)
