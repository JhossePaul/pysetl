"""
Provides the CsvConnector for reading and writing CSV files within the
pysetl.connector package.
"""
from typing import Union
from typing_extensions import Self
from pysetl.config import CsvConfig, FileConfig
from pysetl.utils.exceptions import InvalidConfigException
from pysetl.enums import FileStorage
from .file_connector import FileConnector


class CsvConnector(FileConnector):
    """CsvConnector allows to read CSV files."""

    def __init__(self: Self, options: Union[FileConfig, CsvConfig]) -> None:
        """Initialize CsvConnector."""
        if options.config.storage != FileStorage.CSV:
            raise InvalidConfigException("Not a CsvConfig for a CsvConnector")

        super().__init__(options)
