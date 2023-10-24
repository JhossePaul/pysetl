"""ParquetConnector module."""
from typing import Union
from typing_extensions import Self
from pysetl.config import ParquetConfig, FileConfig
from pysetl.utils.exceptions import BadConfiguration
from pysetl.enums import FileStorage
from .file_connector import FileConnector


class ParquetConnector(FileConnector):
    """ParquetConnector allows to read PARQUET files."""

    def __init__(
            self: Self,
            options: Union[FileConfig, ParquetConfig]) -> None:
        """Initialize ParquetConnector."""
        if options.config.storage != FileStorage.PARQUET:
            raise BadConfiguration(
                "Not a ParquetConfig for a ParquetConnector"
            )

        super().__init__(options)
