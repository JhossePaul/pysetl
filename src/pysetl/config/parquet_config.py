"""
ParquetConfig module for PySetl.

Defines configuration models and validation for Parquet file connectors.
"""
from typing_extensions import Self
from pysetl.enums import FileStorage
from .file_config import FileConfig, FileConfigModel


class ParquetConfigModel(FileConfigModel):
    """
    Validator for Parquet file connector configurations.
    Inherits all fields from FileConfigModel.
    """


class ParquetConfig(FileConfig):
    """
    Configuration for Parquet file connectors.

    Provides validated configuration for reading and writing Parquet files.
    """

    @property
    def config(self: Self) -> ParquetConfigModel:
        """
        Returns the validated Parquet file connector configuration.

        Returns:
            ParquetConfigModel: The validated configuration model for Parquet files.
        """
        _config: dict = self.params | {"storage": FileStorage.PARQUET}

        return ParquetConfigModel(**_config)
