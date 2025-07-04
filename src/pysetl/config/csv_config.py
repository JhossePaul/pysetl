"""
CsvConfig module for PySetl.

Defines configuration models and validation for CSV file connectors.
"""
from typing_extensions import Self
from pysetl.enums import FileStorage
from .file_config import FileConfig, FileConfigModel


class CsvConfigModel(FileConfigModel):
    """
    Validator for CSV file connector configurations.

    Attributes:
        inferSchema (str): Whether to infer schema from the CSV file (default: "true").
        header (str): Whether the CSV file has a header row (default: "true").
        delimiter (str): Delimiter used in the CSV file (default: ",").
    """

    inferSchema: str = "true"
    header: str = "true"
    delimiter: str = ","


class CsvConfig(FileConfig):
    """
    Configuration for CSV file connectors.

    Provides validated configuration for reading and writing CSV files.
    """

    @property
    def config(self: Self) -> CsvConfigModel:
        """
        Returns the validated CSV file connector configuration.

        Returns:
            CsvConfigModel: The validated configuration model for CSV files.
        """
        _config: dict = self.params | {"storage": FileStorage.CSV}

        return CsvConfigModel(**_config)
