"""
JsonConfig module for PySetl.

Defines configuration models and validation for JSON file connectors.
"""
from typing_extensions import Self
from pysetl.enums import FileStorage
from .file_config import FileConfig, FileConfigModel


class JsonConfigModel(FileConfigModel):
    """
    Validator for JSON file connector configurations.
    Inherits all fields from FileConfigModel.
    """


class JsonConfig(FileConfig):
    """
    Configuration for JSON file connectors.

    Provides validated configuration for reading and writing JSON files.
    """

    @property
    def config(self: Self) -> JsonConfigModel:
        """
        Returns the validated JSON file connector configuration.

        Returns:
            JsonConfigModel: The validated configuration model for JSON files.
        """
        _config: dict = self.params | {"storage": FileStorage.JSON}

        return JsonConfigModel(**_config)
