"""JsonConfig module."""
from typing_extensions import Self
from pysetl.enums import FileStorage
from .file_config import FileConfig, FileConfigModel


class JsonConfigModel(FileConfigModel):
    """JsonConfig validator."""


class JsonConfig(FileConfig):
    """JsonConfig class."""

    @property
    def config(self: Self) -> JsonConfigModel:
        """Returns validated JSON configuration."""
        _config: dict = self.params | {"storage": FileStorage.JSON}

        return JsonConfigModel(**_config)
