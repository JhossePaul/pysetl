"""CsvConfig module."""
from typing_extensions import Self
from pysetl.enums import FileStorage
from .file_config import FileConfig, FileConfigModel


class CsvConfigModel(FileConfigModel):
    """CsvConfig validator."""

    inferSchema: str = "true"
    header: str = "true"
    delimiter: str = ","


class CsvConfig(FileConfig):
    """CsvConfig class."""

    @property
    def config(self: Self) -> CsvConfigModel:
        """Return validated dataset."""
        _config: dict = self._config | {"storage": FileStorage.CSV}

        return CsvConfigModel(**_config)
