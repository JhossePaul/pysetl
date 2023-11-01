"""
ParquetConfig module
"""
from typing_extensions import Self
from pysetl.enums import FileStorage
from .file_config import FileConfig, FileConfigModel


class ParquetConfigModel(FileConfigModel):
    """
    ParquetConfig validator
    """


class ParquetConfig(FileConfig):
    """
    ParquetConfig class
    """
    @property
    def config(self: Self) -> ParquetConfigModel:
        _config: dict = self.params | {"storage": FileStorage.PARQUET}

        return ParquetConfigModel(**_config)
