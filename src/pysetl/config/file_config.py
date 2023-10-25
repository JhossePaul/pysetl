"""FileConnectorConfig module."""
from typing import Optional
from typing_extensions import Self
from pydantic import ConfigDict
from pyspark.sql.types import StructType
from pysetl.enums import SaveMode, FileStorage
from .aws_credentials import AwsCredentials
from .config import Config, BaseConfigModel


class FileConfigModel(BaseConfigModel):
    """Validator for file configurations."""
    model_config = ConfigDict(
        extra="allow",
        frozen=True,
        strict=True,
        arbitrary_types_allowed=True
    )

    storage: FileStorage
    path: str
    partition_by: list[str] = []
    savemode: SaveMode = SaveMode.ERRORIFEXISTS
    data_schema: Optional[StructType] = None
    aws_credentials: Optional[AwsCredentials] = None


class FileConfig(Config[FileConfigModel]):
    """Configuration for file connectors."""

    @property
    def config(self: Self) -> FileConfigModel:
        """Returns validated configuration."""
        return FileConfigModel(**self._config)

    @property
    def reader_config(self: Self) -> dict:
        """Configurations passed to DataFrameReader."""
        return {
            k: v
            for k, v
            in self.config
            if k not in FileConfigModel.model_fields
        }

    @property
    def writer_config(self) -> dict:
        """Configurations passed to DataFrameWriter."""
        return {
            k: v
            for k, v
            in self.config
            if k not in FileConfigModel.model_fields
        }
