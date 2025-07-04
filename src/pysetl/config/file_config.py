"""
FileConnectorConfig module for PySetl.

Defines configuration models and validation for file-based data connectors.
"""
from typing import Optional
from typing_extensions import Self
from pyspark.sql.types import StructType
from pydantic import ConfigDict
from pysetl.enums import SaveMode, FileStorage
from .aws_credentials import AwsCredentials
from .config import Config, BaseConfigModel


class FileConfigModel(BaseConfigModel):
    """
    Validator for file connector configurations.

    Attributes:
        storage (FileStorage): The file storage backend type.
        path (str): Path to the file or directory.
        partition_by (list[str]): List of columns to partition by.
        savemode (SaveMode): Save mode for writing files.
        data_schema (Optional[StructType]): Optional Spark schema for the data.
        aws_credentials (Optional[AwsCredentials]): Optional AWS credentials for S3 or similar.
    """

    model_config = ConfigDict(
        extra="allow", frozen=True, strict=True, arbitrary_types_allowed=True
    )

    storage: FileStorage
    path: str
    partition_by: list[str] = []
    savemode: SaveMode = SaveMode.ERRORIFEXISTS
    data_schema: Optional[StructType] = None
    aws_credentials: Optional[AwsCredentials] = None


class FileConfig(Config[FileConfigModel]):
    """
    Configuration for file connectors.

    Provides validated configuration and reader/writer options for file-based data sources.
    """

    @property
    def config(self: Self) -> FileConfigModel:
        """
        Returns the validated file connector configuration.

        Returns:
            FileConfigModel: The validated configuration model.
        """
        return FileConfigModel(**self.params)

    @property
    def reader_config(self: Self) -> dict:
        """
        Returns the configuration dictionary for DataFrameReader.

        Returns:
            dict: Reader configuration options.
        """
        return {
            k: v for k, v in self.config if k not in FileConfigModel.model_fields.keys()
        }

    @property
    def writer_config(self) -> dict:
        """
        Returns the configuration dictionary for DataFrameWriter.

        Returns:
            dict: Writer configuration options.
        """
        return {
            k: v for k, v in self.config if k not in FileConfigModel.model_fields.keys()
        }
