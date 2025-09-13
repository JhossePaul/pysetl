"""
FileConnector module for PySetl.

Provides the FileConnector for handling operations on various file-based data
sources using Spark and PyArrow.
"""
from urllib.parse import urlparse, ParseResult
from pathlib import Path
from typing_extensions import Self
from pyarrow.fs import (  # type: ignore
    S3FileSystem,
    FileSystem,  # type: ignore
    LocalFileSystem,
    FileSelector,  # type: ignore
)
from pyspark.sql import DataFrame, DataFrameReader, DataFrameWriter
from pyspark.sql.types import StructType
from pysetl.config.file_config import FileConfig, FileConfigModel
from pysetl.utils.mixins import HasReader, HasWriter, CanDrop, CanPartition
from pysetl.utils.exceptions import InvalidConfigException
from .connector import Connector


class FileConnector(Connector, HasReader, HasWriter, CanDrop, CanPartition):
    """
    Connector for reading and writing data files using Spark and PyArrow.

    Handles file input/output operations on local and remote file systems,
    including partitioning and schema enforcement.
    """

    def __init__(self: Self, options: FileConfig) -> None:
        """
        Initialize the FileConnector.

        Args:
            options (FileConfig): The file configuration for the connector.
        """
        self.config: FileConfigModel = options.config
        self.reader_config = options.reader_config
        self.writer_config = options.writer_config

    @property
    def _reader(self: Self) -> DataFrameReader:
        """
        Returns a Spark DataFrameReader configured with reader options.

        Returns:
            DataFrameReader: Configured Spark DataFrameReader.
        """
        return self.spark.read.options(**self.reader_config)

    def __check_partitions(self: Self, data: DataFrame) -> bool:
        """
        Check if all partition columns are present in the DataFrame.

        Args:
            data (DataFrame): The DataFrame to check.

        Returns:
            bool: True if all partition columns are present, False otherwise.
        """
        return (
            all(x in data.columns for x in self.config.partition_by)
            if self.config.partition_by
            else True
        )

    def _writer(self: Self, data: DataFrame) -> DataFrameWriter:
        """
        Returns a Spark DataFrameWriter configured with writer options and partitioning.

        Args:
            data (DataFrame): The DataFrame to write.

        Returns:
            DataFrameWriter: Configured Spark DataFrameWriter.

        Raises:
            InvalidConfigException: If partition columns are missing in the data.
        """
        if not self.__check_partitions(data):
            raise InvalidConfigException(
                "Partition columns in configuration not in data"
            )

        if isinstance(self.config.data_schema, StructType):
            ordered_data = data.select([x.name for x in self.config.data_schema])
        else:
            ordered_data = data

        return (
            ordered_data.write.mode(self.config.savemode.value)
            .options(**self.writer_config)
            .partitionBy(*self.config.partition_by)
            .format(self.config.storage.value)
        )

    @property
    def uri(self: Self) -> ParseResult:
        """
        Parse the string path into a URI.

        Returns:
            ParseResult: The parsed URI object.
        """
        return urlparse(self.config.path)

    @property
    def filesystem(self) -> FileSystem:
        """
        Get the current file system (local or S3) based on URI and configuration.

        Returns:
            FileSystem: The PyArrow FileSystem instance.

        Raises:
            InvalidConfigException: If S3 credentials are missing for S3 URIs.
        """
        if self.uri.scheme == "s3":
            if not self.config.aws_credentials:
                raise InvalidConfigException("No S3 credentials provided")

            return S3FileSystem(
                access_key=self.config.aws_credentials.access_key,
                secret_key=self.config.aws_credentials.secret_key,
                endpoint_override=f"{self.uri.hostname}:{self.uri.port}",
                allow_bucket_creation=True,
                allow_bucket_deletion=True,
            )

        fs, _ = FileSystem.from_uri(self.uri.geturl())  # type: ignore

        return fs

    @property
    def absolute_path(self) -> str:
        """
        Returns the absolute path for the file or directory.

        Returns:
            str: The absolute path.
        """
        return (
            str(Path(self.uri.path).absolute())
            if isinstance(self.filesystem, LocalFileSystem)
            else self.uri.path
        )

    @property
    def base_path(self) -> str:
        """
        Returns the base path (parent directory) for the file or directory.

        Returns:
            str: The base path.
        """
        return str(Path(self.absolute_path).parent)

    @property
    def has_wildcard(self: Self) -> bool:
        """
        Verifies if the path has a wildcard.

        Returns:
            bool: True if the path ends with a wildcard, False otherwise.
        """
        return Path(self.absolute_path).name.endswith("*")

    def list_partitions(self: Self) -> list[str]:
        """
        Return all found partitions in the base path.

        Returns:
            list[str]: List of partition directory paths.
        """
        selector = FileSelector(self.absolute_path)

        return [
            x.path
            for x in self.filesystem.get_file_info(selector)
            if x.type.name == "Directory"
        ]

    def write(self: Self, data: DataFrame) -> None:
        """
        Write data into the file system.

        Args:
            data (DataFrame): The Spark DataFrame to write.

        Raises:
            InvalidConfigException: If the path contains a wildcard.
        """
        if self.has_wildcard:
            raise InvalidConfigException("Can't write to wildcard path")

        self._writer(data).save(self.absolute_path)

    def drop(self: Self) -> None:
        """
        Remove data files from the table directory.
        """
        self.filesystem.delete_dir(self.absolute_path)

    def read(self: Self) -> DataFrame:
        """
        Read data from the file system into a Spark DataFrame.

        Returns:
            DataFrame: The loaded Spark DataFrame.
        """
        return self._reader.format(self.config.storage.value).load(self.absolute_path)

    def read_partitions(self, partitions: list[str]) -> DataFrame:
        """
        Read specific partitions from the data source.

        Args:
            partitions (list[str]): List of partition paths to read.

        Returns:
            DataFrame: The loaded Spark DataFrame for the specified partitions.
        """
        return (
            self._reader.option("basePath", self.base_path)
            .format(self.config.storage.value)
            .load(partitions)
        )
