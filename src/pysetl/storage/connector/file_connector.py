"""Generic file connector module."""
from urllib.parse import urlparse, ParseResult
from pathlib import Path
from typing_extensions import Self
from pyarrow.fs import S3FileSystem, FileSystem, LocalFileSystem, FileSelector
from pyspark.sql import DataFrame, DataFrameReader, DataFrameWriter
from pyspark.sql.types import StructType
from pysetl.config.file_config import FileConfig, FileConfigModel
from pysetl.utils.mixins import HasReader, HasWriter, CanDrop, CanPartition
from pysetl.utils.exceptions import InvalidConfigException
from .connector import Connector


class FileConnector(Connector, HasReader, HasWriter, CanDrop, CanPartition):
    """
    Connector to data files.

    This connector can handle file input/outputs on the file system.
    """

    def __init__(self: Self, options: FileConfig) -> None:
        """
        Initiallize for FileConnector.

        A file connector requieres a FileConfig
        """
        self.config: FileConfigModel = options.config
        self.reader_config = options.reader_config
        self.writer_config = options.writer_config

    @property
    def _reader(self: Self) -> DataFrameReader:
        return self.spark.read.options(**self.reader_config)

    def __check_partitions(self: Self, data: DataFrame) -> bool:
        return all(x in data.columns for x in self.config.partition_by) \
            if self.config.partition_by \
            else True

    def _writer(self: Self, data: DataFrame) -> DataFrameWriter:
        if not self.__check_partitions(data):
            raise InvalidConfigException(
                "Partition columns in configuration not in data"
            )

        if isinstance(self.config.data_schema, StructType):
            ordered_data = data.select([
                x.name
                for x
                in self.config.data_schema
            ])
        else:
            ordered_data = data

        return (
            ordered_data.write
            .mode(self.config.savemode.value)
            .options(**self.writer_config)
            .partitionBy(*self.config.partition_by)
            .format(self.config.storage.value)
        )

    @property
    def uri(self: Self) -> ParseResult:
        """Parse string path into a URI."""
        return urlparse(self.config.path)

    @property
    def filesystem(self) -> FileSystem:
        """Get current FS based on URI and configuration."""
        if self.uri.scheme == "s3":
            if not self.config.aws_credentials:
                raise InvalidConfigException("No S3 credentials provided")

            return S3FileSystem(
                access_key=self.config.aws_credentials.access_key,
                secret_key=self.config.aws_credentials.secret_key,
                endpoint_override=f"{self.uri.hostname}:{self.uri.port}",
                allow_bucket_creation=True,
                allow_bucket_deletion=True
            )

        fs, _ = FileSystem.from_uri(uri=self.uri.geturl())  # type: ignore

        return fs

    @property
    def absolute_path(self) -> str:
        """Returns absolute path."""
        return str(Path(self.uri.path).absolute()) \
            if isinstance(self.filesystem, LocalFileSystem) \
            else self.uri.path

    @property
    def base_path(self) -> str:
        """Returns base path."""
        return str(Path(self.absolute_path).parent)

    @property
    def has_wildcard(self: Self) -> bool:
        """Verifies if path has a wildcard."""
        return Path(self.absolute_path).name.endswith("*")

    def list_partitions(self: Self) -> list[str]:
        """Return all found partitions."""
        selector = FileSelector(self.absolute_path)

        return [
            x.path
            for x
            in self.filesystem.get_file_info(selector)
            if x.type.name == "Directory"
        ]

    def write(self: Self, data: DataFrame) -> None:
        """Write data into FileSystem."""
        if self.has_wildcard:
            raise InvalidConfigException("Can't write to wildcard path")

        self._writer(data).save(self.absolute_path)

    def drop(self: Self) -> None:
        """Remove data files from the table directory."""
        self.filesystem.delete_dir(self.absolute_path)

    def read(self: Self) -> DataFrame:
        """Read data from FileSystem into DataFrame."""
        return (
            self._reader
            .format(self.config.storage.value)
            .load(self.absolute_path)
        )

    def read_partitions(self, partitions: list[str]) -> DataFrame:
        """Read specific partitions from data source."""
        return (
            self._reader
            .option("basePath", self.base_path)
            .format(self.config.storage.value)
            .load(partitions)
        )
