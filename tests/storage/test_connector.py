"""Unit testing for pysetl.storage.connector module."""
from os.path import dirname

import pytest

from pyspark.sql.types import StructType, StructField, StringType
from pyarrow.fs import LocalFileSystem, S3FileSystem, FileType

from pysetl.enums import FileStorage
from pysetl.storage.connector import (
    FileConnector, CsvConnector, ParquetConnector, JsonConnector,
    ConnectorBuilder
)
from pysetl.utils.exceptions import InvalidConfigException, BuilderException


@pytest.mark.spark
def test_file_connector_s3(s3_file_config):
    """Test if Connector builds a S3FileSystem if S3 URL passed."""
    s3_file_connector = FileConnector(s3_file_config)

    assert isinstance(s3_file_connector.filesystem, S3FileSystem)


@pytest.mark.spark
def test_file_connector_s3_bad_aws_config(s3_file_config):
    """A connector should throw an InvalidConfigException if S3 Filesystem
    expected but no credentials provided."""
    s3_file_config.set("aws_credentials", None)
    s3_file_connector = FileConnector(s3_file_config)

    with pytest.raises(InvalidConfigException) as error:
        _ = s3_file_connector.filesystem

    assert str(error.value) == "No S3 credentials provided"


@pytest.mark.spark
def test_file_connector_write_on_wildcard(dataframe, local_file_config):
    """A connector should fail if write on a wildcard path."""
    local_file_config.set("path", local_file_config.config.path + "*")
    connector = FileConnector(local_file_config)

    with pytest.raises(InvalidConfigException) as error:
        connector.write(dataframe)

    assert str(error.value) == "Can't write to wildcard path"


@pytest.mark.spark
def test_file_connector(dataframe, local_file_config):
    """Test Connector basic functionalities."""
    # Instantitate file Connector
    file_connector = FileConnector(local_file_config)
    local_fs = file_connector.filesystem

    assert file_connector
    assert isinstance(local_fs, LocalFileSystem)
    assert file_connector.absolute_path == local_file_config.config.path
    assert file_connector.base_path == dirname(local_file_config.config.path)
    assert file_connector.uri.scheme == ""
    assert not file_connector.has_wildcard

    # Write dataframe
    file_connector.write(dataframe)
    output_type = local_fs.get_file_info(file_connector.config.path).type
    assert output_type == FileType.Directory

    # Read dataframe
    data_from_fs = file_connector.read()

    # Compare with original dataframe
    assert dataframe.exceptAll(data_from_fs).count() == 0

    # Remove data
    file_connector.drop()
    type_after_drop = local_fs.get_file_info(file_connector.absolute_path)
    assert type_after_drop.type == FileType.NotFound


@pytest.mark.spark
def test_file_connector_with_partitions(dataframe, local_file_config):
    """Test Connector functionalities with partitions."""
    local_file_config.set("partition_by", ["language"])
    local_file_config.set(
        "data_schema",
        StructType([
            StructField("language", StringType(), False),
            StructField("users_count", StringType(), False),
        ])
    )

    # Instantitate file Connector
    file_connector = FileConnector(local_file_config)
    local_fs = file_connector.filesystem

    assert file_connector
    assert isinstance(local_fs, LocalFileSystem)

    # Write dataframe
    file_connector.write(dataframe)

    # Validate partitions
    partitions = file_connector.list_partitions()
    output_type = local_fs.get_file_info(file_connector.config.path).type
    assert output_type == FileType.Directory

    assert all([
        local_fs.get_file_info(p).type == FileType.Directory
        for p
        in partitions
    ])
    assert set([
        p.replace(file_connector.absolute_path, "")
        for p
        in partitions
    ]) == set(["/language=Scala", "/language=Java", "/language=Python"])

    # Read dataframe
    data_from_fs = file_connector.read_partitions(partitions)

    # Compare with original dataframe
    assert (
        dataframe
        .select("users_count", "language")
        .exceptAll(data_from_fs)
        .count()
    ) == 0

    # Remove data
    file_connector.drop()
    type_after_drop = local_fs.get_file_info(file_connector.absolute_path)
    assert type_after_drop.type == FileType.NotFound


@pytest.mark.spark
def test_file_connector_bad_partition_config(dataframe, local_file_config):
    """A connector should fail if config partitions not in dataframe."""
    local_file_config.set("partition_by", ["non_existent"])

    # Instantitate file Connector
    file_connector = FileConnector(local_file_config)
    local_fs = file_connector.filesystem

    assert file_connector
    assert isinstance(local_fs, LocalFileSystem)

    # Write dataframe
    with pytest.raises(InvalidConfigException) as error:
        file_connector.write(dataframe)

    assert str(error.value) == "Partition columns in configuration not in data"


@pytest.mark.spark
def test_child_file_connectors(csv_config, parquet_config, json_config):
    assert CsvConnector(csv_config)
    assert JsonConnector(json_config)
    assert ParquetConnector(parquet_config)


@pytest.mark.spark
def test_csv_connector_bad_config(parquet_config):
    with pytest.raises(InvalidConfigException) as error:
        _ = CsvConnector(parquet_config)

    assert str(error.value) == "Not a CsvConfig for a CsvConnector"


@pytest.mark.spark
def test_json_connector_bad_config(parquet_config):
    with pytest.raises(InvalidConfigException) as error:
        _ = JsonConnector(parquet_config)

    assert str(error.value) == "Not a JsonConfig for a JsonConnector"




@pytest.mark.spark
def test_parquet_connector_bad_config(csv_config):
    with pytest.raises(InvalidConfigException) as error:
        _ = ParquetConnector(csv_config)

    assert str(error.value) == "Not a ParquetConfig for a ParquetConnector"


@pytest.mark.spark
def test_connector_builder_csv(local_file_config):
    connector = ConnectorBuilder(local_file_config).build().get()

    assert isinstance(connector, CsvConnector)


@pytest.mark.spark
def test_connector_builder_json(local_file_config):
    local_file_config.set("storage", FileStorage.JSON)
    connector = ConnectorBuilder(local_file_config).build().get()

    assert isinstance(connector, JsonConnector)


@pytest.mark.spark
def test_connector_builder_parquet(local_file_config):
    local_file_config.set("storage", FileStorage.PARQUET)
    connector = ConnectorBuilder(local_file_config).build().get()

    assert isinstance(connector, ParquetConnector)


@pytest.mark.spark
def test_connector_builder_not_built(local_file_config):
    with pytest.raises(BuilderException) as error:
        _ = ConnectorBuilder(local_file_config).get()

    assert str(error.value) == "Connector is not defined"
