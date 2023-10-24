"""Unit testing for pysetl.storage.connector module."""
from tempfile import TemporaryDirectory
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyarrow.fs import LocalFileSystem, S3FileSystem, FileType
from pysetl.enums import FileStorage
from pysetl.utils.exceptions import BadConfiguration, InvalidConnectorException
from pysetl.storage.connector import (
    FileConnector, CsvConnector, ParquetConnector, JsonConnector,
    ConnectorBuilder
)
from pysetl.config import (
    FileConfig, AwsCredentials, CsvConfig, JsonConfig, ParquetConfig
)


def test_file_connector_s3():
    file_config = FileConfig(
        storage=FileStorage.CSV,
        path="s3://my-bucket/data",
        aws_credentials=AwsCredentials(
            access_key="",
            secret_key="",
            session_token="",
            credentials_provider=""
        )
    )
    s3_file_connector = FileConnector(file_config)

    assert isinstance(s3_file_connector.filesystem, S3FileSystem)


def test_file_connector_s3_bad_aws_config():
    file_config = FileConfig(
        storage=FileStorage.CSV,
        path="s3://my-bucket/data"
    )
    s3_file_connector = FileConnector(file_config)

    with pytest.raises(BadConfiguration) as error:
        _ = s3_file_connector.filesystem

    assert str(error.value) == "No S3 credentials provided"


def test_file_connector_paths():
    config = FileConfig(
        storage=FileStorage.CSV,
        path="/ruta/al/archivo"
    )
    connector = FileConnector(config)

    assert connector.absolute_path == "/ruta/al/archivo"
    assert connector.base_path == "/ruta/al"
    assert connector.uri.scheme == ""
    assert not connector.has_wildcard


def test_file_connector_write_on_wildcard():
    # Create a SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Create a DataFrame
    columns = ["language", "users_count"]
    data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
    dataframe = spark.sparkContext.parallelize(data).toDF(columns)

    config = FileConfig(
        storage=FileStorage.CSV,
        path="/ruta/al/archivo/*"
    )
    connector = FileConnector(config)

    with pytest.raises(BadConfiguration) as error:
        connector.write(dataframe)

    assert str(error.value) == "Can't write to wildcard path"


def test_file_connector():
    # Create a SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Create a DataFrame
    columns = ["language", "users_count"]
    data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
    dataframe = spark.sparkContext.parallelize(data).toDF(columns)

    # Create FileConfig pointing to a tempdir
    with TemporaryDirectory() as path:
        config = FileConfig(
            storage=FileStorage.CSV,
            path=path,
            header="true"
        )

    # Instantitate file Connector
    file_connector = FileConnector(config)
    local_fs = file_connector.filesystem

    assert file_connector
    assert isinstance(local_fs, LocalFileSystem)

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


def test_file_connector_with_partitions():
    # Create a SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Create a DataFrame
    columns = ["language", "users_count"]
    data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
    dataframe = spark.sparkContext.parallelize(data).toDF(columns)

    # Create FileConfig pointing to a tempdir
    with TemporaryDirectory() as path:
        config = FileConfig(
            storage=FileStorage.CSV,
            path=path,
            header="true",
            partition_by=["language"],
            data_schema=StructType([
                StructField("language", StringType(), False),
                StructField("users_count", StringType(), False),
            ])
        )

    # Instantitate file Connector
    file_connector = FileConnector(config)
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


def test_file_connector_bad_partition_config():
    # Create a SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Create a DataFrame
    columns = ["language", "users_count"]
    data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
    dataframe = spark.sparkContext.parallelize(data).toDF(columns)

    # Create FileConfig pointing to a tempdir
    with TemporaryDirectory() as path:
        config = FileConfig(
            storage=FileStorage.CSV,
            path=path,
            header="true",
            partition_by=["non_existent"]
        )

    # Instantitate file Connector
    file_connector = FileConnector(config)
    local_fs = file_connector.filesystem

    assert file_connector
    assert isinstance(local_fs, LocalFileSystem)

    # Write dataframe
    with pytest.raises(BadConfiguration) as error:
        file_connector.write(dataframe)

    assert str(error.value) == "Partition columns in configuration not in data"


def test_csv_connector():
    config = CsvConfig(path="/ruta/al/archivo")

    assert CsvConnector(config)


def test_csv_connector_bad_config():
    config = ParquetConfig(path="/ruta/al/archivo")

    with pytest.raises(BadConfiguration) as error:
        _ = CsvConnector(config)

    assert str(error.value) == "Not a CsvConfig for a CsvConnector"


def test_json_connector():
    config = JsonConfig(path="/ruta/al/archivo")

    assert JsonConnector(config)


def test_json_connector_bad_config():
    config = ParquetConfig(path="/ruta/al/archivo")

    with pytest.raises(BadConfiguration) as error:
        _ = JsonConnector(config)

    assert str(error.value) == "Not a JsonConfig for a JsonConnector"


def test_parquet_connector():
    config = ParquetConfig(path="/ruta/al/archivo")

    assert ParquetConnector(config)


def test_parquet_connector_bad_config():
    config = CsvConfig(path="/ruta/al/archivo")

    with pytest.raises(BadConfiguration) as error:
        _ = ParquetConnector(config)

    assert str(error.value) == "Not a ParquetConfig for a ParquetConnector"


def test_connector_builder_csv():
    config = FileConfig(
        storage=FileStorage.CSV,
        path="/ruta/al/archivo",
        header="true"
    )

    connector = ConnectorBuilder(config).build().get()

    assert isinstance(connector, CsvConnector)


def test_connector_builder_json():
    config = FileConfig(
        storage=FileStorage.JSON,
        path="/ruta/al/archivo"
    )

    connector = ConnectorBuilder(config).build().get()

    assert isinstance(connector, JsonConnector)


def test_connector_builder_parquet():
    config = FileConfig(
        storage=FileStorage.PARQUET,
        path="/ruta/al/archivo"
    )

    connector = ConnectorBuilder(config).build().get()

    assert isinstance(connector, ParquetConnector)


def test_connector_builder_not_built():
    config = FileConfig(
        storage=FileStorage.CSV,
        path="/ruta/al/archivo"
    )

    with pytest.raises(InvalidConnectorException) as error:
        _ = ConnectorBuilder(config).get()

    assert str(error.value) == "Connector is not defined"
