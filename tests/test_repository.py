"""Test pysetl.storage.repository."""
from tempfile import TemporaryDirectory
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from typedspark import Schema, DataSet, Column
from pyarrow.fs import FileType
from pysetl.storage.repository import SparkRepository, SparkRepositoryBuilder
from pysetl.storage.connector import ParquetConnector, Connector
from pysetl.enums import FileStorage
from pysetl.config import FileConfig
from pysetl.utils.exceptions import InvalidConnectorException


def get_data():
    """Return dummy data for tests."""
    spark = SparkSession.builder.getOrCreate()

    return (
        spark.sparkContext
        .parallelize([
            ("Java", "20000"),
            ("Python", "100000"),
            ("Scala", "3000")
        ])
        .toDF(["language", "users_count"])
    )


class DummyData(Schema):
    """Dummy schema for testing purposes."""

    language: Column[StringType]
    users_count: Column[StringType]


class TestConnector(Connector):
    """Dummy connector for testing purposes."""

    def read(self):
        """Test repository."""
        return NotImplemented

    def write(self, data):
        """Test repository."""
        return NotImplemented


def test_sparkrepository():
    """Test repository basis functionalities."""
    dataframe = get_data()
    dataset = DataSet[DummyData](dataframe)
    with TemporaryDirectory() as _dir:
        config = FileConfig(
            storage=FileStorage.PARQUET,
            path=_dir
        )

    builder = SparkRepositoryBuilder[DummyData](config, False)
    sparkrepository = builder.build().getOrCreate().set_cache(True)

    connector = sparkrepository.connector
    fs = connector.filesystem

    assert str(sparkrepository) == "SparkRepository[DummyData]"
    assert str(sparkrepository) == repr(sparkrepository)
    assert sparkrepository.cache is True
    assert isinstance(sparkrepository.connector, ParquetConnector)
    assert (
        sparkrepository.__orig_class__ ==
        SparkRepository[DummyData]
    )

    sparkrepository.cache = False

    assert sparkrepository.cache is False

    # Write dataframe
    sparkrepository.save(dataset)
    output_type = fs.get_file_info(config.config.path).type
    assert output_type == FileType.Directory

    # Read dataframe
    data_from_fs = sparkrepository.load()

    assert data_from_fs.__orig_class__ == DataSet[DummyData]

    # Compare with original dataframe
    assert dataframe.exceptAll(data_from_fs).count() == 0

    # Remove data
    sparkrepository.drop()
    type_after_drop = fs.get_file_info(config.config.path)
    assert type_after_drop.type == FileType.NotFound


def test_sparkrepository_cached():
    """Test repository with cached data."""
    with TemporaryDirectory() as _dir:
        config = FileConfig(
            storage=FileStorage.PARQUET,
            path=_dir
        )

    builder = SparkRepositoryBuilder[DummyData](config, True)
    sparkrepository = builder.build().getOrCreate()
    connector = sparkrepository.connector
    fs = connector.filesystem

    assert sparkrepository
    assert sparkrepository.cache is True

    # Write dataframe
    dataframe = get_data()
    dataset = DataSet[DummyData](dataframe)
    sparkrepository.save(dataset)
    output_type = fs.get_file_info(config.config.path).type
    assert output_type == FileType.Directory

    # Read dataframe
    data_from_fs = sparkrepository.load()

    assert data_from_fs.__orig_class__ == DataSet[DummyData]

    # Second load should get cached data
    data_from_fs_cached = sparkrepository.load()

    assert data_from_fs_cached.is_cached is True

    # Compare with original dataframe
    assert dataframe.exceptAll(data_from_fs).count() == 0

    # Remove data
    sparkrepository.drop()
    type_after_drop = fs.get_file_info(config.config.path)
    assert type_after_drop.type == FileType.NotFound


def test_sparkrepository_partitioned():
    """Test partitioned repository."""
    with TemporaryDirectory() as _dir:
        config = FileConfig(
            storage=FileStorage.PARQUET,
            path=_dir,
            partition_by=["language"]
        )

    builder = SparkRepositoryBuilder[DummyData](config, True)
    sparkrepository = builder.build().getOrCreate()
    connector = sparkrepository.connector
    fs = connector.filesystem

    assert sparkrepository
    assert sparkrepository.cache is True

    # Write dataframe
    dataframe = get_data()
    dataset = DataSet[DummyData](dataframe)
    sparkrepository.save(dataset)
    output_type = fs.get_file_info(config.config.path).type
    assert output_type == FileType.Directory

    # Validate partitions
    partitions = sparkrepository.list_partitions()
    output_type = fs.get_file_info(config.config.path).type
    assert output_type == FileType.Directory

    assert all([
        fs.get_file_info(p).type == FileType.Directory
        for p
        in partitions
    ])
    assert set([
        p.replace(config.config.path, "")
        for p
        in partitions
    ]) == set(["/language=Scala", "/language=Java", "/language=Python"])

    # Read dataframe
    data_from_fs = sparkrepository.load_partitions(partitions)

    assert data_from_fs.__orig_class__ == DataSet[DummyData]

    # Compare with original dataframe
    assert (
        dataframe
        .select("users_count", "language")
        .exceptAll(data_from_fs)
        .count()
    ) == 0

    # Remove data
    sparkrepository.drop()


def test_sparkrepository_fails_no_mixins():
    """Test exceptions."""
    connector = TestConnector()
    sparkrepository = SparkRepository[DummyData](connector)

    with pytest.raises(InvalidConnectorException) as error:
        sparkrepository.list_partitions()

    assert str(error.value) == "Current connector doesn't support partitions"

    with pytest.raises(InvalidConnectorException) as error:
        sparkrepository.load_partitions([])

    assert str(error.value) == "Current connector doesn't support partitions"

    with pytest.raises(InvalidConnectorException) as error:
        sparkrepository.drop()

    assert str(error.value) == "Current connector doesn't support drop"


def test_sparkrepository_load_fails_with_no_type():
    """Throw error if no type parameter passed."""
    config = FileConfig(
        storage=FileStorage.PARQUET,
        path="/ruta/al/archivo"
    )

    builder = SparkRepositoryBuilder(config, False)

    with pytest.raises(AttributeError) as error:
        _ = builder.build().get()

    assert (
        str(error.value) ==
        "'SparkRepositoryBuilder' object has no attribute '__orig_class__'"
    )
