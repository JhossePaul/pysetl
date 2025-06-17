"""Test pysetl.storage.repository."""
import pytest
from typedspark import DataSet
from pyarrow.fs import FileType


from pysetl.storage.repository import SparkRepository, SparkRepositoryBuilder
from pysetl.storage.connector import ParquetConnector
from pysetl.utils.exceptions import InvalidConnectorException, BuilderException

from tests.dummy_schemas import DummyData
from tests.conftest import DummyConnector


@pytest.mark.spark
def test_sparkrepository(dataframe, parquet_config):
    """Test repository basis functionalities."""
    dataset = DataSet[DummyData](dataframe)
    builder = SparkRepositoryBuilder[DummyData](parquet_config, False)
    sparkrepository = builder.build().getOrCreate().set_cache(True)

    connector = sparkrepository.connector
    fs = getattr(connector, "filesystem")

    assert str(sparkrepository) == "SparkRepository[DummyData]"
    assert str(sparkrepository) == repr(sparkrepository)
    assert sparkrepository.cache is True
    assert isinstance(sparkrepository.connector, ParquetConnector)
    assert (
        getattr(sparkrepository, "__orig_class__") ==
        SparkRepository[DummyData]
    )

    sparkrepository.cache = False

    assert sparkrepository.cache is False

    # Write dataframe
    sparkrepository.save(dataset)
    output_type = fs.get_file_info(parquet_config.config.path).type

    assert output_type == FileType.Directory

    # Read dataframe
    data_from_fs = sparkrepository.load()

    assert isinstance(data_from_fs, DataSet)
    assert data_from_fs.typedspark_schema is DummyData

    # Compare with original dataset
    assert dataset.exceptAll(data_from_fs).count() == 0

    # Remove data
    sparkrepository.drop()
    type_after_drop = fs.get_file_info(parquet_config.config.path)
    assert type_after_drop.type == FileType.NotFound


@pytest.mark.spark
def test_sparkrepository_cached(dataframe, parquet_config):
    """Test repository with cached data."""
    builder = SparkRepositoryBuilder[DummyData](parquet_config, True)
    sparkrepository = builder.build().getOrCreate()
    connector = sparkrepository.connector
    fs = getattr(connector, "filesystem")

    assert sparkrepository
    assert sparkrepository.cache is True

    # Write dataframe
    dataset = DataSet[DummyData](dataframe)
    sparkrepository.save(dataset)
    output_type = fs.get_file_info(parquet_config.config.path).type

    assert output_type == FileType.Directory

    # Read dataframe
    data_from_fs = sparkrepository.load()

    assert isinstance(data_from_fs, DataSet)
    assert data_from_fs.typedspark_schema is DummyData

    # Second load should get cached data
    data_from_fs_cached = sparkrepository.load()

    assert data_from_fs_cached.is_cached is True

    # Compare with original dataframe
    assert dataframe.exceptAll(data_from_fs).count() == 0

    # Remove data
    sparkrepository.drop()
    type_after_drop = fs.get_file_info(parquet_config.config.path)
    assert type_after_drop.type == FileType.NotFound


@pytest.mark.spark
def test_sparkrepository_partitioned(dataframe, parquet_config):
    """Test partitioned repository."""
    parquet_config.set("partition_by", ["language"])
    builder = SparkRepositoryBuilder[DummyData](parquet_config, True)
    sparkrepository = builder.build().getOrCreate()
    connector = sparkrepository.connector
    fs = getattr(connector, "filesystem")

    assert sparkrepository
    assert sparkrepository.cache is True

    # Write dataframe
    dataset = DataSet[DummyData](dataframe)
    sparkrepository.save(dataset)
    output_type = fs.get_file_info(parquet_config.config.path).type
    assert output_type == FileType.Directory

    # Validate partitions
    partitions = sparkrepository.list_partitions()
    output_type = fs.get_file_info(parquet_config.config.path).type

    assert output_type == FileType.Directory

    assert all([
        fs.get_file_info(p).type == FileType.Directory
        for p
        in partitions
    ])
    assert set([
        p.replace(parquet_config.config.path, "")
        for p
        in partitions
    ]) == set(["/language=Scala", "/language=Java", "/language=Python"])

    # Read dataframe
    data_from_fs = sparkrepository.load_partitions(partitions)

    assert isinstance(data_from_fs, DataSet)
    assert data_from_fs.typedspark_schema is DummyData

    # Compare with original dataframe
    assert (
        dataframe
        .select("users_count", "language")
        .exceptAll(data_from_fs)
        .count()
    ) == 0

    # Remove data
    sparkrepository.drop()


@pytest.mark.spark
def test_sparkrepository_fails_no_mixins():
    """Test exceptions."""
    connector = DummyConnector()
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


@pytest.mark.spark
def test_sparkrepository_load_fails_with_no_type(parquet_config):
    """Throw error if no type parameter passed."""
    builder = SparkRepositoryBuilder(parquet_config, False)

    with pytest.raises(AttributeError) as error:
        _ = builder.build().get()

    assert (
        str(error.value) ==
        "'SparkRepositoryBuilder' object has no attribute '__orig_class__'"
    )


@pytest.mark.spark
def test_builder_exception(parquet_config):
    """Throw a Builder exception if get before built."""
    builder = SparkRepositoryBuilder(parquet_config)
    with pytest.raises(BuilderException) as error:
        _ = builder.get()

    assert str(error.value) == "No SparkRepository built"
