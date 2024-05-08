"""Test PySetl main constructor."""
from tempfile import TemporaryDirectory

import pytest

from pyspark import SparkConf
from pyspark.sql import SparkSession, Row

from typedspark import create_partially_filled_dataset

from pysetl import PySetl
from pysetl.enums import SaveMode
from pysetl.config import CsvConfig, ParquetConfig
from pysetl.config.config import Config
from pysetl.storage.repository import SparkRepositoryBuilder
from pysetl.utils.exceptions import InvalidConfigException, BuilderException

from tests.dummy_schemas import DSSchema, DUMMY_DATA, DS3Schema
from tests.dummy_factories import RepositoryLoad, ProducerFactory, ConsumerFactory


def test_pysetl_has_spark_session():
    """Test PySelt should have SparkSession."""
    spark = SparkSession.getActiveSession()

    if spark:
        spark.stop()

    spark_conf = (
        SparkConf()
        .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    config_dict = {
        "spark": Config.builder().from_dict({
            "spark.app.name": "my_app",
            "spark.sql.shuffle.partitions": "1000"
        })
    }

    pysetl = (
        PySetl.builder()
        .set_spark_master("local[2]")
        .set_config(config_dict)
        .set_spark_config(spark_conf)
        .set_shuffle_partitions(500)
        .build()
        .getOrCreate()
    )
    conf = pysetl.spark.sparkContext.getConf()

    assert pysetl.spark.sparkContext.appName == "my_app"
    assert conf.get("spark.sql.sources.partitionOverwriteMode") == "dynamic"
    assert conf.get("spark.sql.shuffle.partitions") == "500"


def test_pysetl_sparkrepository():
    """Test if PySetl can create SparkRepository."""

    _dir = TemporaryDirectory()

    config = {
        "csv_config": CsvConfig(
            path=_dir.name,
            inferSchema="true",
            delimiter=";",
            header="true",
            savemode=SaveMode.APPEND
        )
    }

    pysetl = PySetl.builder().set_config(config).getOrCreate()
    ds = create_partially_filled_dataset(pysetl.spark, DSSchema, DUMMY_DATA["DSSchema"])

    repo = pysetl.get_sparkrepository(DSSchema, "csv_config")

    if repo:
        loaded_data = repo.save(ds).load()

        assert loaded_data.count() == 2

        repo.drop()


def test_pysetl_set_sparkrepository():
    """Test PySetl sets SparksRepository."""
    _dir = TemporaryDirectory()
    config = {
        "csv_config": CsvConfig(
            path=_dir.name,
            inferSchema="true",
            delimiter=";",
            header="true",
            savemode=SaveMode.APPEND
        )
    }
    pysetl = (
        PySetl.builder()
        .set_config(config)
        .getOrCreate()
        .set_spark_repository_from_config(
            DSSchema,
            "csv_config",
            delivery_id="id"
        )
    )
    assert "csv_config" in pysetl.list_inputs()
    assert len(pysetl.list_inputs()) == 1
    assert pysetl.has_external_input("csv_config")
    assert not pysetl.has_external_input("bad_config")

    ds = create_partially_filled_dataset(pysetl.spark, DSSchema, DUMMY_DATA["DSSchema"])
    repo = pysetl.get_sparkrepository(DSSchema, "csv_config")

    assert repo

    repo.save(ds)

    _ = pysetl.new_pipeline().add_stage_from_type(RepositoryLoad).run()

    repo.drop()


def test_pysetl_pipelines():
    """Test PySetl multiple repositories in Pipeline."""
    _dir_csv = TemporaryDirectory()
    _dir_parquet = TemporaryDirectory()
    config = {
        "csv_config": CsvConfig(
            path=_dir_csv.name,
            inferSchema="true",
            delimiter=";",
            header="true",
            savemode=SaveMode.APPEND
        ),
        "parquet_config": ParquetConfig(
            path=_dir_parquet.name,
            savemode=SaveMode.APPEND
        )
    }
    pysetl = (
        PySetl.builder()
        .set_config(config)
        .getOrCreate()
        .set_spark_repository_from_config(DSSchema, "csv_config")
        .set_spark_repository_from_config(DS3Schema, "parquet_config")
    )
    _ = (
        pysetl
        .new_pipeline()
        .add_stage_from_type(ProducerFactory)
        .add_stage_from_type(ConsumerFactory)
        .run()
    )
    repo_producer = pysetl.get_sparkrepository(DSSchema, "csv_config")
    repo_consumer = pysetl.get_sparkrepository(DS3Schema, "parquet_config")

    assert repo_producer
    assert repo_consumer
    assert repo_producer.load().count() == 2
    assert repo_consumer.load().count() == 2
    assert len(repo_producer.load().columns) == 4
    assert len(repo_consumer.load().columns) == 5

    repo_producer.drop()
    repo_consumer.drop()


def test_pysetl_sparkrepository_with_consumers():
    """Test PySetl differentiate between repositories by consumer list."""

    _dir_csv = TemporaryDirectory()
    _dir_parquet = TemporaryDirectory()
    config = {
        "csv_config": CsvConfig(
            path=_dir_csv.name,
            inferSchema="true",
            delimiter=";",
            header="true",
            savemode=SaveMode.APPEND
        ),
        "parquet_config": ParquetConfig(
            path=_dir_parquet.name,
            savemode=SaveMode.APPEND
        )
    }
    pysetl = (
        PySetl.builder()
        .set_config(config)
        .getOrCreate()
        .set_spark_repository_from_config(
            DSSchema,
            "csv_config",
            set([ProducerFactory])
        )
        .set_spark_repository_from_config(DSSchema, "parquet_config")
    )

    _ = (
        pysetl
        .new_pipeline()
        .add_stage_from_type(ProducerFactory)
        .run()
    )
    repo = pysetl.get_sparkrepository(DSSchema, "csv_config")

    assert repo
    assert repo.connector.config
    assert repo.connector.config.model_dump().get("path") == _dir_csv.name

    repo.drop()


def test_pysetl_set_repository():
    """Test PySetl with instanciated repositories."""
    pysetl = PySetl.builder().getOrCreate()
    _dir = TemporaryDirectory().name
    config = CsvConfig(path=_dir)
    repository = SparkRepositoryBuilder[DSSchema](config).getOrCreate()
    pipeline = (
        pysetl
        .set_spark_repository(repository, "factory1", set([ProducerFactory]))
        .set_spark_repository(repository, "factory1")
        .new_pipeline()
        .add_stage_from_type(ProducerFactory)
        .run()
    )
    registered = pysetl.get_pipeline(pipeline.uuid)
    deliverable = pysetl.external_inputs.get("factory1")

    assert deliverable and deliverable.consumers == set([ProducerFactory])
    assert len(pysetl.list_inputs()) == 1
    assert pipeline.get_last_output().count() == 2
    assert pipeline.get_last_output().collect() == [
        Row(partition1=1, partition2="a", clustering1="A", value=1),
        Row(partition1=2, partition2="b", clustering1="B", value=2)
    ]
    assert registered and len(registered.stages) == 1

    repository.drop()


def test_pysetl_set_sparksession():
    """Test PySetl SparkSession can be configurated and passed."""
    spark = SparkSession.getActiveSession()

    if spark:
        spark.stop()

    spark_config = (
        SparkConf()
        .setMaster("local")
        .setAppName("setl_test_app")
        .set("key", "value")
    )
    session = SparkSession.Builder().config(conf=spark_config).getOrCreate()
    pysetl = PySetl.builder().set_spark_session(session).getOrCreate()

    assert pysetl.spark.sparkContext.getConf().get("key") == "value"
    assert not pysetl.spark.sparkContext.getConf().get("no_key")


def test_pysetl_set_sparkconf():
    """Test PySetl SparkConf can be passed."""

    spark = SparkSession.getActiveSession()

    if spark:
        spark.stop()

    spark_config = (
        SparkConf()
        .setMaster("local")
        .setAppName("setl_test_app")
        .set("key", "value")
    )
    pysetl = PySetl.builder().set_spark_config(spark_config).getOrCreate()

    assert pysetl.spark.sparkContext.getConf().get("key") == "value"
    assert not pysetl.spark.sparkContext.getConf().get("no_key")

    pysetl.stop()


def test_pysetl_exceptions():
    """Throw InvalidConfigException if no config found."""

    with pytest.raises(BuilderException) as error:
        _ = PySetl.builder().get()

    assert str(error.value) == "No PySetl object built"

    pysetl = PySetl.builder().getOrCreate()

    with pytest.raises(InvalidConfigException) as error:
        pysetl.set_spark_repository_from_config(DSSchema, "non_existent")

    assert str(error.value) == "No config found with key: non_existent"
