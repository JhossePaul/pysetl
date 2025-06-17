"""Test pysetl.workflow.stage module."""
from tempfile import TemporaryDirectory
import pytest
from pyarrow.fs import FileType
from pyspark.sql.types import StringType
from pyspark.sql import Row, SparkSession
from typedspark import DataSet, Column, Schema
from pysetl.workflow import Stage, Factory
from pysetl.workflow.node import Node
from pysetl.storage.connector import CsvConnector
from pysetl.config import CsvConfig
from pysetl.utils.exceptions import AlreadyExistsException
from pysetl.utils.mixins import HasSparkSession
from pysetl.enums import SaveMode


class Persisted(Schema):
    """Schema for testing purposes."""

    col: Column[StringType]


class PersistanceFactory(Factory[DataSet[Persisted]], HasSparkSession):
    """Factory for testing purposes."""

    def __init__(self, connector: CsvConnector):
        super().__init__()
        dataframe = self.spark.createDataFrame(
            [Row(col="a"), Row(col="b")],
            ["col"]
        )
        self.connector = connector
        self.output = DataSet[Persisted](dataframe)

    def read(self):
        """Return itself."""
        return self

    def process(self):
        """Return itself."""
        return self

    def write(self):
        """Persist data and return itself."""
        self.connector.write(self.output)

        return self

    def get(self):
        """Return output."""
        return self.output


class IntFactory(Factory[int]):
    """Factory for testing purposes."""

    output = 1

    def read(self):
        """Return itself."""
        return self

    def process(self):
        """Return itself."""
        return self

    def write(self):
        """Return itself."""
        return self

    def get(self):
        """Return output."""
        return self.output


def test_stage_exceptions():
    """Test exceptions in Stage class."""
    factory = IntFactory()
    stage = Stage()

    with pytest.raises(AlreadyExistsException) as error:
        stage.add_factory(factory).add_factory(factory)

    assert "of type IntFactory already exists" in str(error.value)


def test_no_spark_stage():
    """Simple stage with no spark session."""
    spark = SparkSession.Builder().getOrCreate()
    spark.stop()

    factory = IntFactory()
    stage = Stage().add_factory(factory).run()
    [node, *_] = stage.create_nodes()

    assert stage.deliverable[0].payload == 1
    assert node == Node.from_factory(factory, 0, True)


def test_stage():
    """Test Stage class."""
    _ = SparkSession.Builder().getOrCreate()

    with TemporaryDirectory() as _dir:
        config = CsvConfig(
            path=_dir,
            infer_schema="true",
            delimiter="|",
            header="true",
            savemode=SaveMode.APPEND
        )
    connector = CsvConnector(config)
    fs = connector.filesystem
    stage = (
        Stage(True)
        .writable(False)
        .add_factory_from_type(PersistanceFactory, connector)
    )
    stage.stage_id = 1

    assert stage.benchmarked
    assert not stage.start
    assert stage.end
    assert stage.stage_id == 1
    assert not stage.deliverable
    assert "PersistanceFactory -> DataSet[Persisted]" == str(stage)
    assert repr(stage) == str(stage)

    stage.run()
    assert fs.get_file_info(config.config.path).type == FileType.NotFound
    assert stage.deliverable[0].payload_type == DataSet[Persisted]

    stage.writable(True).run()
    assert fs.get_file_info(config.config.path).type == FileType.Directory

    connector.drop()
    assert fs.get_file_info(config.config.path).type == FileType.NotFound

    benchmark = stage.get_benchmarks()

    assert len(benchmark) == 2

    not_writing_benchmark, writing_benchmark, *_ = benchmark
    assert not_writing_benchmark.total < writing_benchmark.total
