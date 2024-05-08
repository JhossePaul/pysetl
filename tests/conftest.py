"""Pytest fixtures and classes."""
import os
import sys

import pytest

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


from pysetl.config import CsvConfig, AwsCredentials, FileConfig, ParquetConfig, JsonConfig
from pysetl.storage.connector import Connector
from pysetl.enums import FileStorage, SaveMode


@pytest.fixture(scope="session")
def spark():
    """Fixture for creating a spark session."""

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    _spark = SparkSession.Builder().getOrCreate()
    yield _spark
    _spark.stop()


@pytest.fixture
def csv_config(tmp_path) -> CsvConfig:
    """Mock CSV configuration."""

    return CsvConfig(
        path=str(tmp_path),
        partition_by=[],
        savemode=SaveMode.ERRORIFEXISTS,
        data_schema=StructType(),
        inferSchema="false",
        header="true",
        delimiter=","
    )


@pytest.fixture
def config_dict(tmp_path):
    """Dictionary to build a configuration."""

    return {
        "storage": FileStorage.CSV,
        "path": str(tmp_path),
        "partition_by": [],
        "savemode": SaveMode.ERRORIFEXISTS,
        "data_schema": StructType()
    }


@pytest.fixture
def config_object(tmp_path):
    """Object to build a configuration."""

    class ConfigObject:
        """Test simple config object."""
        path = str(tmp_path)

    return ConfigObject


@pytest.fixture
def aws_credentials():
    """Dummy AWS S3 credentials."""

    return AwsCredentials(
        access_key="this",
        secret_key="should",
        session_token="work",
        credentials_provider="smoothly"
    )

@pytest.fixture
def s3_file_config():
    """Dummy generic file configuration."""

    return FileConfig(
        storage=FileStorage.CSV,
        path="s3://my-bucket/data",
        aws_credentials=AwsCredentials(
            access_key="",
            secret_key="",
            session_token="",
            credentials_provider=""
        )
    )

@pytest.fixture
def local_file_config(tmp_path):
    """Configuration for a local file."""

    return FileConfig(
        storage=FileStorage.CSV,
        path=str(tmp_path),
        header="true",
        savemode=SaveMode.OVERWRITE
    )


@pytest.fixture
def parquet_config(tmp_path):
    """Dummy PARQUET configuration."""

    return ParquetConfig(path=str(tmp_path), savemode=SaveMode.OVERWRITE)


@pytest.fixture
def json_config(tmp_path):
    """Dummy JSON configuration."""

    return JsonConfig(path=str(tmp_path), savemode=SaveMode.OVERWRITE)


@pytest.fixture(scope="session")
def dataframe(spark):
    """Dummy dataframe."""

    columns = ["language", "users_count"]
    data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

    return spark.sparkContext.parallelize(data).toDF(columns)




class DummyConnector(Connector):
    """Dummy connector for testing purposes."""

    def read(self):
        """Test repository."""
        return NotImplemented

    def write(self, data):
        """Test repository."""
        return NotImplemented
