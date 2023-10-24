"""
Unit tests for AwsCredentials
"""
import textwrap
from copy import deepcopy

import pytest
from pyspark.sql.types import StructType
from pydantic import ValidationError
from pysetl.config import (
    CsvConfig, JsonConfig, FileConfig, ParquetConfig,
    AwsCredentials
)
from pysetl.enums import SaveMode, FileStorage


csv_config = CsvConfig(
    path="ruta/al/archivo.csv",
    partition_by=["column1", "column2"],
    savemode=SaveMode.ERRORIFEXISTS,
    data_schema=StructType(),
    inferSchema="false",
    header="true",
    delimiter=","
)

config_dict = {
    "storage": FileStorage.CSV,
    "path": "ruta/al/archivo",
    "partition_by": ["column1", "column2"],
    "savemode": SaveMode.ERRORIFEXISTS,
    "data_schema": StructType()
}


class ConfigObject:
    """Test simple config object."""
    path = "ruta/al/archivo.csv"


def test_csv_config():
    assert all([
        csv_config.config.path == "ruta/al/archivo.csv",
        csv_config.config.partition_by == ["column1", "column2"],
        csv_config.config.savemode == SaveMode.ERRORIFEXISTS,
        csv_config.config.data_schema == StructType(),
        csv_config.config.inferSchema == "false",
        csv_config.config.header == "true",
        csv_config.config.delimiter == ","
    ])


def test_csv_config_dict():
    assert csv_config.config_dict == {
        'storage': FileStorage.CSV,
        'path': 'ruta/al/archivo.csv',
        'partition_by': ['column1', 'column2'],
        'savemode': SaveMode.ERRORIFEXISTS,
        'aws_credentials': None,
        'data_schema': StructType(),
        'inferSchema': 'false',
        'header': 'true',
        'delimiter': ','
    }


def test_csv_config_set():
    csv_config_mutant = deepcopy(csv_config)

    csv_config_mutant.set("inferSchema", "true")

    assert csv_config_mutant.get("inferSchema") == "true"


def test_csv_config_get():
    assert csv_config.get("inferSchema") == "false"


def test_csv_config_has():
    assert csv_config.has("path")


def test_csv_config_contains():
    assert "path" in csv_config


def test_csv_config_concat():
    csv_config_mutant = csv_config | CsvConfig(inferSchema="true")

    assert csv_config_mutant.config.inferSchema == "true"


def test_csv_config_from_dict():
    csv_config_from_dict = CsvConfig.builder().from_dict(config_dict)

    assert csv_config_from_dict.config.storage == FileStorage.CSV


def test_csv_config_from_object():
    csv_config_from_object = (
        CsvConfig.builder()
        .from_object(ConfigObject)
    )

    assert csv_config_from_object.config.path == "ruta/al/archivo.csv"


def test_csv_config_fail():
    with pytest.raises(ValidationError) as error:
        _ = CsvConfig.builder().from_dict({
            "path": "ruta_correcta",
            "inferSchema": True
        }).config

    assert error.value.errors()[0]["msg"] == "Input should be a valid string"


def test_csv_config_concat_fail():
    with pytest.raises(TypeError) as error:
        _ = csv_config | {"inferSchema": "true"}

    out = str(error.value)
    assert out == "unsupported operand type(s) for |: 'CsvConfig' and 'dict'"


def test_csv_config_str():
    assert str(csv_config) == textwrap.dedent("""    PySetl CsvConfig:
      path: ruta/al/archivo.csv
      partition_by: ['column1', 'column2']
      savemode: SaveMode.ERRORIFEXISTS
      data_schema: StructType(List())
      inferSchema: false
      header: true
      delimiter: ,""")


def test_csv_config_repr():
    assert repr(csv_config) == str(csv_config)


def test_json_config():
    json_config = JsonConfig.builder().from_dict(config_dict)

    assert json_config.config.storage == FileStorage.JSON


def test_parquet_config():
    parquet_config = ParquetConfig.builder().from_dict(config_dict)

    assert parquet_config.config.storage == FileStorage.PARQUET


def test_file_config():
    file_config = FileConfig.builder().from_dict(config_dict)

    assert file_config.config.storage == FileStorage.CSV


def test_file_config_reader_config():
    file_config = FileConfig.builder().from_dict(
        config_dict | {"extra_reader_option": "true"}
    )

    assert file_config.reader_config == {"extra_reader_option": "true"}


def test_file_config_writer_config():
    file_config = FileConfig.builder().from_dict(
        config_dict | {"extra_writer_option": "true"}
    )

    assert file_config.writer_config == {"extra_writer_option": "true"}


def test_aws_credentials():
    aws_credentials = AwsCredentials(
        access_key="this",
        secret_key="should",
        session_token="work",
        credentials_provider="smoothly"
    )

    assert all([
        aws_credentials.access_key == "this",
        aws_credentials.secret_key == "should",
        aws_credentials.session_token == "work",
        aws_credentials.credentials_provider == "smoothly"
    ])


def test_aws_credentials_fail():
    with pytest.raises(ValidationError) as error:
        AwsCredentials(
            access_key=1,  # type: ignore
            secret_key=2,  # type: ignore
            session_token=3,  # type: ignore
            credentials_provider=4  # type: ignore
        )

    assert error.value.error_count() == 4
