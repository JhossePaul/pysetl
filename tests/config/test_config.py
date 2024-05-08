"""Unit tests config module."""
import textwrap
from copy import deepcopy

import pytest
from pyspark.sql.types import StructType
from pydantic import ValidationError
from pysetl.config import (
    CsvConfig, JsonConfig, FileConfig, ParquetConfig,
)
from pysetl.enums import SaveMode, FileStorage


def test_csv_config(csv_config: CsvConfig):
    """Test basic properties for Config object."""

    assert csv_config.config_dict == {
        'storage': FileStorage.CSV,
        'path': csv_config.params["path"],
        'partition_by': [],
        'savemode': SaveMode.ERRORIFEXISTS,
        'aws_credentials': None,
        'data_schema': StructType(),
        'inferSchema': 'false',
        'header': 'true',
        'delimiter': ','
    }
    assert csv_config.config.path == csv_config.params.get("path")
    assert csv_config.config.partition_by == []
    assert csv_config.config.savemode == SaveMode.ERRORIFEXISTS
    assert csv_config.config.data_schema == StructType()
    assert csv_config.config.inferSchema == "false"
    assert csv_config.config.header == "true"
    assert csv_config.config.delimiter == ","
    assert csv_config.get("inferSchema") == "false"
    assert csv_config.has("path")
    assert "path" in csv_config
    assert str(csv_config) == textwrap.dedent(f"""    PySetl CsvConfig:
      path: {csv_config.params.get("path")}
      partition_by: []
      savemode: SaveMode.ERRORIFEXISTS
      data_schema: StructType([])
      inferSchema: false
      header: true
      delimiter: ,""")
    assert repr(csv_config) == str(csv_config)

    csv_config_mutant = deepcopy(csv_config)
    csv_config_mutant.set("inferSchema", "true")

    assert csv_config_mutant.get("inferSchema") == "true"
    assert (csv_config | CsvConfig(inferSchema="true")).config.inferSchema == "true"


def test_csv_config_from_dict(config_dict):
    """Test Config constructor from a dictionary."""

    csv_config_from_dict = CsvConfig.builder().from_dict(config_dict)

    assert csv_config_from_dict.config.storage == FileStorage.CSV


def test_csv_config_from_object(config_object):
    """Test Config constructor from an object."""

    csv_config_from_object = (
        CsvConfig.builder()
        .from_object(config_object)
    )

    assert csv_config_from_object.config.path == config_object.path


def test_csv_config_exception():
    """Test Config type validation."""

    with pytest.raises(ValidationError) as error:
        _ = CsvConfig.builder().from_dict({
            "path": "ruta_correcta",
            "inferSchema": True
        }).config

    assert error.value.errors()[0]["msg"] == "Input should be a valid string"


def test_csv_config_concat_exception(csv_config):
    """Test config concat exception when concat with a non Config object."""

    with pytest.raises(TypeError) as error:
        _ = csv_config | {"inferSchema": "true"}

    out = str(error.value)
    assert out == "unsupported operand type(s) for |: 'CsvConfig' and 'dict'"


def test_json_config(config_dict):
    """Test JsonConfig builder."""

    json_config = JsonConfig.builder().from_dict(config_dict)

    assert json_config.config.storage == FileStorage.JSON


def test_parquet_config(config_dict):
    """Test ParquetConfig builder."""

    parquet_config = ParquetConfig.builder().from_dict(config_dict)

    assert parquet_config.config.storage == FileStorage.PARQUET


def test_file_config(config_dict):
    """Test FileConfig builder."""

    file_config = FileConfig.builder().from_dict(config_dict)

    assert file_config.config.storage == FileStorage.CSV


def test_file_config_reader_config(config_dict):
    """Test extra properties passed to reader."""

    file_config = FileConfig.builder().from_dict(
        config_dict | {"extra_reader_option": "true"}
    )

    assert file_config.reader_config == {"extra_reader_option": "true"}


def test_file_config_writer_config(config_dict):
    """Test extra properties passed to writer."""

    file_config = FileConfig.builder().from_dict(
        config_dict | {"extra_writer_option": "true"}
    )

    assert file_config.writer_config == {"extra_writer_option": "true"}


def test_aws_credentials(aws_credentials):
    """Check AwsCredentials type validation."""

    assert all([
        aws_credentials.access_key == "this",
        aws_credentials.secret_key == "should",
        aws_credentials.session_token == "work",
        aws_credentials.credentials_provider == "smoothly"
    ])
