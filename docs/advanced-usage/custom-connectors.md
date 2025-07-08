# How to Extend Connectors

In PySetl, **Connectors** are the bridge between your ETL pipelines and various
data storage systems (files, databases, APIs, etc.). They abstract away the
complexities of reading from and writing to these systems, providing a clean,
consistent interface. Extending PySetl's connector framework allows you to
integrate custom data sources or specialized storage behaviors, maintaining
PySetl's principles of type safety, modularity, and testability.

## Understanding PySetl's Connector Abstractions

Before creating a custom connector, it's essential to understand the
foundational classes and mixins provided by PySetl:

### 1. `BaseConnector`

This is the most fundamental abstract base class for all connectors. It defines
the minimal interface that any connector must implement:

* `read(self) -> DataFrame`: An abstract method to read data and return a Spark
`DataFrame`.
* `write(self, data: DataFrame) -> None`: An abstract method to write a Spark
`DataFrame` to the data source.

Any class inheriting directly from `BaseConnector` *must* provide concrete
implementations for these two methods.

### 2. `Connector`

The `Connector` class is the primary abstract base for Spark-based data
connectors within PySetl. It builds upon `BaseConnector` by integrating
essential PySetl capabilities through mixins:

```python
# src/pysetl/storage/connector/connector.py
from abc import ABCMeta
from pysetl.utils.mixins import HasSparkSession, HasLogger
from pysetl.config.config import BaseConfigModel
from .base_connector import BaseConnector


class Connector(BaseConnector, HasSparkSession, HasLogger, metaclass=ABCMeta):
    """
    Abstract base class for Spark-based data connectors with logging.

    Inherits from BaseConnector, HasSparkSession, and HasLogger.
    All connectors should implement read/write operations with a SparkSession object.
    This class cannot be instantiated directly.

    Attributes:
        config (BaseConfigModel): The configuration model for the connector.
    """
    config: BaseConfigModel # All concrete connectors must define a config attribute
```

**Key Features of `Connector`:**

  * **Inheritance:** It inherits from `BaseConnector` (enforcing `read` and
  `write` methods), `HasSparkSession`, and `HasLogger`.
  * **`config` Attribute:** It declares an abstract `config` attribute of type
  `BaseConfigModel`. This ensures that every concrete connector will be
  initialized with a validated configuration object, adhering to PySetl's
  "Type Safety First" philosophy.
  * **Abstract:** As an `ABCMeta` class, `Connector` cannot be instantiated
  directly; it serves as a template for concrete connector implementations.

### 3. Essential Mixins for Connectors

PySetl uses mixins extensively to provide reusable functionalities to various
components, including connectors. These mixins are abstract and provide
properties or methods that concrete classes must implement or gain access to.

  * **`HasSparkSession`**
    This mixin provides a convenient `spark` property, allowing any class that
    inherits it to easily access the active SparkSession instance.

    ```python
    # src/pysetl/utils/mixins/has_spark_session.py
    from pyspark.sql import SparkSession

    class HasSparkSession:
        """Abstract mixin to add SparkSession functionality to a class."""
        @property
        def spark(self) -> SparkSession:
            """Get the SparkSession instance."""
            return SparkSession.Builder().getOrCreate()
    ```

  * **`HasLogger`**
    This mixin provides methods for logging information, debug messages,
    warnings, and errors, integrating observability directly into your
    components.

    ```python
    # src/pysetl/utils/mixins/has_logger.py
    import logging

    class HasLogger:
        """Abstract mixin to add logging functionality to a class."""
        def log_info(self, msg: str) -> None:
            """Log info level message."""
            logging.info(msg)
        # ... (other logging methods like log_debug, log_warning, log_error)
        def log(self, message: str) -> None:
            """Abstract method to log a message (to be implemented by concrete class)."""
            # This abstract method needs to be implemented by the concrete class
            # to define how the generic 'log' call should behave.
            raise NotImplementedError
    ```

    *Note: The `log` method in `HasLogger` is abstract and needs to be
    implemented by the concrete connector class if you wish to use it directly.
    The `log_info`, `log_debug`, etc., are concrete implementations.*

  * **Other Common Mixins:**

      * `HasReader`:
        Provides an abstract `_reader` property for configuring Spark's `DataFrameReader`.
      * `HasWriter`:
        Provides an abstract `_writer` method for configuring Spark's `DataFrameWriter`.
      * `CanDrop`:
        Provides an abstract `drop` method for deleting data.
      * `CanPartition`: Provides methods related to managing partitioned data.

## How to Create a Custom Connector

To create your own custom connector in PySetl, follow these steps:

### Step 1: Define Your Custom Configuration

As discussed in the "Custom Configurations" section, every custom
connector needs a dedicated configuration. This involves creating:

  * A Pydantic model inheriting from `pysetl.config.BaseConfigModel` to define
  the schema and validate parameters.

  * A PySetl `Config` subclass (e.g., `MyCustomConfig`) inheriting from
  `pysetl.config.Config[MyCustomConfigModel]`, which will hold the parameters
  and expose the validated Pydantic model.

This ensures that your connector always receives a valid and type-checked set of options.

### Step 2: Inherit from `Connector` and Relevant Mixins

Your custom connector class should inherit from
`pysetl.storage.connector.Connector` and any other mixins that provide the
capabilities your connector needs (e.g., `HasReader`, `HasWriter`, `CanDrop`,
`CanPartition`).

```python
# src/pysetl/storage/connector/my_custom_connector.py (conceptual)
from pysetl.storage.connector import Connector
from pysetl.utils.mixins import HasReader, HasWriter # Example mixins
from pyspark.sql import DataFrame
from typing_extensions import Self

# Assuming you've defined MyCustomConfig and MyCustomConfigModel
from my_project.config.my_custom_config import MyCustomConfig, MyCustomConfigModel


class MyCustomConnector(Connector, HasReader, HasWriter):
    """
    A custom connector for interacting with a specific data source.
    """
    # The config attribute must be defined as per the Connector abstract class
    config: MyCustomConfigModel

    # ... rest of the implementation
```

### Step 3: Implement the `__init__` Method

Your connector's `__init__` method should accept an instance of your custom
`Config` class. It's responsible for validating the configuration (by accessing
its `.config` property) and storing the relevant parts of the configuration for
later use.

```python
# src/pysetl/storage/connector/my_custom_connector.py (continued)
# ... (imports from above)

class MyCustomConnector(Connector, HasReader, HasWriter):
    config: MyCustomConfigModel

    def __init__(self: Self, options: MyCustomConfig) -> None:
        """
        Initialize the MyCustomConnector.

        Args:
            options (MyCustomConfig): The custom configuration for this connector.
        """
        self.config = options.config # This triggers Pydantic validation
        self.reader_options = options.reader_config # Store Spark reader options
        self.writer_options = options.writer_config # Store Spark writer options
        # You might also initialize other client libraries here (e.g., database client)
```

### Step 4: Implement Abstract Methods (`read`, `write`)

Provide concrete implementations for the `read` and `write` methods inherited
from `BaseConnector`. These methods will use the `self.spark` session (provided
by `HasSparkSession`) and your connector's configuration (`self.config`,
`self.reader_options`, `self.writer_options`) to interact with the data source.

```python
# src/pysetl/storage/connector/my_custom_connector.py (continued)
# ... (imports and __init__)

class MyCustomConnector(Connector, HasReader, HasWriter):
    # ... (__init__ and config attribute)

    def read(self: Self) -> DataFrame:
        """
        Reads data from the custom data source into a Spark DataFrame.
        """
        self.log_info(f"Reading data from {self.config.source_uri}") # Using HasLogger
        # Example: Use self.spark and self.reader_options
        df = self.spark.read \
            .format(self.config.format_type) \
            .options(**self.reader_options) \
            .load(self.config.source_uri)
        return df

    def write(self: Self, data: DataFrame) -> None:
        """
        Writes a Spark DataFrame to the custom data source.
        """
        self.log_info(f"Writing data to {self.config.destination_uri} with mode {self.config.savemode.value}")
        # Example: Use self.spark and self.writer_options
        data.write \
            .format(self.config.format_type) \
            .mode(self.config.savemode.value) \
            .options(**self.writer_options) \
            .save(self.config.destination_uri)
```

### Step 5: Implement Mixin-Specific Logic (Optional, but common)

If you've included mixins like `HasReader`, `HasWriter`, `CanDrop`, or
`CanPartition`, you'll need to implement the abstract properties or methods they
define.

For `HasReader` and `HasWriter`, this typically involves defining `_reader` and
`_writer` properties/methods that return configured Spark `DataFrameReader` and
`DataFrameWriter` instances, respectively.

```python
# src/pysetl/storage/connector/my_custom_connector.py (continued)
# ... (imports, __init__, read, write)

class MyCustomConnector(Connector, HasReader, HasWriter):
    # ...

    @property
    def _reader(self: Self) -> DataFrameReader:
        """
        Returns a Spark DataFrameReader configured with connector-specific options.
        """
        return self.spark.read.options(**self.reader_options)

    def _writer(self: Self, data: DataFrame) -> DataFrameWriter:
        """
        Returns a Spark DataFrameWriter configured with connector-specific options.
        """
        # You might add pre-write checks or transformations here
        return data.write.mode(self.config.savemode.value).options(**self.writer_options)
```

## Concrete Example: `FileConnector` Deep Dive

The `FileConnector` is a robust implementation of this extension pattern,
handling various file-based storage types (CSV, Parquet, JSON) and integrating
with PyArrow for filesystem interactions.

```python
# src/pysetl/storage/connector/file_connector.py
from urllib.parse import urlparse, ParseResult
from pathlib import Path
from typing_extensions import Self
from pyarrow.fs import (
    S3FileSystem,
    FileSystem,
    LocalFileSystem,
    FileSelector,
)
from pyspark.sql import DataFrame, DataFrameReader, DataFrameWriter
from pyspark.sql.types import StructType
from pysetl.config.file_config import FileConfig, FileConfigModel # Custom config
from pysetl.utils.mixins import HasReader, HasWriter, CanDrop, CanPartition # Relevant mixins
from pysetl.utils.exceptions import InvalidConfigException # Custom exception
from .connector import Connector # Base PySetl Connector


class FileConnector(Connector, HasReader, HasWriter, CanDrop, CanPartition):
    """
    Connector for reading and writing data files using Spark and PyArrow.

    Handles file input/output operations on local and remote file systems,
    including partitioning and schema enforcement.
    """

    def __init__(self: Self, options: FileConfig) -> None:
        """
        Initialize the FileConnector.

        Args:
            options (FileConfig): The file configuration for the connector.
        """
        # Validate and store configuration
        self.config: FileConfigModel = options.config
        self.reader_config = options.reader_config
        self.writer_config = options.writer_config

    @property
    def _reader(self: Self) -> DataFrameReader:
        """
        Returns a Spark DataFrameReader configured with reader options.
        """
        return self.spark.read.options(**self.reader_config)

    def __check_partitions(self: Self, data: DataFrame) -> bool:
        """
        Check if all partition columns are present in the DataFrame.
        """
        return (
            all(x in data.columns for x in self.config.partition_by)
            if self.config.partition_by
            else True
        )

    def _writer(self: Self, data: DataFrame) -> DataFrameWriter:
        """
        Returns a Spark DataFrameWriter configured with writer options and partitioning.

        Raises:
            InvalidConfigException: If partition columns are missing in the data.
        """
        if not self.__check_partitions(data):
            raise InvalidConfigException(
                "Partition columns in configuration not in data"
            )

        if isinstance(self.config.data_schema, StructType):
            ordered_data = data.select([x.name for x in self.config.data_schema])
        else:
            ordered_data = data

        return (
            ordered_data.write.mode(self.config.savemode.value)
            .options(**self.writer_config)
            .partitionBy(*self.config.partition_by)
            .format(self.config.storage.value)
        )

    @property
    def uri(self: Self) -> ParseResult:
        """
        Parse the string path into a URI (e.g., for 's3://bucket/path' or '/local/path').
        """
        return urlparse(self.config.path)

    @property
    def filesystem(self) -> FileSystem:
        """
        Get the current PyArrow FileSystem (Local or S3) based on URI scheme and configuration.

        Raises:
            InvalidConfigException: If S3 credentials are missing for S3 URIs.
        """
        if self.uri.scheme == "s3":
            if not self.config.aws_credentials:
                raise InvalidConfigException("No S3 credentials provided")

            return S3FileSystem(
                access_key=self.config.aws_credentials.access_key,
                secret_key=self.config.aws_credentials.secret_key,
                endpoint_override=f"{self.uri.hostname}:{self.uri.port}" if self.uri.hostname else None,
                allow_bucket_creation=True,
                allow_bucket_deletion=True,
            )

        # For local files or other schemes supported by PyArrow
        fs, _ = FileSystem.from_uri(uri=self.uri.geturl())
        return fs

    @property
    def absolute_path(self) -> str:
        """
        Returns the absolute path for the file or directory, handling local vs. remote.
        """
        return (
            str(Path(self.uri.path).absolute())
            if isinstance(self.filesystem, LocalFileSystem)
            else self.uri.path
        )

    @property
    def base_path(self) -> str:
        """
        Returns the base path (parent directory) for the file or directory.
        Useful for reading partitioned data.
        """
        return str(Path(self.absolute_path).parent)

    @property
    def has_wildcard(self: Self) -> bool:
        """
        Verifies if the path has a wildcard character (`*`).
        """
        return Path(self.absolute_path).name.endswith("*")

    def list_partitions(self: Self) -> list[str]:
        """
        Return all found partitions in the base path using the underlying filesystem.
        """
        selector = FileSelector(self.absolute_path)
        return [
            x.path
            for x in self.filesystem.get_file_info(selector)
            if x.type.name == "Directory"
        ]

    def write(self: Self, data: DataFrame) -> None:
        """
        Write data into the file system.

        Raises:
            InvalidConfigException: If the path contains a wildcard (cannot write to wildcard paths).
        """
        if self.has_wildcard:
            raise InvalidConfigException("Can't write to wildcard path")

        self._writer(data).save(self.absolute_path)

    def drop(self: Self) -> None:
        """
        Remove data files from the table directory.
        Implements the `CanDrop` mixin.
        """
        self.filesystem.delete_dir(self.absolute_path)

    def read(self: Self) -> DataFrame:
        """
        Read data from the file system into a Spark DataFrame.
        Implements the `BaseConnector`'s abstract `read` method.
        """
        return self._reader.format(self.config.storage.value).load(self.absolute_path)

    def read_partitions(self, partitions: list[str]) -> DataFrame:
        """
        Read specific partitions from the data source.
        """
        return (
            self._reader.option("basePath", self.base_path)
            .format(self.config.storage.value)
            .load(partitions)
        )
```

**Key Takeaways from `FileConnector`:**

  * **Configuration Integration:** The `__init__` method takes `FileConfig`,
    immediately validates it by accessing `options.config`, and stores the
    validated model (`self.config`) along with `reader_config` and
    `writer_config`.
  * **Mixins in Action:**
      * `HasSparkSession` provides `self.spark`.
      * `HasLogger` provides `self.log_info`, etc. (though not explicitly called
        in the provided snippet beyond the abstract `log` method, which would
        need implementation).
      * `HasReader` is implemented by `_reader` property.
      * `HasWriter` is implemented by `_writer` method.
      * `CanDrop` is implemented by `drop` method.
        `CanPartition` is implemented by `list_partitions` and `read_partitions`
        (and used by `__check_partitions`).
  * **Spark Integration:** Uses `self.spark.read` and `DataFrame.write` with
    options derived from the configuration.
  * **PyArrow `FileSystem`:** The `filesystem` property dynamically provides a
    PyArrow `FileSystem` (local or S3), abstracting away the underlying storage
    details. This is robust for handling different storage locations.
  * **URI Parsing:** The `uri` property uses `urllib.parse.urlparse` for
    flexible path handling (e.g., `s3://` vs. local paths).
  * **Error Handling:** Raises `InvalidConfigException` for critical
    configuration issues (e.g., missing S3 credentials, attempting to write to a
    wildcard path).
  * **Partitioning Logic:** Includes logic for checking and handling partitioned
    data, demonstrating advanced file interaction.

## Benefits of Custom Connectors

  * **Abstraction:** Hide the complexities of interacting with diverse data
    sources behind a consistent `read`/`write` interface.
  * **Reusability:** Once defined, your custom connector can be reused across
    multiple factories and pipelines.
  * **Testability:** Connectors can be tested in isolation, mocking the
    underlying data source interactions.
  * **Type Safety:** Through the integrated configuration, your connector's
    inputs are always validated.
  * **Modularity:** Keep your data interaction logic separate from your
    transformation logic.

By extending PySetl's connector framework, you empower your data engineering
projects to seamlessly integrate with any data ecosystem while maintaining high
standards of code quality and predictability
