# Custom Configurations

PySetl's configuration system is designed to be highly extensible, allowing you
to define custom configuration classes tailored to your specific data sources,
transformations, or sinks. This extensibility is achieved by leveraging
`pysetl.config.Config` and, crucially, by pairing it with a Pydantic
`BaseConfigModel` for robust, type-safe validation.

## The Extension Pattern: `Config` and `BaseConfigModel`

The core pattern for creating custom configurations in PySetl involves two main
components:

1.  **A Pydantic Model (inheriting from `pysetl.config.BaseConfigModel`):**

    * This model defines the strict schema and types for your configuration
    parameters.
    * It enforces type safety and allows Pydantic to validate parameters during
    instantiation, ensuring your configuration adheres to a predefined
    structure.
    * It can also use Pydantic's `ConfigDict` for advanced validation behaviors
    (e.g., `extra="allow"` for flexible parameters, `frozen=True` for
    immutability).

2.  **A PySetl Config Class (inheriting from `pysetl.config.Config`):**

    * This class acts as the runtime container and interface for your
    configuration.
    * It is generic, parameterized by your custom `BaseConfigModel` (e.g.,
    `Config[MyCustomConfigModel]`), linking the runtime configuration to its
    validation schema.
    * It must implement the `config` property, which returns an instance of your
    validated Pydantic model, effectively performing the validation.
    * It typically provides `reader_config` and `writer_config` properties to
    expose configuration parameters in a format suitable for underlying
    libraries like Apache Spark's `DataFrameReader` and `DataFrameWriter`.

Let's look at the `FileConfig` and `FileConfigModel` as a concrete example of
this pattern.

## Example: `FileConfig` for File Connectors

The `FileConfig` class is a prime example of how to extend PySetl's
configuration for a specific purpose – managing settings for file-based data
connectors (CSV, Parquet, JSON, etc.).

### 1. Defining the Configuration Schema: `FileConfigModel`

The `FileConfigModel` defines the expected parameters for any file-based
connector. It inherits from `BaseConfigModel`, making it a Pydantic model that
automatically provides validation capabilities.

```python
# src/pysetl/config/file_config.py (conceptual location)
from typing import Optional
from pyspark.sql.types import StructType
from pydantic import ConfigDict # Pydantic's configuration helper
from pysetl.enums import SaveMode, FileStorage # PySetl's enums for type safety
from .aws_credentials import AwsCredentials # Assuming this is another Pydantic model
from .config import BaseConfigModel # The base Pydantic model for PySetl configs


class FileConfigModel(BaseConfigModel):
    """
    Validator for file connector configurations.

    This Pydantic model ensures that all file-related configuration
    parameters adhere to the specified types and constraints.
    """

    model_config = ConfigDict(
        extra="allow",          # Allows extra fields not explicitly defined in the model
        frozen=True,            # Makes the instance immutable after creation
        strict=True,            # Ensures strict type checking
        arbitrary_types_allowed=True # Allows types like StructType from PySpark
    )

    storage: FileStorage        # Must be one of the FileStorage enums (e.g., CSV, PARQUET)
    path: str                   # The file or directory path
    partition_by: list[str] = [] # Columns for partitioning, defaults to empty list
    savemode: SaveMode = SaveMode.ERRORIFEXISTS # Save mode for writing, with default
    data_schema: Optional[StructType] = None # Optional Spark schema
    aws_credentials: Optional[AwsCredentials] = None # Optional AWS credentials for cloud storage
```

**Key Aspects of `FileConfigModel`:**

  * **Pydantic `BaseModel`:** Inheriting from `BaseConfigModel` (which is a
  `pydantic.BaseModel`) grants all Pydantic's validation features.
  * **Type Annotations:** Each attribute (e.g., `storage: FileStorage`, `path:
  str`) is type-annotated, enabling Pydantic to validate input types
  automatically.
  * **Default Values:** Parameters like `partition_by` and `savemode` have
  default values, making the configuration flexible.
  * **`model_config` (`ConfigDict`):**
      * `extra="allow"`: This is crucial for flexibility. It means that while
      `FileConfigModel` defines core parameters, you can pass additional,
      arbitrary key-value pairs to `FileConfig` (and thus to `self.params`)
      that are specific to certain file formats (e.g., `header="true"` for
      CSV, `inferSchema="true"`, `delimiter=","`). These extra parameters will
      be stored in `self.params` and *will still be passed through to the
      underlying Spark reader/writer options*.
      * `frozen=True`: Makes instances immutable, ensuring configuration
      stability after creation.
      * `strict=True`: Enforces stricter type checking.
      `arbitrary_types_allowed=True`: Necessary for types like `StructType`
      from PySpark.

### 2. Implementing the Configuration Interface: `FileConfig`

The `FileConfig` class ties the validated schema (`FileConfigModel`) to PySetl's
`Config` interface. It manages the raw parameters internally and exposes the
validated Pydantic model.

```python
# src/pysetl/config/file_config.py (conceptual location, continued)
from typing_extensions import Self
from .config import Config # The base PySetl Config class


class FileConfig(Config[FileConfigModel]):
    """
    Configuration for file connectors.

    This class serves as the runtime container for file-based configuration,
    providing methods to access validated settings and generate Spark-compatible
    reader/writer options.
    """

    @property
    def config(self: Self) -> FileConfigModel:
        """
        Returns the validated file connector configuration.

        This property is essential. It instantiates `FileConfigModel` using
        the parameters stored in `self.params`. If `self.params` contains
        any values that violate the schema defined in `FileConfigModel`
        (e.g., wrong type, missing required field), Pydantic will raise
        a validation error here, ensuring an early "fail-fast" mechanism.

        Returns:
            FileConfigModel: The validated configuration model instance.
        """
        return FileConfigModel(**self.params)

    @property
    def reader_config(self: Self) -> dict:
        """
        Returns the configuration dictionary for Spark's DataFrameReader.

        This method extracts parameters relevant for reading data. It includes
        all parameters that were *not* explicitly defined in `FileConfigModel`
        (due to `extra="allow"`), allowing you to pass format-specific Spark options.
        """
        # Exclude parameters explicitly handled by FileConfigModel,
        # but include any extra parameters allowed by model_config(extra="allow").
        return {
            k: v for k, v in self.params.items() if k not in FileConfigModel.model_fields.keys()
        }

    @property
    def writer_config(self) -> dict:
        """
        Returns the configuration dictionary for Spark's DataFrameWriter.

        Similar to reader_config, this provides parameters for writing data.
        """
        return {
            k: v for k, v in self.params.items() if k not in FileConfigModel.model_fields.keys()
        }

```

**Key Aspects of `FileConfig`:**

  * **Generic `Config[FileConfigModel]`:** The generic type parameter
  `FileConfigModel` informs PySetl (and type checkers) about the expected
  validation model for this configuration.
  * **`config` Property (Crucial for Validation):** This abstract property from
  `Config` *must* be implemented. Its implementation
  (`FileConfigModel(**self.params)`) is where the actual Pydantic validation
  occurs. Any invalid parameters passed to the `FileConfig` instance will
  cause a `ValidationError` when this property is accessed.
  * **`reader_config` and `writer_config` Properties:** These properties are
  designed to extract and provide configuration parameters in a dictionary
  format, often directly usable by Spark's `DataFrameReader.option()` or
  `DataFrameWriter.option()` methods. The current implementation cleverly
  filters out the core model fields, passing through any "extra" parameters
  provided (due to `extra="allow"` in `FileConfigModel`). This is excellent
  for handling format-specific options (e.g., `header`, `delimiter` for CSV,
  `mergeSchema` for Parquet).

## Putting it Together: Using Custom Configurations

Here's how you would use your custom `FileConfig` in practice, demonstrating how
validation works and how to supply format-specific options.

```python
from pysetl.config import Config, BaseConfigModel
from pysetl.enums import FileStorage, SaveMode
from pyspark.sql.types import StructType, StringType, IntegerType, LongType, ArrayType
from pydantic import ValidationError # To catch validation errors

# (Assume FileConfigModel and FileConfig are defined as above)

# --- Valid Configuration Example (Parquet) ---
try:
    parquet_settings = FileConfig.builder().from_dict({
        "storage": FileStorage.PARQUET,
        "path": "/path/to/my_data/parquet",
        "partition_by": ["year", "month"],
        "savemode": SaveMode.OVERWRITE,
        "compression": "snappy" # 'compression' is an extra parameter allowed by FileConfigModel's ConfigDict
    })
    print("--- Valid Parquet Config ---")
    print(parquet_settings)
    print(f"Validated Storage: {parquet_settings.config.storage}")
    print(f"Spark Writer Options: {parquet_settings.writer_config}")
    # Expected: {'compression': 'snappy'}

except ValidationError as e:
    print(f"Validation Error: {e}")

# --- Valid Configuration Example (CSV with specific options) ---
try:
    csv_settings = FileConfig.builder().from_dict({
        "storage": FileStorage.CSV,
        "path": "/path/to/my_data/csv",
        "header": "true",          # CSV-specific option
        "delimiter": ",",          # CSV-specific option
        "inferSchema": "true",     # CSV-specific option
        "savemode": SaveMode.APPEND
    })
    print("\n--- Valid CSV Config ---")
    print(csv_settings)
    print(f"Validated Storage: {csv_settings.config.storage}")
    print(f"Spark Reader Options: {csv_settings.reader_config}")
    # Expected: {'header': 'true', 'delimiter': ',', 'inferSchema': 'true'}

except ValidationError as e:
    print(f"Validation Error: {e}")


# --- Invalid Configuration Example (Missing required 'path') ---
try:
    invalid_settings = FileConfig.builder().from_dict({
        "storage": FileStorage.JSON,
        # 'path' is missing, which is a required field in FileConfigModel
    })
    # Accessing .config will trigger validation
    print(invalid_settings.config)

except ValidationError as e:
    print("\n--- Invalid Config (Missing 'path') ---")
    print(f"Validation Error caught as expected:\n{e}")
    # Expected: ValidationError: 1 validation error for FileConfigModel
    # path
    #   Field required [type=missing, input_value={}, input_type=dict]


# --- Invalid Configuration Example (Wrong type for 'storage') ---
try:
    wrong_type_settings = FileConfig.builder().from_dict({
        "storage": "not_an_enum_member", # Should be FileStorage.XXX
        "path": "/some/path"
    })
    # Accessing .config will trigger validation
    print(wrong_type_settings.config)

except ValidationError as e:
    print("\n--- Invalid Config (Wrong type for 'storage') ---")
    print(f"Validation Error caught as expected:\n{e}")
    # Expected: ValidationError: 1 validation error for FileConfigModel
    # storage
    #   Input should be 'csv', 'parquet', 'json', or 'generic' [type=enum, input_value='not_an_enum_member', input_type=str]
```

By following this pattern, you gain:

  * **Strong Type Safety:** Pydantic ensures that your configuration values are
  of the correct type.
  * **Fail-Fast Validation:** Errors are caught at configuration loading time,
  not during runtime execution.
  * **Clear Schema:** Your `BaseConfigModel` acts as living documentation for
  your configuration parameters.
  * **Flexibility with `extra="allow"`:** Allows you to define core
  configuration fields while still supporting arbitrary, format-specific
  options (e.g., Spark DataFrame read/write options).
  * **Maintainability:** Changes to your configuration schema are centrally
  managed and automatically validated.

This robust system ensures that your ETL pipelines are built on a solid and
predictable configuration foundation.
