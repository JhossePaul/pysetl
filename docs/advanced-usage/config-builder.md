# Builders for Configuration

PySetl emphasizes type-safe and declarative configuration. To streamline the
creation and management of configuration objects, PySetl provides
`ConfigBuilder`. This builder allows you to construct `Config` instances from
various sources, ensuring that your ETL settings are well-defined and validated
early in the development cycle.

## The `ConfigBuilder` Class

The `ConfigBuilder` is a utility class designed to facilitate the instantiation
of `Config` objects. It acts as a factory for your configuration, allowing you
to populate configuration parameters from dictionaries or existing objects.

You typically access the `ConfigBuilder` through the `builder()` class method
available on any `Config` subclass.

### Accessing the Builder

To get an instance of `ConfigBuilder` for a specific `Config` class, you use the
`builder()` class method. PySetl leverages enums like `FileStorage` and
`DbStorage` (defined in `pysetl.enums`) to provide type-safe options for
configuration parameters, especially for `storage` types.

```python
from pysetl.config import Config, BaseConfigModel
from pysetl.enums import FileStorage

# Example: Define a concrete Config subclass for a file-based configuration
class FileConfigModel(BaseConfigModel):
    storage: FileStorage
    path: str
    recursive: bool = False # Example of another parameter

class FileConfig(Config[FileConfigModel]):
    @property
    def config(self) -> FileConfigModel:
        """Expose the validated config object."""
        return FileConfigModel(**self.params)

    @property
    def reader_config(self) -> dict:
        """Returns the reader configuration dictionary."""
        # Example: Specific reader options for file storage
        return {
            "path": self.params.get("path"),
            "recursiveFileLookup": self.params.get("recursive")
        }

    @property
    def writer_config(self) -> dict:
        """Returns the writer configuration dictionary."""
        # Example: Specific writer options for file storage
        return {"path": self.params.get("path")}

# Get the builder for FileConfig
config_builder = FileConfig.builder()
print(config_builder)
```

## Constructing Configurations

`ConfigBuilder` offers two primary methods for constructing `Config` objects:
`from_dict()` and `from_object()`.

### 1. `from_dict()`: Building from a Dictionary

The `from_dict()` method allows you to create a `Config` instance by passing a
dictionary containing your configuration parameters. This is particularly useful
when loading settings from external sources like YAML files, JSON files, or
environment variables.

**Method Signature:**

```python
from_dict(__config: dict) -> Config
```

**Parameters:**

  * `__config` (`dict`): A dictionary where keys are configuration parameter
  * names and values are their corresponding settings.

**Example Usage:**

```python
from pysetl.config import Config, BaseConfigModel
from pysetl.enums import FileStorage # Using PySetl's FileStorage enum

class FileConfigModel(BaseConfigModel):
    storage: FileStorage
    path: str
    recursive: bool = False

class FileConfig(Config[FileConfigModel]):
    @property
    def config(self) -> FileConfigModel:
        return FileConfigModel(**self.params)

    @property
    def reader_config(self) -> dict:
        return {
            "path": self.params.get("path"),
            "recursiveFileLookup": self.params.get("recursive")
        }

    @property
    def writer_config(self) -> dict:
        return {"path": self.params.get("path")}

# Configuration parameters as a dictionary
parquet_settings_dict = {
    "storage": FileStorage.PARQUET, # Using a supported FileStorage type
    "path": "/data/my_parquet_files",
    "recursive": True
}

# Build the config using from_dict
parquet_config_instance = FileConfig.builder().from_dict(parquet_settings_dict)

print(parquet_config_instance)
# Expected Output:
# PySetl FileConfig:
#   storage: FileStorage.PARQUET
#   path: /data/my_parquet_files
#   recursive: True

# Example with CSV storage
csv_settings_dict = {
    "storage": FileStorage.CSV,
    "path": "/data/my_csv_files",
    "header": True # Additional parameter for CSV
}

csv_config_instance = FileConfig.builder().from_dict(csv_settings_dict)
print(csv_config_instance)
# Expected Output:
# PySetl FileConfig:
#   storage: FileStorage.CSV
#   path: /data/my_csv_files
#   header: True
```

**Behind the Scenes:**

The `from_dict` method leverages Python's `reduce` function to iteratively apply
each key-value pair from the input dictionary to a new `Config` instance using
its `set()` method. This ensures that all parameters are correctly populated.

### 2. `from_object()`: Building from an Object's Attributes

The `from_object()` method allows you to construct a `Config` instance by
extracting attributes from another Python object. This is convenient when you
have configuration defined as class attributes in a settings module or a simple
data object. It automatically filters out "private" attributes (those starting
with `__`).

**Method Signature:**

```python
from_object(__config: object) -> Config
```

**Parameters:**

  * `__config` (`object`): An object whose public attributes (not starting with
  `__`) will be used as configuration parameters.

**Example Usage:**

```python
from pysetl.config import Config, BaseConfigModel
from pysetl.enums import FileStorage

class FileConfigModel(BaseConfigModel):
    storage: FileStorage
    path: str
    enabled: bool = True

class FileConfig(Config[FileConfigModel]):
    @property
    def config(self) -> FileConfigModel:
        return FileConfigModel(**self.params)

    @property
    def reader_config(self) -> dict:
        return {"path": self.params.get("path")}

    @property
    def writer_config(self) -> dict:
        return {"path": self.params.get("path")}

# Define a simple object with configuration attributes
class AppSettings:
    STORAGE = FileStorage.JSON # Using a supported FileStorage type
    DATA_PATH = "/app/json_data"
    DEBUG_MODE = True
    __private_attribute = "should_be_ignored" # This will be ignored

# Build the config using from_object
app_config_instance = FileConfig.builder().from_object(AppSettings)

print(app_config_instance)
# Expected Output:
# PySetl FileConfig:
#   STORAGE: FileStorage.JSON
#   DATA_PATH: /app/json_data
#   DEBUG_MODE: True
```

**Note on `from_object()`:** The keys in the resulting `Config` object will
match the attribute names of the source object (e.g., `STORAGE`, `DATA_PATH`).
Ensure these align with the expected parameters of your `BaseConfigModel` if
you plan to validate the configuration using the `.config` property.

## Benefits of Using Config Builders

  * **Flexibility:** Easily load configurations from different sources
  (dictionaries, objects).
  * **Readability:** Clearly separate configuration definition from its
  instantiation logic.
  * **Consistency:** Provides a standardized way to create `Config` objects
  across your project.
  * **Chaining:** The `set()` method on `Config` allows for fluent chaining,
  which is implicitly used by `from_dict()` for concise parameter assignment.
  * **Pydantic Validation (via `Config.config` property):** While the builder
  itself populates the `params` dictionary, the ultimate validation against
  your `BaseConfigModel` happens when you access the `.config` property of
  your `Config` instance, ensuring type safety and catching issues like
  invalid `storage` types early.

By leveraging `ConfigBuilder`, PySetl promotes a robust and maintainable
approach to managing ETL configurations, allowing you to focus on data logic
rather than configuration parsing complexities.
