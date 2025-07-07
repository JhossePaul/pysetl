# Configuration System (`pysetl.config`)

PySetl provides a modern, type-safe, and extensible configuration system for all connectors and workflows. This system is built on top of [Pydantic](https://docs.pydantic.dev/) for robust validation and leverages Python's type system for clarity and safety.

---

## How Configuration Fits Into a PySetl Workflow

- **User Code**: Defines configuration objects (e.g., CsvConfig, FileConfig)
- **Config**: Validates and holds all settings for data sources, sinks, and connectors
- **Connector/Repository**: Receives config, uses it to read/write data
- **Factory/Stage/Pipeline**: Use repositories and configs to build ETL logic
- **PySetl**: Orchestrates the entire workflow

---

## Philosophy & Design

- **Type Safety:** All configurations are validated using Pydantic models, ensuring correctness at runtime.
- **Extensibility:** Easily extend or create new config types for custom connectors.
- **Builder Pattern:** Construct configs from dicts, objects, or code for flexibility.
- **Separation of Concerns:** User-facing config is separate from internal API config.

---

## Core Components

``` mermaid
graph LR
    A[BaseConfig]
    B[Config]
    C[FileConfig]
    D[ParquetConfig]
    E[CsvConfig]
    F[JsonConfig]

    A --> B
    B --> C
    C --> D
    C --> E
    C --> F
```

### BaseConfigModel

All config models inherit from `BaseConfigModel`, which enforces a `storage`
type and provides a foundation for validation.

### Config[T]

The main configuration class, parameterized by a Pydantic model. Provides
methods to set/get/validate config values, and exposes validated config via the
`.config` property.

#### Key Methods & Properties
- `set(key, value)`: Set a config value
- `get(key)`: Get a config value
- `has(key)`: Check if a key exists
- `config_dict`: Get the validated config as a dict
- `builder().from_dict({...})`: Build a config from a dictionary
- `config`: Returns the validated Pydantic model
- `reader_config` / `writer_config`: Dicts for DataFrameReader/Writer

---

## How Configs Are Used with Connectors and Repositories

Configs are passed directly to connectors and repositories, which use them to
read and write data. This ensures that all data access is type-safe and
validated.

```python
from pysetl.config import CsvConfig
from pysetl.storage.connector.csv_connector import CsvConnector

# Create a validated config
csv_config = CsvConfig(path="/data/input.csv", header="true")

# Pass config to connector
connector = CsvConnector(csv_config)
df = connector.read()  # Uses config for all options
```

**In a pipeline, configs are often managed by the PySetl context and injected automatically into repositories and factories.**

---

## Type Safety and Error Handling

PySetl's configuration system uses Pydantic for validation, so errors are caught early and clearly reported.

```python
from pysetl.config import CsvConfig

try:
    # Missing required 'path' field
    config = CsvConfig(header="true")
except Exception as e:
    print("Config validation error:", e)
```

**Output:**
```
Config validation error: 1 validation error for CsvConfigModel
path
  Field required [type=missing, ...]
```

- All config errors are explicit and easy to debug
- IDEs provide autocomplete and type checking for config fields
- You can always access the validated config via `.config`

---

## Built-in Config Types

### FileConfig
For file-based storage (CSV, JSON, Parquet, etc.).

**Fields:**

- `storage`: FileStorage (CSV, JSON, PARQUET, ...)
- `path`: str
- `partition_by`: list[str] (optional)
- `savemode`: SaveMode (default: ERRORIFEXISTS)
- `data_schema`: Optional[StructType]
- `aws_credentials`: Optional[AwsCredentials]

### CsvConfig

Specializes FileConfig for CSV files.

**Additional Fields:**
- `inferSchema`: str (default: "true")
- `header`: str (default: "true")
- `delimiter`: str (default: ",")

### JsonConfig

Specializes FileConfig for JSON files.

### ParquetConfig

Specializes FileConfig for Parquet files.

### AwsCredentials

Pydantic model for AWS S3 credentials.

- `access_key`: str
- `secret_key`: str
- `session_token`: str
- `credentials_provider`: str

---

## Usage Examples

### 1. Creating a FileConfig

```python
from pysetl.config import FileConfig

config = FileConfig(path="/data/input.csv", storage="CSV")
print(config.config)
```

### 2. Using the Builder Pattern
```python
from pysetl.config import CsvConfig

csv_dict = {
    "path": "/data/input.csv",
    "delimiter": ";",
    "header": "true"
}
csv_config = CsvConfig.builder().from_dict(csv_dict)
print(csv_config.config)
```

### 3. AWS Credentials

```python
from pysetl.config import AwsCredentials

creds = AwsCredentials(
    access_key="...",
    secret_key="...",
    session_token="...",
    credentials_provider="..."
)
```

### 4. Accessing Reader/Writer Configs

```python
from pysetl.config import CsvConfig

config = CsvConfig(path="/data/input.csv")
reader_conf = config.reader_config
writer_conf = config.writer_config
```

---

## Extending the Config System

To create your own config type, subclass `BaseConfigModel` and `Config`, then
add your fields and logic. Example:

```python
from pysetl.config import BaseConfigModel, Config
from pydantic import ConfigDict

class MyConfigModel(BaseConfigModel):
    my_field: str
    model_config = ConfigDict(extra="allow", frozen=True)

class MyConfig(Config[MyConfigModel]):
    @property
    def config(self):
        return MyConfigModel(**self.params)
```

---

## Best Practices
- Always use the `.config` property to access validated configs.
- Use the builder for constructing configs from dicts or objects.
- Prefer explicit field names and types for clarity.
- Leverage Pydantic validation for custom logic.

---

## Next Steps

- [Data Access Layer](dal.md): See how configs are used with repositories and connectors
- [Workflow Guide](workflow.md): Learn how to build ETL workflows using your configs
- [PySetl Context](pysetl_context.md): Understand advanced dependency injection and config management

For more details, see the [API Reference](../api/config.md).
