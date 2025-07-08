# Data Access Layer (DAL)

The PySetl Data Access Layer (DAL) provides a high-level, type-safe, and
extensible interface for reading and writing data, abstracting over file formats
and storage backends. It is built around the concepts of repositories and
connectors.

---

## How the DAL Fits Into a PySetl Workflow

- **Config**: Defines how to access data (e.g., CsvConfig, ParquetConfig)
- **Connector**: Uses config to read/write from a specific storage backend
- **Repository**: Provides a type-safe interface for loading and saving data
- **Factory/Stage/Pipeline**: Use repositories to build ETL logic
- **PySetl**: Orchestrates the entire workflow

---

## Architecture & Philosophy

- **Type Safety:** All repositories are parameterized by a schema (using [typedspark](https://typedspark.readthedocs.io/)).
- **Extensibility:** Easily add new connectors or repositories for custom data sources.
- **Separation of Concerns:** Repositories handle business logic, connectors handle storage specifics.
- **Consistency:** Unified API for reading and writing data, regardless of backend.

---

## Core Components

``` mermaid
graph LR
    A[BaseConnector]
    B[Connector]
    C[FileConnector]
    D[ParquetConnector]
    E[CsvConnector]
    F[JsonConnector]

    A --> B
    B --> C
    C --> D
    C --> E
    C --> F
```

### Repositories

- **BaseRepository[T]:** Abstract base for all repositories. Defines the
interface for loading and saving typed datasets.
- **SparkRepository[T]:** Concrete implementation for Spark, providing
read-after-write consistency, partition management, and integration with
connectors.

### Connectors

- **BaseConnector:** Abstract base for all connectors, defining read/write operations.
- **Connector:** Integrates Spark and logging, and is subclassed for specific file types.
- **FileConnector:** Handles file-based data sources (CSV, JSON, Parquet, etc.), with support for partitioning, S3/local filesystems, and schema enforcement.
- **CsvConnector, JsonConnector, ParquetConnector:** Specializations for each file format, enforcing correct config and storage type.

### Builders

- **ConnectorBuilder:** Instantiates the correct connector based on config (CSV,
JSON, Parquet).
- **SparkRepositoryBuilder:** Builds a SparkRepository for a given schema and
config, using the appropriate connector.

---

## How Config, Connector, Repository, and Factory Interact

Configs are passed to connectors, which are used by repositories. Factories use repositories to load and save data as part of ETL workflows.

```python
from pysetl.config import CsvConfig
from pysetl.storage.connector.csv_connector import CsvConnector
from pysetl.storage.repository import SparkRepository
from typedspark import DataSet, Schema, Column
from pyspark.sql.types import StringType

# 1. Define a schema
class MySchema(Schema):
    name: Column[StringType]

# 2. Create a config
csv_config = CsvConfig(path="/data/input.csv")

# 3. Create a connector
connector = CsvConnector(csv_config)

# 4. Create a repository
repo = SparkRepository[MySchema](connector)

# 5. Use in a factory (simplified)
class MyFactory(Factory[DataSet[MySchema]]):
    def read(self):
        self.data = repo.load()
        return self
    # ...
```

**In a real PySetl pipeline, repositories are often managed by the context and injected automatically into factories.**

---

## Type Safety and Error Handling

PySetl enforces type safety at every layer. If you use the wrong schema or an invalid config, errors are caught early and clearly reported.

```python
from pysetl.config import CsvConfig
from pysetl.storage.repository import SparkRepository
from typedspark import Schema, Column
from pyspark.sql.types import StringType

class MySchema(Schema):
    name: Column[StringType]

try:
    # Missing required 'path' field in config
    config = CsvConfig()
    repo = SparkRepository[MySchema](config)
except Exception as e:
    print("DAL error:", e)
```

**Output:**

```
DAL error: 1 validation error for CsvConfigModel
path
  Field required [type=missing, ...]
```

- All config and schema errors are explicit and easy to debug
- IDEs provide autocomplete and type checking for schemas and configs
- You can always access the validated config via `.config` and schema via `DataSet[Schema]`

---

## Features

- **Type Safety:** All repositories are parameterized by a schema (using typedspark).
- **Partition Management:** List and load specific partitions if supported by the connector.
- **Caching:** Optionally cache loaded data for performance.
- **Drop Support:** Remove all data from storage if supported.
- **Filesystem Abstraction:** Seamless support for local and S3 filesystems.

---

## Usage Examples

### 1. Define a Schema
```python
from typedspark import Schema, Column
from pyspark.sql.types import StringType, IntegerType

class MySchema(Schema):
    name: Column[StringType]
    age: Column[IntegerType]
```

### 2. Create a Config
```python
from pysetl.config import CsvConfig

config = CsvConfig(path="/data/input.csv")
```

### 3. Build a Repository
```python
from pysetl.storage.repository import SparkRepositoryBuilder

repo = SparkRepositoryBuilder[MySchema](config).build().get()
```

### 4. Load Data
```python
dataset = repo.load()  # Returns DataSet[MySchema]
```

### 5. Save Data
```python
repo.save(dataset)
```

### 6. Partition Management
```python
# List available partitions (if supported)
partitions = repo.list_partitions()

# Load specific partitions
subset = repo.load_partitions(partitions[:2])
```

### 7. Drop Data
```python
repo.drop()
```

### 8. Caching
```python
repo.cache = True  # Enable caching
```

---

## Advanced: S3 and Filesystem Abstraction

PySetl connectors support both local and S3 filesystems. Simply provide the appropriate path and credentials in your config.

```python
from pysetl.config import CsvConfig, AwsCredentials

config = CsvConfig(
    path="s3://my-bucket/data/input.csv",
    aws_credentials=AwsCredentials(
        access_key="...",
        secret_key="...",
        session_token="...",
        credentials_provider="..."
    )
)
```

---

## Extending the DAL

To add a new connector or repository:

- Subclass `BaseConnector` and implement `read`/`write` methods.
- Subclass `BaseRepository` and implement `load`/`save` methods.
- Register your connector with a builder if needed.

---

## Best Practices

- Use typed schemas for all data operations.
- Prefer repository and builder patterns for consistency and testability.
- Leverage partitioning and caching for large datasets.
- Use S3 support for scalable, cloud-native workflows.

---

## Next Steps

- [Configuration Guide](configuration.md): Learn how to create and validate
configs for your data sources
- [Workflow Guide](workflow.md): See how repositories are used in ETL pipelines
- [PySetl Context](pysetl_context.md): Understand advanced dependency injection
and repository management

For more details, see the [API Reference](../api/storage.md).
