# Data Access Layer (DAL)

The PySetl Data Access Layer (DAL) provides a high-level, type-safe, and extensible interface for reading and writing data, abstracting over file formats and storage backends. It is built around the concepts of repositories and connectors.

---

## Architecture & Philosophy

- **Type Safety:** All repositories are parameterized by a schema (using [typedspark](https://typedspark.readthedocs.io/)).
- **Extensibility:** Easily add new connectors or repositories for custom data sources.
- **Separation of Concerns:** Repositories handle business logic, connectors handle storage specifics.
- **Consistency:** Unified API for reading and writing data, regardless of backend.

---

## Core Components

### Repositories
- **BaseRepository[T]:** Abstract base for all repositories. Defines the interface for loading and saving typed datasets.
- **SparkRepository[T]:** Concrete implementation for Spark, providing read-after-write consistency, partition management, and integration with connectors.

### Connectors
- **BaseConnector:** Abstract base for all connectors, defining read/write operations.
- **Connector:** Integrates Spark and logging, and is subclassed for specific file types.
- **FileConnector:** Handles file-based data sources (CSV, JSON, Parquet, etc.), with support for partitioning, S3/local filesystems, and schema enforcement.
- **CsvConnector, JsonConnector, ParquetConnector:** Specializations for each file format, enforcing correct config and storage type.

### Builders
- **ConnectorBuilder:** Instantiates the correct connector based on config (CSV, JSON, Parquet).
- **SparkRepositoryBuilder:** Builds a SparkRepository for a given schema and config, using the appropriate connector.

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
from typedspark import Schema, column, StringType, IntegerType

class MySchema(Schema):
    name = column(StringType())
    age = column(IntegerType())
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

For more details, see the [API Reference](../api/storage.md).
