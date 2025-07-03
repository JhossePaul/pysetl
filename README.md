# PySetl - A PySpark ETL Framework

[![PyPI](https://img.shields.io/pypi/v/pysetl)](https://pypi.org/project/pysetl)
[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![PySpark](https://img.shields.io/badge/PySpark-3.4%2B-orange.svg?logo=apache-spark&logoColor=white)](https://spark.apache.org/docs/latest/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)
[![Build Status](https://github.com/JhossePaul/pysetl/actions/workflows/build.yml/badge.svg)](https://github.com/JhossePaul/pysetl/actions/workflows/build.yml)
[![Code Coverage](https://codecov.io/gh/JhossePaul/pysetl/branch/main/graph/badge.svg)](https://codecov.io/gh/JhossePaul/pysetl)
[![Documentation Status](https://readthedocs.org/projects/pysetl/badge/?version=latest)](https://pysetl.readthedocs.io/en/latest/?badge=latest)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![Type checked with mypy](https://img.shields.io/badge/mypy-checked-blue.svg)](http://mypy-lang.org/)
[![Pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)

## Overview

PySetl is a framework to improve the readability and structure of PySpark ETL
projects. It is designed to take advantage of Python's typing syntax to reduce
runtime errors through linting tools and verifying types at runtime, effectively
enhancing stability for large ETL pipelines.

To accomplish this, we provide some tools:

- **`pysetl.config`**: Type-safe configuration.
- **`pysetl.storage`**: Agnostic and extensible data sources connections.
- **`pysetl.workflow`**: Pipeline management and dependency injection.

PySetl is designed with Python typing syntax at its core. We strongly suggest
using [typedspark](https://typedspark.readthedocs.io/en/latest/) and
[pydantic](https://docs.pydantic.dev/latest/) for development.

## Why use PySetl?

- Model complex data pipelines.
- Reduce risks at production with type-safe development.
- Improve large project structure and readability.

## Quick Start

```python
from pysetl.config import CsvConfig
from pysetl.workflow import Factory, Stage, Pipeline
from typedspark import DataSet, Schema, Column, create_partially_filled_dataset
from pyspark.sql.types import StringType, IntegerType

# Define your data schema
class Citizen(Schema):
    name: Column[StringType]
    age: Column[IntegerType]
    city: Column[StringType]

# Create a factory
class CitizensFactory(Factory[DataSet[Citizen]]):
    def read(self):
        self.citizens = create_partially_filled_dataset(
            spark, Citizen,
            [{Citizen.name: "Alice", Citizen.age: 30, Citizen.city: "NYC"}]
        )
        return self
    def process(self): return self
    def write(self): return self
    def get(self): return self.citizens

# Build and run pipeline
stage = Stage().add_factory_from_type(CitizensFactory)
pipeline = Pipeline().add_stage(stage).run()
```

## Installation

PySetl is available on PyPI:

```bash
pip install pysetl
```

### Optional Dependencies

PySetl provides several optional dependencies for different use cases:

- **PySpark**: For local development (most production environments come with
their own Spark distribution)

  ```bash
  pip install "pysetl[pyspark]"
  ```

- **Documentation**: For building documentation locally
  ```bash
  pip install "pysetl[docs]"
  ```

## Documentation

- 📖 [User Guide](https://pysetl.readthedocs.io/en/latest/user-guide/configuration.html)
- 🔧 [API Reference](https://pysetl.readthedocs.io/en/latest/api/pysetl.html)
- 🚀 [Getting Started](https://pysetl.readthedocs.io/en/latest/getting-started.html)
- 🤝 [Contributing](https://pysetl.readthedocs.io/en/latest/contributing.html)

## Development

```bash
git clone https://github.com/JhossePaul/pysetl.git
cd pysetl
hatch shell
pre-commit install
```

### Development Commands

- **Run all checks**: `hatch run test:all`
- **Run tests only**: `hatch run test:test`
- **Lint code**: `hatch run test:lint`
- **Format code**: `hatch run test:format`
- **Type checking**: `hatch run test:type`
- **Build documentation**: `hatch run docs:docs`
- **Security checks**: `hatch run security:all`

## Contributing

We welcome contributions! Please see our
[Contributing Guide](https://pysetl.readthedocs.io/en/latest/contributing.html)
for details.

## License

This project is licensed under the Apache License 2.0 - see the
[LICENSE](LICENSE) file for details.

## Acknowledgments

PySetl is a port from [SETL](https://setl-framework.github.io/setl/). We want to
fully recognize this package is heavily inspired by the work of the SETL team.
We just adapted things to work in Python.
