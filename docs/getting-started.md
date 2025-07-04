# Getting Started

Welcome to **PySetl** — a modern, type-driven PySpark ETL framework.

This guide will help you get up and running quickly with PySetl.

---

## Installation

PySetl requires Python 3.9–3.13 and a working Spark environment.

```bash
pip install pysetl
```

Or, for development:
```bash
pip install hatch
git clone https://github.com/JhossePaul/pysetl.git
cd pysetl
hatch env create  # Set up the default environment
hatch env show    # See available scripts
hatch shell
```

---

## Minimal Example

Here's a minimal example to get you started:

```python
from pysetl.config import CsvConfig
from pysetl.workflow import Factory, Stage, Pipeline
from typedspark import DataSet, Schema, Column, create_partially_filled_dataset
from pyspark.sql.types import StringType, IntegerType

class Citizen(Schema):
    name: Column[StringType]
    age: Column[IntegerType]
    city: Column[StringType]

class CitizensFactory(Factory[DataSet[Citizen]]):
    def read(self):
        # Replace with your own data loading logic
        self.citizens = create_partially_filled_dataset(
            spark,
            Citizen,
            [
                {Citizen.name: "Alice", Citizen.age: 30, Citizen.city: "NYC"},
                {Citizen.name: "Bob", Citizen.age: 25, Citizen.city: "LA"}
            ]
        )
        return self
    def process(self):
        return self
    def write(self):
        return self
    def get(self):
        return self.citizens

config = {"citizens": CsvConfig(path="/tmp/citizens.csv")}

stage = Stage().add_factory_from_type(CitizensFactory)

pipeline = Pipeline().add_stage(stage).run()

print(pipeline.get_last_output())
```

---

## Next Steps
- Explore the [User Guide](user-guide/configuration.md) for in-depth tutorials and concepts.
- Dive into the [API Reference](api/pysetl.md) for details on every class and function.
- Check out the [examples](https://github.com/JhossePaul/pysetl/tree/main/examples) for real-world usage.

---

## Running Checks

- Type checking: `hatch run type`
- Lint code: `hatch run lint`
- Format code: `hatch run format`
- Run tests (default environment only): `hatch test`
- Run all test matrix: `hatch test --all`
- Run all tests with coverage (all matrix): `hatch test --cover --all`
- Build docs: `hatch run docs:docs`
- Serve docs: `hatch run docs:serve`
- Security checks: `hatch run security:all`

---

Happy ETL-ing! 🚀
