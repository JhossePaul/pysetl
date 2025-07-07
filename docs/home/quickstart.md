# Quickstart

Welcome to **PySetl** — a modern, type-driven PySpark ETL framework.

This guide will help you get up and running with PySetl quickly and efficiently.

---

## Choose Your Path

### 🚀 QuickStart (You're here!)

**For experienced developers who want to dive in immediately**

- Minimal working example
- Key concepts overview
- Get running in minutes

### 📦 [Installation](installation.md)

**For setting up your environment**

- Installation methods (pip, conda, uv)
- Development setup
- Environment configuration
- Troubleshooting guide

### 📚 [Examples](examples.md)

**For seeing real-world usage patterns**

- Complete working examples
- Common patterns and best practices
- How to extend and create new examples

---

## Recommended Order

1. **Start with [Installation](installation.md)** if you haven't set up PySetl yet
2. **Follow this QuickStart** to run your first pipeline
3. **Explore [Examples](examples.md)** to see advanced patterns

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

## What Just Happened?

1. **Schema Definition**: We defined a `Citizen` schema using TypedSpark
2. **Factory Creation**: Created a `CitizensFactory` that handles read, process, and write operations
3. **Pipeline Assembly**: Built a pipeline with stages and factories
4. **Execution**: Ran the pipeline and got the results

---

## Key Concepts

- **Factories**: Encapsulate ETL logic (read → process → write)
- **Stages**: Group related factories for sequential execution
- **Pipelines**: Orchestrate the entire ETL workflow
- **Schemas**: Provide type safety for your data

---

## What You'll Learn

By the end of this section, you'll be able to:

- ✅ Install and configure PySetl
- ✅ Create your first ETL pipeline
- ✅ Understand core concepts (Factories, Stages, Pipelines)
- ✅ Run working examples
- ✅ Apply best practices

---

## Next Steps

- 📖 [Installation Guide](installation.md) - Set up your environment
- 🚀 [Examples](examples.md) - See real-world usage patterns
- 📚 [User Guide](../user-guide/configuration.md) - Deep dive into concepts
- 🔧 [API Reference](../api/pysetl.md) - Complete API documentation
- 🤝 [Contributing](../development/index.md) - Help improve PySetl

---

Happy ETL-ing! 🚀
