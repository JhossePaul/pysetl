# PySetl Context: Putting it all together

The `pysetl.PySetl` class is the main entrypoint and context manager for
orchestrating Spark ETL workflows in PySetl. It encapsulates the Spark session,
configuration, external inputs, and pipeline management, making it easy to
build, configure, and execute complex data pipelines.

---

## How PySetl Context Ties Everything Together

- **Central Orchestration:** PySetl context manages the Spark session,
configuration registry, repositories, external inputs, and pipelines.
- **Dependency Injection:** Automatically injects configs and repositories into
factories and pipelines, reducing boilerplate and errors.
- **Lifecycle Management:** Handles setup, execution, and teardown (including
Spark shutdown) for the entire ETL workflow.
- **Reproducibility:** Ensures all resources and dependencies are registered and
available before pipeline execution.

---

## Dependency Injection and Context Management

PySetl context makes it easy to inject repositories and configs into factories
and pipelines. This enables modular, testable, and maintainable workflows.

```python
from pysetl import PySetl
from pysetl.config import CsvConfig
from pysetl.storage.repository import SparkRepository
from pysetl.workflow import Factory, Delivery
from typedspark import DataSet, Schema, Column
from pyspark.sql.types import StringType

class MySchema(Schema):
    name: Column[StringType]

class MyFactory(Factory[DataSet[MySchema]]):
    repo_delivery = Delivery[SparkRepository[MySchema]]()
    def read(self):
        repo = self.repo_delivery.get()  # Injected automatically by PySetl context
        self.data = repo.load()
        return self
    # ...

config = {"mydata": CsvConfig(path="/data/input.csv")}
pysetl = PySetl.builder().set_config(config).getOrCreate()
pysetl.set_spark_repository_from_config(MySchema, "mydata")

# Now, when you build a pipeline, the repository is injected into the factory
```

---

## Error Handling and Type Safety

PySetl context validates that all required configs and repositories are
registered before pipeline execution. If something is missing, you get a clear
error.

```python
from pysetl import PySetl
from pysetl.workflow import Factory, Delivery
from typedspark import DataSet, Schema, Column
from pyspark.sql.types import StringType

class MySchema(Schema):
    name: Column[StringType]

class MyFactory(Factory[DataSet[MySchema]]):
    repo_delivery = Delivery[SparkRepository[MySchema]]()
    def read(self):
        repo = self.repo_delivery.get()
        self.data = repo.load()
        return self
    # ...

pysetl = PySetl.builder().getOrCreate()  # No config or repository registered

try:
    # This will fail because the repository is missing
    pipeline = pysetl.new_pipeline().add_stage_from_type(MyFactory).run()
except Exception as e:
    print("Context error:", e)
```

**Output:**
```
Context error: No deliverable found for Delivery[SparkRepository[MySchema]]
```

- All missing dependency errors are explicit and easy to debug
- IDEs provide autocomplete and type checking for configs and repositories
- PySetl context ensures reproducibility and reliability

---

## What is the PySetl Context?
- **Spark Session Management:** Holds and manages the active `SparkSession` for
all operations.
- **Configuration Registry:** Accepts a dictionary of validated config objects
(see the configuration guide).
- **External Inputs:** Manages external deliverables (e.g., pre-built
repositories or data sources) that can be injected into pipelines.
- **Pipeline Management:** Create and register pipelines, set up pipelines with
external inputs, and retrieve/manage pipelines by UUID.
- **Repository Management:** Build and register SparkRepository objects from
config, and retrieve repositories for use as external pipeline inputs.
- **Builder Pattern:** The `PySetlBuilder` class provides a fluent interface for
constructing a `PySetl` context, including Spark config, session, and
benchmarking options.
- **Graceful Shutdown:** Provides a `stop()` method to cleanly stop the Spark
session.

---

## Full Example

Below is a fully functional example that demonstrates how to use the PySetl
context, configuration, repositories, factories, stages, and pipelines together.
For a full example, see the
[structured example](https://github.com/JhossePaul/pysetl/tree/main/examples):

```python
from typing_extensions import Self
from typedspark import DataSet, Column, Schema, create_partially_filled_dataset
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType
from pysetl import PySetl
from pysetl.enums import SaveMode
from pysetl.storage.repository import SparkRepository
from pysetl.config import CsvConfig, ParquetConfig
from pysetl.workflow import Factory, Delivery, Stage, Deliverable

spark = SparkSession.builder.getOrCreate()

class Citizen(Schema):
    name: Column[StringType]
    age: Column[IntegerType]
    city: Column[StringType]

class City(Schema):
    city: Column[StringType]
    country: Column[StringType]

class CitizenCountry(Citizen):
    country: Column[StringType]

citizens = create_partially_filled_dataset(
    spark,
    Citizen,
    [
        {Citizen.name: "Gerardo", Citizen.age: 28, Citizen.city: "Los Angeles"},
        {Citizen.name: "Paul", Citizen.age: 31, Citizen.city: "Puebla"},
    ]
)

cities = create_partially_filled_dataset(
    spark,
    City,
    [
        {City.city: "Puebla", City.country: "Mexico"},
        {City.city: "CDMX", City.country: "Mexico"},
        {City.city: "Los Angeles", City.country: "USA"}
    ]
)

fake_cities = create_partially_filled_dataset(
    spark,
    City,
    [
        {City.city: "Seoul", City.country: "Korea"},
        {City.city: "Berlin", City.country: "Germany"},
        {City.city: "Los Angeles", City.country: "USA"}
    ]
)

class CitizensFactory(Factory[DataSet[Citizen]]):
    citizens: DataSet[Citizen]
    repo_delivery = Delivery[SparkRepository[Citizen]]()

    def read(self) -> Self:
        repository = self.repo_delivery.get()
        self.citizens = repository.load()
        return self

    def process(self) -> Self:
        return self

    def write(self) -> Self:
        return self

    def get(self) -> DataSet[Citizen]:
        return self.citizens

class CitiesFactory(Factory[DataSet[City]]):
    repo_delivery = Delivery[SparkRepository[City]]()
    cities: DataSet[City]

    def read(self) -> Self:
        repository = self.repo_delivery.get()
        self.cities = repository.load()
        return self

    def process(self) -> Self:
        return self

    def write(self) -> Self:
        return self

    def get(self) -> DataSet[City]:
        return self.cities

class CitizenCountryFactory(Factory[DataSet[CitizenCountry]]):
    output: DataSet[CitizenCountry]
    citizens_delivery = Delivery[DataSet[Citizen]]()
    cities_delivery = Delivery[DataSet[City]](producer=CitiesFactory)

    def read(self) -> Self:
        self.citizens = self.citizens_delivery.get()
        self.states = self.cities_delivery.get()
        return self

    def process(self) -> Self:
        self.output = DataSet[CitizenCountry](self.citizens.join(self.states, "city"))
        return self

    def write(self) -> Self:
        return self

    def get(self) -> DataSet[CitizenCountry]:
        self.output.show()
        return self.output

config = {
    "citizens": CsvConfig(
        path="/content/citizens.csv",
        inferSchema="true",
        savemode=SaveMode.OVERWRITE
    ),
    "cities": ParquetConfig(
        path="/content/city.parquet",
        savemode=SaveMode.ERRORIFEXISTS
    )
}

pysetl = (
    PySetl.builder()
    .set_config(config)
    .getOrCreate()
    .set_spark_repository_from_config(Citizen, "citizens")
    .set_spark_repository_from_config(City, "cities")
)

repo_cities = pysetl.get_sparkrepository(City, "cities")
repo_citizens = pysetl.get_sparkrepository(Citizen, "citizens")

deliverable = Deliverable[DataSet[City]](fake_cities)

stage = (
    Stage()
    .add_factory_from_type(CitizensFactory)
    .add_factory_from_type(CitiesFactory)
)

pipeline = (
    pysetl
    .new_pipeline()
    .set_input_from_deliverable(deliverable)
    .add_stage(stage)
    .add_stage_from_type(CitizenCountryFactory)
    .run()
)

pipeline.inspector

print(pipeline.to_diagram())
```

---

## Best Practices
- Use `PySetl.builder()` or `PySetlBuilder` for flexible, reproducible context setup.
- Register all configs and external inputs before creating pipelines.
- Use `pysetl.new_pipeline()` to ensure all external inputs are injected.
- Always call `pysetl.stop()` at the end of your workflow to release Spark resources.

---

## Next Steps

- [Configuration Guide](configuration.md): Learn how to create and validate configs for your data sources
- [Data Access Layer](dal.md): See how repositories and connectors fit into your workflow
- [Workflow Guide](workflow.md): See how to build and execute pipelines using the context

For more details, see the [API Reference](../api/pysetl.md).
