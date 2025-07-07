# Custom Factories

The `Factory` is the cornerstone of business logic in PySetl. It provides a
structured, modular, and testable way to encapsulate your ETL transformations.
While the `Factory` abstract base class defines a clear lifecycle (`read`,
`process`, `write`, `get`), PySetl encourages you to extend it with custom
initialization, internal methods, and additional properties to suit your
specific needs.

## The `Factory` Lifecycle (Review)

Before diving into customization, let's briefly recap the mandatory methods of a
`Factory`:

* **`read(self) -> Self`**: Responsible for reading input data or dependencies.
  This is where you would typically use `self.my_delivery.get()` to retrieve
  injected data.
* **`process(self) -> Self`**: Contains the core business logic and data
  transformations.
* **`write(self) -> Self`**: Handles persisting the transformed data to a sink.
  This step is optional and can be skipped if the factory's output is only for
  an intermediate dependency.
* **`get(self) -> Optional[T]`**: Returns the final output data of the factory.
  This output will be wrapped in a `Deliverable` by the pipeline and made
  available to downstream consumers.

Each of these methods should return `self` to enable fluent method chaining
(`factory.read().process().write()`).

## Customizing Your `Factory`

Extending the `Factory` class allows you to inject parameters, implement complex
internal logic, and leverage mixins for cross-cutting concerns.

### 1. Custom `__init__` Method

The `Factory` class itself has a `__post_init__` method for internal framework
setup (like `IsIdentifiable`). When you define your own `__init__` in a
subclass, you do **not** need to call `super().__init__()` or
`super().__post_init__()` explicitly, as the PySetl framework handles the proper
initialization of mixins and base classes during factory instantiation.

Your custom `__init__` is where you can define parameters that configure your
factory's behavior. These parameters are typically passed when you add the
factory to a `Stage` or `Pipeline` using methods like `add_factory_from_type`.

**Example: `CustomParameterFactory`**

Let's imagine a factory that processes data based on a configurable `threshold`
and a `mode`.

```python
from pysetl.workflow import Factory, Delivery
from typedspark import DataSet
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from typing_extensions import Self
# Assume these schemas are defined in etl/schemas.py
# from etl.schemas import InputSchema, OutputSchema

# Mock schemas for demonstration purposes
class InputSchema(DataSet):
    value: Column[IntegerType]
    data: Column[StringType]

class OutputSchema(DataSet):
    value: Column[IntegerType]
    data: Column[StringType]

class CustomParameterFactory(Factory[DataSet[OutputSchema]]):
    """
    A factory demonstrating custom initialization and internal methods.
    Processes data based on a threshold and a mode.
    """
    # Declare input dependency
    input_data_delivery = Delivery[DataSet[InputSchema]]()

    def __init__(self, threshold: int, mode: str = "strict") -> None:
        """
        Initializes the CustomParameterFactory with a threshold and processing mode.

        Args:
            threshold (int): The numerical threshold for processing.
            mode (str): The processing mode ('strict' or 'loose'). Defaults to 'strict'.
        """
        # Store custom parameters as instance attributes
        self.threshold = threshold
        self.mode = mode
        self.input_df: DataFrame = None # Will be populated in read()

        # No need to call super().__init__() or super().__post_init__().
        # PySetl handles mixin and base class initialization automatically.
        self.log_info(f"CustomParameterFactory initialized with threshold={self.threshold}, mode={self.mode}")

    def read(self) -> Self:
        """Reads the input data from the delivery."""
        self.input_df = self.input_data_delivery.get()
        self.log_info(f"Read input data with {self.input_df.count()} rows.")
        return self

    def process(self) -> Self:
        """Applies transformation based on threshold and mode."""
        if self.mode == "strict":
            self.processed_df = self.input_df.filter(col("value") > self.threshold)
            self.log_info(f"Processed in strict mode. Rows after filter: {self.processed_df.count()}")
        else:
            self.processed_df = self.input_df.filter(col("value") >= self.threshold)
            self.log_info(f"Processed in loose mode. Rows after filter: {self.processed_df.count()}")
        return self

    def write(self) -> Self:
        """Writes the processed data (optional)."""
        # In a real scenario, you'd use a connector here
        self.log_info("Writing processed data (simulated).")
        # self.processed_df.write.parquet("/path/to/output")
        return self

    def get(self) -> DataSet[OutputSchema]:
        """Returns the processed data as a DataSet."""
        return DataSet[OutputSchema](self.processed_df)

# --- How to add this factory to a Pipeline or Stage ---
from pysetl import PySetl
from pysetl.workflow import Stage, Deliverable, External
from pyspark.sql.types import IntegerType, StringType

# Mock SparkSession and initial data for example
spark = SparkSession.builder.appName("CustomFactoryExample").getOrCreate()
initial_data_df = spark.createDataFrame([(10, "A"), (150, "B"), (50, "C")], ["value", "data"])
initial_data = Deliverable[DataSet[InputSchema]](
    payload=DataSet[InputSchema](initial_data_df),
    producer=External
)

# 1. Adding to a Pipeline directly using add_stage_from_type
# This is the most common way to pass arguments to a Factory's __init__
pipeline = (
    PySetl.builder()
    .getOrCreate()
    .new_pipeline()
    .set_input_from_deliverable(initial_data)
    .add_stage_from_type(CustomParameterFactory, threshold=100, mode="loose") # Pass args/kwargs here
)
# pipeline.run() # Uncomment to run the pipeline

# 2. Adding to a Stage first, then adding the Stage to a Pipeline
my_stage = (
    Stage()
    .add_factory_from_type(CustomParameterFactory, threshold=50, mode="strict") # Pass args/kwargs here
)

another_pipeline = (
    PySetl.builder()
    .getOrCreate()
    .new_pipeline()
    .set_input_from_deliverable(initial_data)
    .add_stage(my_stage)
)
# another_pipeline.run() # Uncomment to run the pipeline
```

As demonstrated above, the `Pipeline.add_stage_from_type()` and
`Stage.add_factory_from_type()` methods are designed to accept `*args` and
`**kwargs`. These arguments are then directly forwarded to the `__init__` method
of the `Factory` class being instantiated. This provides a clean and flexible
way to configure your factories when building your pipeline.

### 2. Adding Custom Internal Methods and Properties

You are free to add any number of custom methods or properties to your `Factory`
subclasses. These methods can encapsulate complex logic, break down the
`process` method into smaller, more manageable units, or provide utility
functions specific to your factory's domain.

**Example: `DataCleaningFactory` with Helper Methods**

```python
from pysetl.workflow import Factory, Delivery
from typedspark import DataSet
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, trim
from typing_extensions import Self
# Assume these schemas are defined in etl/schemas.py
# from etl.schemas import RawData, CleanedData

# Mock schemas for demonstration purposes
class RawData(DataSet):
    name: Column[StringType]
    age: Column[IntegerType]
    city: Column[StringType]

class CleanedData(DataSet):
    name: Column[StringType]
    age: Column[IntegerType]
    city: Column[StringType]


class DataCleaningFactory(Factory[DataSet[CleanedData]]):
    """
    A factory that cleans raw data, demonstrating internal helper methods.
    """
    raw_data_delivery = Delivery[DataSet[RawData]]()

    def read(self) -> Self:
        self.raw_df = self.raw_data_delivery.get()
        return self

    def process(self) -> Self:
        # Call internal helper methods to break down complex logic
        df = self._standardize_strings(self.raw_df)
        df = self._handle_missing_values(df)
        self.cleaned_df = df
        return self

    def write(self) -> Self:
        # Logic to write self.cleaned_df
        self.log_info("Writing cleaned data (simulated).")
        return self

    def get(self) -> DataSet[CleanedData]:
        return DataSet[CleanedData](self.cleaned_df)

    # --- Custom Internal Methods ---
    def _standardize_strings(self, df: DataFrame) -> DataFrame:
        """Internal helper to trim whitespace from string columns."""
        self.log_debug("Standardizing string columns...")
        for c in df.columns:
            # Check if column is of string type before applying trim
            if df.schema[c].dataType.simpleString() == "string":
                df = df.withColumn(c, trim(col(c)))
        return df

    def _handle_missing_values(self, df: DataFrame) -> DataFrame:
        """Internal helper to fill missing values for specific columns."""
        self.log_debug("Handling missing values...")
        # Example: Fill nulls in 'age' column with 0
        if "age" in df.columns and df.schema["age"].dataType.simpleString() in ["integer", "long", "float", "double"]:
            df = df.withColumn("age", when(col("age").isNull(), 0).otherwise(col("age")))
        return df

    @property
    def _is_production_ready(self) -> bool:
        """A custom property to check if the factory is ready for production."""
        # Example: Check if all necessary configs are set or data quality checks passed
        return True # Placeholder
```

### 3. Leveraging Mixins

As seen in the `Connector` documentation, PySetl extensively uses mixins (e.g.,
`HasLogger`, `HasSparkSession`, `HasBenchmark`) to inject common
functionalities. You can include these mixins in your `Factory` definition to
gain their capabilities without polluting your factory's core logic.

**Example: `BenchmarkedFactory`**

```python
from pysetl.workflow import Factory, Delivery
from pysetl.utils.mixins import HasBenchmark # Import the mixin
from typedspark import DataSet
from pyspark.sql import DataFrame
from typing_extensions import Self
# Assume these schemas are defined in etl/schemas.py
# from etl.schemas import LargeInput, AggregatedOutput

# Mock schemas for demonstration purposes
class LargeInput(DataSet):
    category: Column[StringType]
    value: Column[IntegerType]

class AggregatedOutput(DataSet):
    category: Column[StringType]
    count: Column[LongType]


class AggregationFactory(Factory[DataSet[AggregatedOutput]], HasBenchmark):
    """
    A factory that performs aggregation and uses the HasBenchmark mixin.
    """
    large_input_delivery = Delivery[DataSet[LargeInput]]()

    def read(self) -> Self:
        with self.benchmark_read: # Use the benchmark context manager from HasBenchmark
            self.input_df = self.large_input_delivery.get()
        return self

    def process(self) -> Self:
        with self.benchmark_process: # Benchmark the process step
            self.aggregated_df = self.input_df.groupBy("category").count()
        return self

    def write(self) -> Self:
        with self.benchmark_write: # Benchmark the write step
            # In a real scenario, you'd use a connector here
            # self.aggregated_df.write.parquet("/output/aggregated")
            pass # Simulated write
        return self

    def get(self) -> DataSet[AggregatedOutput]:
        return DataSet[AggregatedOutput](self.aggregated_df)

# When this factory runs within a Stage that has benchmarking enabled, the times
# for read, process, and write will be automatically collected via the
# HasBenchmark mixin.
```

## Summary of Customization Benefits

  * **Encapsulation:** Keep factory-specific configuration and logic within the
    factory class.
  * **Readability:** Break down complex transformations into smaller, named
    methods.
  * **Reusability:** Custom methods can be reused within the same factory or
    even across different factories if extracted into utility functions.
    **Testability:** Private helper methods can often be tested independently (though typically you'd test the `process` method end-to-end).
  * **Flexibility:** Adapt your factories to any specific requirement without
    modifying the core PySetl framework.

By embracing these customization patterns, you can build highly organized,
efficient, and maintainable ETL solutions with PySetl.
