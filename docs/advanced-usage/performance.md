# Performance Optimization: Benchmarking and Caching Strategies

Optimizing the performance of ETL pipelines is crucial for handling large
datasets efficiently and meeting strict processing SLAs. PySetl provides
built-in tools for **benchmarking** to help you identify performance bottlenecks
and encourages **caching strategies** to reduce redundant computations.

## Benchmarking Your PySetl Pipelines

PySetl integrates a simple yet effective benchmarking mechanism that allows you
to measure the execution time of individual `Factory` phases (`read`, `process`,
`write`) and entire `Stages`. This data is invaluable for pinpointing where your
pipeline spends the most time.

### 1. Enabling Benchmarking

Benchmarking is controlled by the `benchmarked` flag on `Pipeline` and `Stage`
instances.

* **Pipeline-level Benchmarking:** When you enable benchmarking on the
* `Pipeline`, all stages and their factories will be benchmarked by default.

    ```python
    from pysetl import PySetl
    # Enable benchmarking for the entire pipeline
    pipeline = PySetl.builder().getOrCreate().new_pipeline(benchmark=True)
    ```

* **Stage-level Benchmarking:** You can also enable benchmarking for specific
  stages, overriding the pipeline's setting.

    ```python
    from pysetl.workflow import Stage
    # ... define my_factory ...
    my_stage = Stage(benckmark=True).add_factory(my_factory) # Note the 'benckmark' typo in source
    pipeline.add_stage(my_stage)
    ```

### 2. How Benchmarking Works (Under the Hood)

PySetl's benchmarking relies on two core components:

* **`HasBenchmark` Mixin:**
    This abstract mixin provides the interface for benchmarking. `Stage` and
    `Pipeline` implement this mixin, allowing them to collect benchmark results.
    It defines an abstract `get_benchmarks()` method that concrete classes must
    implement to return their collected `BenchmarkResult` instances.

* **`BenchmarkModifier`:**
    This is a clever wrapper that dynamically intercepts method calls (`read`,
    `process`, `write`) on a `Factory` instance. When a `Factory` is passed to a
    benchmarked `Stage`, the `Stage` wraps the `Factory` with
    `BenchmarkModifier`.
    ```python
    # Simplified from Stage.handle_benchmark
    from pysetl.utils import BenchmarkModifier
    # ...
    benckmarked_factory = BenchmarkModifier(factory).get()
    executed_factory = self.execute_factory(benckmarked_factory)
    # ...
    benchmark_times: dict = executed_factory.times # BenchmarkModifier adds 'times' attribute
    ```
    The `BenchmarkModifier` adds a `times` dictionary attribute to the wrapped
    object, storing the execution duration for each benchmarked method.

* **`BenchmarkResult`:**
    A simple dataclass that encapsulates the timing information for a single
    factory's execution, including `read`, `process`, `write` times, and the
    `total` elapsed time.

    ```python
    from dataclasses import dataclass

    @dataclass
    class BenchmarkResult:
        cls: str
        read: float
        process: float
        write: float
        total: float

        def __str__(self) -> str:
            return "\n".join(
                [
                    f"Benchmark class: {self.cls}",
                    f"Total elapsed time: {self.total} s",
                    f"read: {self.read} s",
                    f"process: {self.process} s",
                    f"write: {self.write} s",
                    "=================",
                ]
            )
    ```

### 3. Retrieving and Interpreting Benchmark Results

After running a pipeline with benchmarking enabled, you can retrieve the `BenchmarkResult` objects from the `Pipeline` instance.

```python
from pysetl import PySetl
from pysetl.workflow import Factory, Delivery, Stage
from typedspark import DataSet, Column
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType
import time

# Mock schemas and factories for demonstration
class DataA(DataSet):
    id: Column[IntegerType]
    value: Column[StringType]

class DataB(DataSet):
    id: Column[IntegerType]
    processed_value: Column[StringType]

class DataC(DataSet):
    id: Column[IntegerType]
    final_value: Column[StringType]

class SourceFactory(Factory[DataSet[DataA]]):
    def read(self) -> Self:
        self.df = self.spark.createDataFrame([(i, f"val_{i}") for i in range(10000)], ["id", "value"])
        time.sleep(0.1) # Simulate work
        return self
    def process(self) -> Self:
        time.sleep(0.05) # Simulate work
        return self
    def write(self) -> Self:
        time.sleep(0.2) # Simulate work
        return self
    def get(self) -> DataSet[DataA]: return DataSet[DataA](self.df)

class TransformFactory(Factory[DataSet[DataB]]):
    input_data = Delivery[DataSet[DataA]](producer=SourceFactory)
    def read(self) -> Self:
        self.df = self.input_data.get()
        time.sleep(0.08) # Simulate work
        return self
    def process(self) -> Self:
        self.processed_df = self.df.withColumn("processed_value", col("value").cast(StringType()))
        time.sleep(0.15) # Simulate work
        return self
    def write(self) -> Self:
        time.sleep(0.03) # Simulate work
        return self
    def get(self) -> DataSet[DataB]: return DataSet[DataB](self.processed_df)

class SinkFactory(Factory[DataSet[DataC]]):
    input_data = Delivery[DataSet[DataB]](producer=TransformFactory)
    def read(self) -> Self:
        self.df = self.input_data.get()
        time.sleep(0.02) # Simulate work
        return self
    def process(self) -> Self:
        self.final_df = self.df.withColumn("final_value", col("processed_value"))
        time.sleep(0.07) # Simulate work
        return self
    def write(self) -> Self:
        time.sleep(0.1) # Simulate work
        return self
    def get(self) -> DataSet[DataC]: return DataSet[DataC](self.final_df)

# Create SparkSession
spark = SparkSession.builder.appName("BenchmarkExample").master("local[*]").getOrCreate()

# Build and run the pipeline with benchmarking enabled
pipeline = (
    PySetl.builder()
    .getOrCreate()
    .new_pipeline(benchmark=True) # Enable benchmarking
    .add_stage_from_type(SourceFactory)
    .add_stage_from_type(TransformFactory)
    .add_stage_from_type(SinkFactory)
)

pipeline.run()

# Retrieve and print benchmark results
print("\n--- Pipeline Benchmark Results ---")
for result in pipeline.get_benchmarks():
    print(result)

spark.stop()
```

**Interpreting the Results:**

The output of `pipeline.get_benchmarks()` will provide a `BenchmarkResult` for
each factory that ran. Each `BenchmarkResult` breaks down the total time into
`read`, `process`, and `write` phases.

  * **High `read` time:** Could indicate slow data sources, network latency,
    inefficient data loading, or large data volumes being read.
  * **High `process` time:** Points to complex transformations, inefficient
  * Spark operations (e.g., too many shuffles, UDFs), or data skew. This is
    often where most optimization efforts are focused.
  * **High `write` time:** Might suggest slow sinks, network bottlenecks during
    data persistence, or issues with partitioning/file formats.
    **`total` time:** The overall time taken for that factory.

By analyzing these timings, you can identify which factories or which phases
within a factory are the primary bottlenecks and direct your optimization
efforts accordingly.

## Caching Strategies for Performance Optimization

Caching is a fundamental optimization technique in Spark-based ETL that can
significantly improve performance by avoiding re-computation of `DataFrames`.
When a `DataFrame` is cached, Spark keeps its data in memory (or on disk) after
its first computation, making subsequent access much faster.

### When to Cache

Consider caching a `DataFrame` when:

1.  **It is used multiple times:** If a `DataFrame` is consumed by several
downstream factories or is accessed repeatedly within a single factory's
`process` method.
2.  **It is computationally expensive to produce:** If the `DataFrame` involves
complex transformations, joins, or aggregations that take a long time to
compute.
3.  **It is an intermediate result:** Caching intermediate results can break the
lineage, making subsequent operations faster by starting from the cached data
instead of re-evaluating the entire lineage from the source.

### How to Implement Caching in PySetl Factories

In PySetl, caching is typically applied within the `process` method of a
`Factory` before its output is returned (or used by subsequent operations). You
directly use Spark's `cache()` or `persist()` methods on the `DataFrame`.

**Example: Caching an Intermediate Result**

Let's modify our `TransformFactory` to cache its output if it's expected to be
consumed by multiple downstream factories.

```python
from pysetl.workflow import Factory, Delivery
from typedspark import DataSet, Column
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pyspark.storagelevel import StorageLevel # Import StorageLevel for persist()
from typing_extensions import Self

# Assume schemas DataA, DataB, DataC, and factories SourceFactory, SinkFactory are defined

class TransformFactoryWithCache(Factory[DataSet[DataB]]):
    input_data = Delivery[DataSet[DataA]](producer=SourceFactory)

    def __init__(self, should_cache: bool = False):
        self.should_cache = should_cache
        self.df: DataFrame = None
        self.processed_df: DataFrame = None

    def read(self) -> Self:
        self.df = self.input_data.get()
        return self

    def process(self) -> Self:
        self.processed_df = self.df.withColumn("processed_value", col("value").cast(StringType()))

        if self.should_cache:
            self.log_info(f"Caching intermediate result of TransformFactoryWithCache with {self.processed_df.count()} rows.")
            # Use cache() for MEMORY_AND_DISK_SER by default, or persist() for more control
            self.processed_df.persist(StorageLevel.MEMORY_AND_DISK) # Example: persist to memory and disk
            self.processed_df.count() # Trigger immediate caching action
        return self

    def write(self) -> Self:
        # If this factory also writes, it would use the cached_df
        return self

    def get(self) -> DataSet[DataB]:
        return DataSet[DataB](self.processed_df)

# Build and run a pipeline using the caching factory
spark = SparkSession.builder.appName("CachingExample").master("local[*]").getOrCreate()

# Scenario 1: Without caching
pipeline_no_cache = (
    PySetl.builder()
    .getOrCreate()
    .new_pipeline(benchmark=True)
    .add_stage_from_type(SourceFactory)
    .add_stage_from_type(TransformFactoryWithCache, should_cache=False)
    .add_stage_from_type(SinkFactory)
)
print("\n--- Running Pipeline WITHOUT Caching ---")
pipeline_no_cache.run()
for result in pipeline_no_cache.get_benchmarks():
    print(result)

# Scenario 2: With caching
pipeline_with_cache = (
    PySetl.builder()
    .getOrCreate()
    .new_pipeline(benchmark=True)
    .add_stage_from_type(SourceFactory)
    .add_stage_from_type(TransformFactoryWithCache, should_cache=True) # Enable caching
    .add_stage_from_type(SinkFactory)
)
print("\n--- Running Pipeline WITH Caching ---")
pipeline_with_cache.run()
for result in pipeline_with_cache.get_benchmarks():
    print(result)

spark.stop()
```

When comparing the benchmark results, you'll typically observe that the `read`
time of `SinkFactory` (or any factory consuming the cached output) is
significantly reduced when caching is enabled on `TransformFactoryWithCache`,
because it's reading from memory/disk instead of re-computing the entire
lineage.

### Unpersisting Cached Data

It's important to `unpersist()` cached `DataFrames` when they are no longer
needed, especially in long-running applications or pipelines with many stages,
to free up memory and disk space.

You can add a `cleanup` method to your factory or manage unpersisting at the
pipeline level if you have a clear understanding of when data is no longer
required.

```python
# Example of unpersisting in a Factory's write or a dedicated cleanup method
class TransformFactoryWithCache(Factory[DataSet[DataB]]):
    # ... (previous code) ...

    def write(self) -> Self:
        # After writing, if this is the last use, unpersist
        if self.should_cache and self.processed_df is not None:
            self.processed_df.unpersist()
            self.log_info("Unpersisted cached DataFrame.")
        return self

    # Or a dedicated method that could be called by the pipeline
    def cleanup_cache(self) -> None:
        if self.should_cache and self.processed_df is not None:
            self.processed_df.unpersist()
            self.log_info("Unpersisted cached DataFrame during cleanup.")
```

### Considerations for Caching:

  * **Memory vs. Disk:** `cache()` defaults to `MEMORY_AND_DISK_SER`.
    `persist()` allows you to specify `StorageLevel` (e.g., `MEMORY_ONLY`,
    `DISK_ONLY`, `OFF_HEAP`). Choose based on data size, reusability, and memory
    availability.
  * **Cost of Caching:** Caching itself has a cost (computation to fill the
    cache, and memory/disk overhead). Don't cache indiscriminately. Only cache
    when the cost of re-computation outweighs the cost of caching.
  * **Lazy Evaluation:** Spark's lazy evaluation means `cache()` only marks a
    `DataFrame` for caching. The actual caching happens when an action (like
    `count()`, `show()`, `write()`) is performed on the `DataFrame`. It's good
    practice to trigger an action immediately after `cache()` if you want it to
    be available for the next step.
  * **Data Skew:** Caching can sometimes exacerbate data skew if one partition
    is much larger than others, leading to OOM errors on specific executors.

By effectively using PySetl's benchmarking tools and strategically applying
Spark caching, you can significantly enhance the performance and efficiency of
your ETL pipelines.
