# Dispatcher & Inspector: Understading the dependency injection core

The PySetl **Dispatcher** is the intelligent core responsible for managing data
flow and dependencies within your pipelines. It acts as a central registry and
resolver, ensuring that every `Factory` receives its required inputs (`Delivery`
instances) by matching them with available outputs (`Deliverable` instances). A
deep understanding of this mechanism, particularly how `Delivery` and
`Deliverable` interact and how the `Inspector` validates the graph, is crucial
for building complex, robust, and conflict-free ETL pipelines.

## The Core Components of Dependency Resolution

PySetl's dependency injection system revolves around three key concepts:
`Delivery`, `Deliverable`, and the runtime orchestration by the `Dispatcher`,
all validated and understood by the `Inspector`.

### 1. `Delivery`: The Dependency Request (Input Slot)

A `Delivery` (`src/pysetl/workflow/delivery.py`) is a **declarative input slot**
on a `Factory`. It's a promise that a `Factory` needs a specific type of data
(`T`) to perform its operations. It does not contain data itself; it's a
placeholder that the `Dispatcher` will fill at runtime.

* **Role:** Declares a `Factory`'s input dependency.
* **Key Attributes for Resolution:**
    * `producer (type[Union[Factory, External]])`: Specifies the class expected
      to *produce* the data for this `Delivery`. By default, it's `External`,
      indicating the data comes from outside the pipeline. Explicitly setting
      this helps resolve ambiguities.
    * `delivery_id (str)`: An optional, unique identifier for the `Delivery`.
      This is critical for disambiguating between multiple `Deliveries` of the
      *same type* from the *same producer*.
* **`payload_type` Property:** Returns the generic type `T` that this `Delivery`
  expects (e.g., `DataSet[Citizen]`). This is the primary mechanism the
  `Dispatcher` uses for type-based matching.

* **`get()` Method:** Used by the `Factory` to retrieve the injected value. If
  the `Delivery` hasn't been filled by the `Dispatcher`, it will raise an
  `InvalidDeliveryException`.

**Example of `Delivery` Declaration:**

```python
from pysetl.workflow import Factory, Delivery
from typedspark import DataSet
from etl.schemas import Citizen, City, JoinedData

class CitizensFactory(Factory[DataSet[Citizen]]):
    """Produces DataSet[Citizen]"""
    def read(self) -> Self: return self
    def process(self) -> Self: return self
    def write(self) -> Self: return self
    def get(self) -> DataSet[Citizen]: return DataSet[Citizen](self.spark.createDataFrame([], schema=Citizen.schema()))

class CitiesFactory(Factory[DataSet[City]]):
    """Produces DataSet[City]"""
    def read(self) -> Self: return self
    def process(self) -> Self: return self
    def write(self) -> Self: return self
    def get(self) -> DataSet[City]: return DataSet[City](self.spark.createDataFrame([], schema=City.schema()))

class JoinFactory(Factory[DataSet[JoinedData]]):
    """Consumes DataSet[Citizen] and DataSet[City]"""
    # Declaring dependencies as class attributes
    citizens_input = Delivery[DataSet[Citizen]](producer=CitizensFactory)
    cities_input = Delivery[DataSet[City]](producer=CitiesFactory)

    def read(self) -> Self:
        # Getting the injected data
        self.citizens = self.citizens_input.get()
        self.cities = self.cities_input.get()
        return self
    def process(self) -> Self: return self
    def write(self) -> Self: return self
    def get(self) -> DataSet[JoinedData]: return DataSet[JoinedData](self.spark.createDataFrame([], schema=JoinedData.schema()))
```

### 2. `Deliverable`: The Data Container (Output Value)

A `Deliverable` is a **concrete, runtime data container** that holds the actual
output data (`payload`) produced by a `Factory` or provided as an external input
to the pipeline. `Deliverables` are registered with the `Dispatcher` and are the
values that fulfill `Delivery` requests.

  * **Role:** Encapsulates the actual data produced by a `Factory` (or external
    source).
  * **Key Attributes:**
      * `payload (T)`: The actual data (e.g., a Spark `DataFrame`).
      * `producer (Union[type[Factory], type[External]])`: The `Factory` class
        (or `External` for external inputs) that generated this `Deliverable`.
      * `consumers (set[type[Factory]])`: A set of `Factory` classes that are
        expected to consume this `Deliverable`. This helps the `Dispatcher` in
        complex resolution scenarios.
      * `delivery_id (str)`: An optional identifier for the `Deliverable`,
        mirroring the `delivery_id` on the `Delivery` it's intended to match.
  * **`payload_type` Property:** Returns the type of the `payload` wrapped in a
    `DeliveryType` object for robust type comparison.
  * **`same_deliverable()` Method:** Used by the `Dispatcher` to determine if
    two `Deliverable` instances are equivalent based on payload type, common
    consumers, and producer.

**Example of `Deliverable` Creation (Internally by `Factory`):**

The `Factory`'s `deliverable` property is responsible for creating the
`Deliverable` instance from its `get()` method's output:

```python
# From src/pysetl/workflow/factory.py
class Factory(IsIdentifiable, Generic[T], HasLogger, IsWritable, ABC):
    # ...
    @property
    def deliverable(self: Self) -> Deliverable:
        """
        Return the Deliverable produced by this factory (for dispatcher registration).
        """
        __type = self.delivery_type().tp # Get the declared output type of the factory
        return Deliverable[__type](
            payload=self.get(),           # The actual data
            producer=type(self),          # This factory is the producer
            consumers=self.consumers,     # Consumers set on the factory
            delivery_id=self.delivery_id, # Delivery ID set on the factory
        )
```

### 3. `DeliveryType`: Robust Type Comparison

`DeliveryType` is an internal utility that wraps Python types to provide robust
equality checks, especially for generic types (like `list[int]`) and
`typedspark.DataSet` schemas. This ensures the `Dispatcher` can accurately match
`Delivery` requests to `Deliverable` outputs based on their underlying data
types. It handles the nuances of Python's type system to provide reliable
comparisons.

### 4. `External`: Marking External Inputs

The `External` class  is a marker used as a `producer` for `Deliveries` that
originate outside the PySetl pipeline itself.  This is crucial for
distinguishing inputs provided directly to the pipeline (e.g., via
`pipeline.set_input_from_deliverable()`) from data produced by other `Factories`
within the pipeline.

```python
# src/pysetl/workflow/external.py
from abc import ABCMeta
from typing import final
from pysetl.utils.mixins import IsIdentifiable


@final
class External(IsIdentifiable, metaclass=ABCMeta):
    """
    Marker class for external input sources in the pipeline.

    Used as the default producer for Deliveries that are not produced by a Factory.
    Enables the pipeline to represent and resolve dependencies on data that comes
    from outside the pipeline, such as user-provided data, files, or other external systems.
    """
    @classmethod
    def delivery_type(cls) -> type:
        """Returns the type of this class (used for dependency resolution)."""
        return External
```

## The `Dispatcher`: The Runtime Dependency Injector

The `Dispatcher` is the central orchestrator of data flow at runtime. It
inherits from `HasRegistry` (for managing `Deliverables`) and `HasLogger`.

**Key Responsibilities of the `Dispatcher`:**

  * **`add_deliverable(self, __deliverable: Deliverable) -> Self`**: Registers a
    `Deliverable` in its internal registry, making it available for injection.
  * **`collect_factory_deliverable(self, factory: Factory) -> Self`**: A
    convenience method to collect the `Deliverable` produced by a `Factory`
    after its execution and add it to the registry.
  * **`collect_stage_deliverables(self, stage: Stage) -> Self`**: Collects all
    `Deliverables` produced by all `Factories` within a `Stage` and registers
    them.
  * **`dispatch(self, consumer: Factory) -> Self`**: This is the main entry
    point for injecting dependencies into a `Factory`. It iterates through all
    `Delivery` instances declared by the `consumer` `Factory` and attempts to
    find a matching `Deliverable`.
  * **`dispatch_delivery(self, consumer: Factory, delivery: Delivery) ->
    None`**: The core logic for resolving a single `Delivery` dependency. It
    searches for a matching `Deliverable` and injects its `payload` into the
    `Delivery` slot using `delivery.set(deliverable.payload)`.
  * **`get_deliverable(...) -> Optional[Deliverable]`**: This is the
    sophisticated matching algorithm used by `dispatch_delivery` to find the
    correct `Deliverable` from a list of candidates.

### `Dispatcher.get_deliverable()`: The Resolution Algorithm

This method is at the heart of how PySetl resolves dependencies and avoids
conflicts. It employs a multi-step matching strategy:

1.  **Initial Filtering by Type and `delivery_id`**:
    The `dispatch_delivery` method first filters the entire `Dispatcher`
    registry to find all `Deliverables` that match the `Delivery`'s
    `payload_type` and `delivery_id`. This creates a list of
    `available_deliverables`.
2.  **Single Candidate Check**:
    If `len(available_deliverables) == 1`, the `Dispatcher` checks if the
    `consumer` `Factory` is listed in the `Deliverable`'s `consumers` set (if
    `consumers` is not empty). If this check passes, the `Deliverable` is
    returned.
3.  **Filtering by `producer`**:
    If there are multiple `available_deliverables`, the `Dispatcher` then
    filters them by the `producer` specified on the `Delivery`
    (`delivery.producer`). This is the primary mechanism to resolve conflicts
    when multiple `Factories` produce the same type of data.
4.  **Filtering by `consumer` (for ambiguous `producer` matches)**:
    If, *even after filtering by producer*, there are still multiple
    `Deliverables` that match (meaning multiple `Factories` of the *same type*
    from the *same producer* are producing the same data type, or the `producer`
    was `External` and multiple external inputs match), the `Dispatcher`
    attempts to filter further by checking if the `consumer` `Factory` is
    explicitly listed in the `Deliverable`'s `consumers` set.
5.  **Raising `InvalidDeliveryException`**:
    If, after all these steps, there are still multiple ambiguous matches, or no
    match is found, an `InvalidDeliveryException` is raised. This "fail-fast"
    behavior prevents silent, incorrect data injection.

## The `Inspector`: Validating the Pipeline Graph

The `Inspector`  plays a crucial role in validating the pipeline structure
*before* execution. It constructs a Directed Acyclic Graph (DAG) of your
pipeline, identifying all nodes (factories/external inputs) and flows (data
transfers). This pre-execution analysis helps detect potential dependency
resolution issues.

### Key Components for Inspection:

  * **`Node`**:
    Represents a `Factory` or an `External` input in the DAG. Each `Node` stores
    its `factory_class`, `factory_uuid`, `stage`, `expected_deliveries`
    (inputs), and `output` (`NodeOutput`).
      * `Node.from_factory()`: Creates a `Node` from a `Factory` instance.
      * `Node.external()`: Creates a `Node` representing an `External` input.
      * `target_node()`: A critical method that determines if another `Node` can
        be a valid consumer of this `Node`'s output, considering types,
        producers, and consumers. This method is used by the `Inspector` to
        build the `Flow`s.
  * **`NodeOutput`**:
    Contains metadata about a `Node`'s output, including its `delivery_type`,
    `consumer` set, `delivery_id`, and flags like `final_output` or `external`.
    It's a lightweight description of the data, not the data itself.
  * **`Flow`**:
    Represents a data transfer connection between two `Nodes` in the DAG. It
    connects a source `Node`'s output to a destination `Node`'s input. `Flow`
    instances are created by the `Inspector` to visualize and validate data
    paths.
  * **`DAG`**:
    A simple dataclass that holds the
    `nodes` and `flows` constructed by the `Inspector`. It ensures the graph is
    acyclic and provides methods for diagram generation.

### How the `Inspector` Detects Unresolved Cases:

The `Inspector`'s `create_flows()` method is central to detecting potential
issues. It first creates all `internal_flows` (data transfers between
`Factories` within the pipeline). Then, it calls `create_external_flows()`.

The `find_external_flows()` method within the `Inspector` is particularly
insightful:

```python
# From src/pysetl/workflow/inspector.py
    def find_external_flows(
        self: Self, node: Node, internal_flows: set[Flow]
    ) -> list[Flow]:
        """
        Find flows from external sources that are needed to satisfy dependencies.

        This is the most complex part of the Inspector's logic. It determines when
        external flows are necessary by analyzing whether internal flows can satisfy
        all dependencies. The key insight is that external flows are only created
        when internal flows are insufficient.

        The logic works as follows:

        1. Group deliveries by payload type to understand dependencies
        2. For each group, check if any deliveries have External as producer
        3. For external deliveries, verify that internal flows cannot satisfy all
           dependencies with the same delivery ID
        4. Create external flows only when internal flows are insufficient
        """
        grouped_deliveries = [
            list(deliveries)
            for _, deliveries in groupby(node.input, lambda _: _.payload_type)
        ]

        return [
            Flow(
                Node.external(
                    output=NodeOutput(
                        delivery_type=delivery.payload_type,
                        consumer=set(),
                        delivery_id=delivery.delivery_id,
                        external=True,
                    )
                ),
                node,
            )
            for deliveries in grouped_deliveries
            for delivery in deliveries
            if (delivery.producer is External) # Only consider deliveries explicitly marked as External
            and (
                self.possible_internal(internal_flows, delivery, node.factory_uuid)
                == (self.deliveries_with_same_id(deliveries, delivery.delivery_id) - 1)
            )
        ]
```

The crucial line `self.possible_internal(...) == (self.deliveries_with_same_id(...) - 1)`
is how the `Inspector` determines if an external flow is *truly* needed. It
checks if the number of internal flows that *could* satisfy a `Delivery` is one less
than the total number of `Deliveries` of that type and `delivery_id` for that
`Node`. If this condition is met, it implies that one `Delivery` of that
specific type/ID *must* come from an external source, and thus an `External`
`Node` and `Flow` are created.

If, after the `Inspector` runs, the `DAG` cannot be fully constructed or if it
detects circular dependencies, a `PipelineException` (or similar validation
error) will be raised, indicating an unresolved or invalid pipeline structure.

## Avoiding Resolution Conflicts: Practical Strategies

The `Dispatcher` and `Inspector` work together to enforce clear dependency
resolution. Here's how to structure your `Factories` and `Deliveries` to avoid
common conflicts:

### 1. Ambiguity by Type: Multiple Producers of the Same Type

If two or more `Factories` (or external inputs) produce `Deliverables` of the
exact same `payload_type`, the `Dispatcher` will face ambiguity.

**Problem Example:**

```python
# etl/schemas.py
from typedspark import DataSet, Column
from pyspark.sql.types import StringType

class CustomerData(DataSet):
    id: Column[StringType]

# etl/factories.py
class LegacyCustomerFactory(Factory[DataSet[CustomerData]]):
    # ... produces old customer data ...
    pass

class NewCustomerFactory(Factory[DataSet[CustomerData]]):
    # ... produces new customer data from a different source ...
    pass

class AnalyticsFactory(Factory[DataSet[ReportData]]):
    customer_input = Delivery[DataSet[CustomerData]]() # Ambiguous!
    # ...
```

**Solution: Explicitly Specify the `producer`**

Always specify the `producer` argument in your `Delivery` declaration when
multiple sources could provide the same type:

```python
class AnalyticsFactory(Factory[DataSet[ReportData]]):
    # Clearly state which customer data source is needed
    customer_input = Delivery[DataSet[CustomerData]](producer=NewCustomerFactory)
    # ...
```

### 2. Ambiguity by Multiple Outputs from a Single Producer

A single `Factory` might logically produce multiple distinct outputs that happen
to share the same data type.

**Problem Example:**

```python
# etl/schemas.py
class AuditLog(DataSet):
    message: Column[StringType]
    status: Column[StringType]

# etl/factories.py
class DataValidationFactory(Factory[DataSet[AuditLog]]): # Primary output: validation summary
    # This factory also conceptually produces a list of rejected records, also of type AuditLog
    # If not disambiguated, both could be seen as the same output
    # ...
    def get(self) -> DataSet[AuditLog]:
        # Returns validation summary
        pass

class ErrorReportingFactory(Factory[DataSet[ErrorReport]]):
    audit_logs = Delivery[DataSet[AuditLog]]() # Which AuditLog?
    # ...
```

**Solution: Use `delivery_id` for Disambiguation**

Assign a unique `delivery_id` to the `Factory`'s `delivery_id` attribute (for
its primary output) and to `Delivery` instances for secondary outputs.

```python
class DataValidationFactory(Factory[DataSet[AuditLog]]):
    # Primary output of this factory
    delivery_id = "validation_summary"

    # Secondary output, explicitly identified
    rejected_records_output = Delivery[DataSet[AuditLog]]() # This Delivery needs a delivery_id too!
    # It would be set on the factory instance itself or via a specific method that returns a Deliverable
    # with this delivery_id. For the factory's *primary* output, use the class-level delivery_id.

    def get(self) -> DataSet[AuditLog]:
        # Returns the primary validation summary data
        pass

    # ... (methods to produce the rejected records internally, perhaps a separate deliverable method)

class ErrorReportingFactory(Factory[DataSet[ErrorReport]]):
    # Consume the primary output
    validation_summary = Delivery[DataSet[AuditLog]](
        producer=DataValidationFactory,
        delivery_id="validation_summary"
    )
    # Consume the rejected records output
    rejected_records = Delivery[DataSet[AuditLog]](
        producer=DataValidationFactory,
        delivery_id="rejected_data_from_validation" # Example ID for the secondary output
    )
    # ...
```

For the `DataValidationFactory` to produce a `Deliverable` with
`delivery_id="rejected_data_from_validation"`, it would need to explicitly
construct and return such a `Deliverable` (perhaps via a separate
`get_rejected_records()` method that the pipeline could call, or by having
`rejected_records_output` be a `Deliverable` directly initialized within the
factory).

### 3. External Inputs and Pipeline Integration

When data originates from outside the PySetl pipeline, mark its `Delivery` with
`producer=External`. The pipeline's `set_input_from_deliverable()` method is
used to register these external `Deliverables` with the `Dispatcher`.

```python
from pysetl.workflow import Delivery, External
from typedspark import DataSet
from etl.schemas import RawApiData

class IngestionFactory(Factory[DataSet[CleanData]]):
    # Declare that raw_api_input comes from an external source
    raw_api_input = Delivery[DataSet[RawApiData]](producer=External, delivery_id="api_data_batch_1")

    def read(self) -> Self:
        self.raw_data = self.raw_api_input.get()
        return self
    # ...
```

When setting up the pipeline:

```python
from pysetl import PySetl
from pysetl.workflow import Deliverable
from typedspark import DataSet
from etl.schemas import RawApiData # Assuming RawApiData schema
from pyspark.sql import SparkSession

# Assume you have a SparkSession and some raw_df
spark = SparkSession.builder.appName("test").getOrCreate()
raw_df = spark.createDataFrame([], schema=RawApiData.schema()) # Mock raw data

# Create an external deliverable
external_api_deliverable = Deliverable[DataSet[RawApiData]](
    payload=DataSet[RawApiData](raw_df),
    producer=External,
    delivery_id="api_data_batch_1" # Must match the delivery_id in the Delivery
)

pipeline = (
    PySetl.builder()
    .getOrCreate()
    .new_pipeline()
    .set_input_from_deliverable(external_api_deliverable) # Register the external input
    .add_stage_from_type(IngestionFactory)
    # ... other stages
)
```

## Best Practices for Conflict Avoidance

  * **Be Explicit with `producer`:** This is your primary tool. If there's any
    chance of two `Factories` producing the same type, always specify the
    `producer` in the `Delivery`.
  * **Use `delivery_id` for Multiple Outputs:** If a single `Factory` needs to
    output multiple distinct `Deliverables` of the *same type*, use
    `delivery_id` to differentiate them. Ensure the `Factory`'s `delivery_id`
    attribute matches its primary output, and create `Deliverables` with
    specific `delivery_id`s for secondary outputs.
  * **Distinct Schemas:** Leverage `typedspark.DataSet` to define distinct
    schemas for logically different datasets, even if their underlying Spark
    types might be similar. This provides strong type-based differentiation.
  * **Clear Naming Conventions:** Use descriptive names for your `Delivery`
    class attributes (e.g., `customer_data_delivery`, `product_info_delivery`)
    to improve readability and make potential conflicts easier to spot during
    code review.
  * **Rely on the `Inspector`:** The `Inspector`'s pre-execution validation is
    invaluable. Run it early and often in your development cycle to catch
    dependency resolution issues before they manifest as runtime errors. The
    detailed output of `Inspector.graph` can help visualize complex
    dependencies.

By understanding and applying these principles, you can effectively leverage
PySetl's powerful `Dispatcher` to build complex, reliable, and maintainable ETL
pipelines free from ambiguous dependency resolutions.
