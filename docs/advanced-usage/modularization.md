# Modularization: Building Scalable and Maintainable ETL Projects

PySetl's core design philosophy champions modularity, recognizing it as
fundamental for building robust, scalable, and maintainable ETL solutions. In
large data engineering projects, a monolithic approach quickly leads to
unmanageable codebases, difficult debugging, and slow development cycles. PySetl
provides the tools and encourages principles to break down complex workflows
into small, independent, and reusable components.

## Why Modularity Matters in ETL

Modular design in ETL offers significant advantages:

* **Maintainability:** Smaller, focused components are easier to understand,
  debug, and modify without affecting unrelated parts of the system.
* **Scalability:** Individual components can be optimized, scaled, or even
  replaced independently as data volumes or requirements change.
* **Reusability:** Well-defined modules can be reused across different
  pipelines, projects, or even different parts of the same pipeline, reducing
  redundant code.
* **Testability:** Isolated units are inherently easier to unit test, leading to
  higher code quality and confidence in deployments.
* **Collaboration:** Multiple developers or teams can work concurrently on
  different modules with minimal conflict, accelerating development.
* **Onboarding:** New team members can quickly grasp the purpose and
* functionality of individual components without needing to understand the
  entire system at once.

## PySetl's Pillars of Modularity

PySetl's architecture is built upon several key abstractions that enforce and
facilitate modularity:

1.  **Factories as Atomic Units:**
    The `Factory` is the primary unit of business logic. Each `Factory` should
    encapsulate a single, well-defined responsibility (e.g., reading a specific
    dataset, performing a single transformation, writing to a particular sink).
    This "single responsibility principle" is paramount.

2.  **Declarative Dependencies (`Delivery` / `Deliverable`):**
    The `Delivery` and `Deliverable` system, is a cornerstone of PySetl's
    modularity. By explicitly declaring dependencies as `Delivery` objects,
    factories clearly state what they *need* and what they *produce*. This
    eliminates hidden dependencies, making interfaces explicit and enabling the
    `Dispatcher` to automatically wire components together based on type and
    `delivery_id`. This declarative nature prevents components from tightly
    coupling to each other's internal implementation details.

3.  **Stages for Logical Grouping:**
    `Stages` provide a way to group related `Factories` sequentially. While
    `Factories` are atomic, `Stages` offer a higher level of organization,
    allowing you to segment your pipeline into logical phases (e.g., "Extraction
    Stage," "Cleaning Stage," "Aggregation Stage").

4.  **Type Safety (especially with `typedspark.DataSet`):**
    PySetl's strong emphasis on type annotations, particularly with
    `typedspark.DataSet` for Spark DataFrames, acts as a formal contract between
    `Factories`. When a `Factory` declares it produces
    `DataSet[CustomerSchema]`, and another declares it consumes
    `DataSet[CustomerSchema]`, the exact structure of that data is known and
    validated. This prevents implicit coupling where components rely on
    undocumented column names or types, making refactoring safer and interfaces
    clearer.

5.  **Connectors and Repositories:**
    `Connectors` and `Repositories` abstract away the complexities of data
    access. By using these abstractions, your `Factories` remain independent of
    the underlying storage technology (e.g., a `CustomerDataFactory` doesn't
    care if data comes from S3, HDFS, or a database; it just receives a
    `DataSet[CustomerSchema]`). This promotes reusability of factories across
    different environments or data sources.

## Strategies for Modularizing Large ETL Projects

Leveraging PySetl's design, here are practical strategies for structuring your
large ETL projects:

### 1. Logical Directory Structure

Organize your project files in a way that mirrors PySetl's components and your
business domains. This makes it easy to locate, understand, and manage different
parts of your ETL system.

```
my\_pysetl\_project/
├── src/
│   └── my\_project/
│       ├── config/             # Custom ConfigModels and Config classes (e.g., DbConfig, ApiConfig)
│       ├── enums/              # Project-specific enums (if any, in addition to pysetl.enums)
│       ├── schemas/            # All TypedSpark DataSet schemas (e.g., RawCustomer, CleanedCustomer, AggregatedSales)
│       ├── connectors/         # Custom Connector implementations (e.g., SalesforceConnector, KafkaConnector)
│       ├── factories/
│       │   ├── raw\_layer/      # Factories for initial data ingestion/standardization
│       │   │   └── customer\_ingestion.py
│       │   │   └── order\_ingestion.py
│       │   ├── clean\_layer/    # Factories for data cleaning and transformation
│       │   │   └── customer\_cleaning.py
│       │   │   └── order\_enrichment.py
│       │   ├── curated\_layer/  # Factories for business-ready, joined datasets
│       │   │   └── sales\_fact\_factory.py
│       │   └── aggregates/     # Factories for final aggregated datasets
│       │       └── daily\_sales\_agg.py
│       ├── pipelines/          # Main pipeline definitions, orchestrating stages and factories
│       │   └── daily\_etl\_pipeline.py
│       │   └── hourly\_api\_pipeline.py
│       └── utils/              # Project-specific utilities, custom mixins, helper functions
├── tests/                      # Corresponding test structure mirroring src/
│   ├── config/
│   ├── connectors/
│   ├── factories/
│   └── pipelines/
├── notebooks/                  # For ad-hoc analysis, prototyping
├── conf/                       # Configuration files (e.g., YAML, JSON)
└── README.md

```

### 2. Granularity of Factories

Avoid creating "God Factories" that try to do too much. Instead, break down
complex transformations into a sequence of smaller, single-purpose factories.
This improves readability, testability, and reusability.

**Bad Example (Monolithic Factory):**

```python
class ComplexCustomerFactory(Factory[DataSet[FinalReport]]):
    # Reads, cleans, joins, aggregates, and validates all in one
    # ... huge read, process, write methods ...
```

**Good Example (Modular Factories):**

```python
# Stage 1: Ingestion
class RawCustomerIngestion(Factory[DataSet[RawCustomer]]): ...
class RawOrderIngestion(Factory[DataSet[RawOrder]]): ...

# Stage 2: Cleaning & Standardization
class CleanCustomerData(Factory[DataSet[CleanedCustomer]]): ...
class StandardizeOrderData(Factory[DataSet[StandardizedOrder]]): ...

# Stage 3: Joining & Enrichment
class JoinCustomerOrders(Factory[DataSet[CustomerOrderFact]]): ...
class EnrichCustomerData(Factory[DataSet[EnrichedCustomer]]): ...

# Stage 4: Aggregation
class DailySalesAggregation(Factory[DataSet[DailySalesSummary]]): ...
```

Each of these factories would have its own `read`, `process`, `write`, and `get` methods, focusing solely on its declared task.

### 3\. Reusability through Parameters and Custom Mixins

  * **Configurable Factories (`__init__` Parameters):**
    Design your factories to accept configuration parameters via their `__init__` method. This allows you to reuse the same factory logic with different behaviors or inputs without duplicating code. As shown in "Extending Factories," these parameters are easily passed when adding factories to a pipeline via `add_stage_from_type(*args, **kwargs)`.

    ```python
    # Example: A generic filtering factory
    class FilteringFactory(Factory[DataSet[FilteredData]]):
        input_data = Delivery[DataSet[RawData]]()

        def __init__(self, filter_column: str, filter_value: Any):
            self.filter_column = filter_column
            self.filter_value = filter_value
            # ...

        def process(self) -> Self:
            self.filtered_df = self.input_df.filter(col(self.filter_column) == self.filter_value)
            return self

    # Reuse in pipeline:
    # pipeline.add_stage_from_type(FilteringFactory, filter_column="status", filter_value="ACTIVE")
    # pipeline.add_stage_from_type(FilteringFactory, filter_column="country", filter_value="USA")
    ```

  * **Custom Mixins:**
    For behaviors or properties that are shared across multiple components (not just factories), but don't fit into a strict inheritance hierarchy, custom mixins are an elegant solution. They allow you to "mix in" functionalities like logging, benchmarking, or custom data quality checks.

    **Example: A `HasDataQualityChecks` Mixin**

    ```python
    # src/my_project/utils/mixins/has_data_quality_checks.py
    from abc import ABC, abstractmethod
    from pyspark.sql import DataFrame
    from typing_extensions import Self
    from pysetl.utils.mixins import HasLogger # Can compose existing mixins

    class HasDataQualityChecks(HasLogger, ABC):
        """
        Mixin for classes that perform data quality checks.
        Requires concrete implementation of 'run_quality_checks'.
        """
        @abstractmethod
        def run_quality_checks(self, df: DataFrame) -> DataFrame:
            """
            Abstract method to implement specific data quality checks.
            Should return the DataFrame after applying checks (e.g., dropping bad rows).
            """
            pass

        def _log_quality_summary(self, initial_count: int, final_count: int) -> None:
            """Helper to log the impact of quality checks."""
            dropped_rows = initial_count - final_count
            if dropped_rows > 0:
                self.log_warning(f"Data quality checks dropped {dropped_rows} rows.")
            else:
                self.log_info("No rows dropped by data quality checks.")

    # How to use this mixin in a Factory:
    # etl/factories/clean_layer/customer_cleaning.py
    class CustomerCleaningFactory(Factory[DataSet[CleanedCustomer]], HasDataQualityChecks):
        raw_customer_input = Delivery[DataSet[RawCustomer]]()

        def read(self) -> Self:
            self.raw_df = self.raw_customer_input.get()
            return self

        def process(self) -> Self:
            initial_count = self.raw_df.count()
            cleaned_df = self.run_quality_checks(self.raw_df) # Call the mixin's abstract method
            self._log_quality_summary(initial_count, cleaned_df.count()) # Use mixin's helper
            self.cleaned_df = cleaned_df
            return self

        def run_quality_checks(self, df: DataFrame) -> DataFrame:
            """Concrete implementation of quality checks for customer data."""
            # Example: Drop rows where customer_id is null
            return df.filter(col("customer_id").isNotNull())
    ```

### 4\. Data Contracts as Schemas

Define clear `typedspark.DataSet` schemas for all data flowing between your factories. These schemas act as explicit data contracts, ensuring that producers and consumers agree on the data structure. This is crucial for independent development and refactoring.

### 5\. Pipeline Composition (Implicitly via Stages)

While PySetl doesn't directly nest `Pipeline` objects, it achieves composition by allowing you to build complex pipelines from modular `Stages` and `Factories`. You can create reusable `Stage` instances (e.g., a `StandardCleaningStage` that bundles several cleaning factories) and then add them to various pipelines.

### 6\. Testing as an Enabler of Modularity

Modular design directly translates to easier testing. Each `Factory`, `Connector`, or custom `Mixin` can be unit-tested in isolation by mocking its dependencies. This allows for rapid feedback and high test coverage, reinforcing the benefits of modularity.

## Benefits of a Modular PySetl Project

By embracing modularization principles within PySetl, your ETL projects will be:

  * **Highly Maintainable:** Easier to understand, debug, and evolve.
  * **Scalable and Flexible:** Adaptable to changing data volumes and business requirements.
  * **Efficiently Reusable:** Components become building blocks, reducing development time.
  * **Rigorously Tested:** Isolated units lead to comprehensive and reliable testing.
  * **Team-Friendly:** Facilitates parallel development and smoother collaboration.

Modularization is not just a coding style; it's a strategic approach to building resilient and future-proof data platforms with PySetl.
