# Advanced Usage Topics

This section delves into advanced concepts and techniques for building
sophisticated, scalable, and highly maintainable ETL pipelines with PySetl.
While the [User Guide](../user-guide/index.md) covers the fundamentals, these
topics provide deeper insights into customizing PySetl's core components,
optimizing performance, and structuring large-scale projects.

Explore the following advanced areas to unlock the full potential of PySetl:

## Configuration Management

* **[Builders for Configuration](config-builder.md)**
    Learn how to leverage `ConfigBuilder` to construct and manage your ETL
    configurations efficiently, loading settings from various sources like
    dictionaries and objects, and ensuring type-safe initialization.

* **[How to Extend Configuration](custom-config.md)**
    Understand the powerful pattern of extending PySetl's `Config` class
    alongside Pydantic's `BaseConfigModel` to create custom, strongly-typed, and
    validated configuration schemas tailored to your specific data sources and
    requirements.

## Data Access and Transformation

* **[How to Extend Connectors](custom-connectors.md)**
    Dive into the architecture of PySetl's `Connector` abstraction. This guide
    explains how to build custom connectors to integrate with new data storage
    systems or implement specialized data access patterns, maintaining PySetl's
    modularity and testability.

* **[How to Extend Factories (Custom `__init__`, Methods, etc.)](custom-factories.md)**
    Master the art of customizing your `Factory` implementations. Learn how to
    define custom initialization parameters, encapsulate complex logic in
    internal methods, and effectively use mixins to enhance your factories'
    capabilities, leading to highly reusable and readable ETL units.

## Pipeline Orchestration and Understanding

* **[Understanding the Dispatcher and Avoiding Resolution Conflicts](dispatcher.md)**
    Gain a deep understanding of PySetl's runtime dependency injection system.
    This section explains how the `Dispatcher` resolves `Delivery` requests with
    `Deliverable` outputs and provides crucial strategies involving producers,
    consumers, and delivery IDs to prevent common dependency resolution
    conflicts.

* **[Advanced Pipeline Operations](pipelines.md)**
    Explore powerful pipeline functionalities beyond basic stage addition:
    * **How to Set External Deliverables from the Pipeline**: Learn to
      seamlessly inject data from outside your PySetl pipeline, such as
      pre-loaded DataFrames or mock data, using `set_input_from_deliverable()`.
    * **How to Inspect Intermediate and Last Output from a Pipeline**: Discover
      how to retrieve results from any factory within your pipeline using
      `get_output()` and `get_last_output()`, invaluable for debugging and
      auditing.
    * **How to Diagram a Pipeline**: Utilize PySetl's built-in Mermaid diagram
      generation to visualize your pipeline's Directed Acyclic Graph (DAG),
      providing a clear overview of data flows and dependencies.

## Project Structure and Optimization

* **[Modularization: Building Scalable and Maintainable ETL Projects](modularization.md)**
    Delve into best practices for structuring large PySetl projects. This guide
    emphasizes the importance of granular factories, logical directory
    structures, and the strategic use of custom mixins to promote reusability,
    testability, and collaborative development.

* **[Performance Optimization](performance.md)**
    Learn how to identify and address performance bottlenecks in your PySetl
    pipelines:
    * **Benchmarking**: Understand PySetl's integrated benchmarking tools to
      measure execution times of factory phases.
    * **Interpreting Results**: Learn how to analyze benchmark reports to
      pinpoint areas for optimization.
    * **Caching Strategies**: Implement effective Spark caching techniques to
      reduce redundant computations and significantly improve pipeline
      efficiency.
