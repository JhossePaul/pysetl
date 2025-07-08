# High Priority

*Items that significantly impact user experience, performance, core functionality, or maintainability.*

## Parallel Execution of Factories Within Stages

**Status:** Investigation Required
**Module:** `src/pysetl/workflow/stage.py`

**Current Behavior:**
- All factories within a stage execute sequentially
- Stages execute sequentially within the pipeline

**Desired Enhancement:**
- Allow parallel execution of independent factories within the same stage
- Maintain dependency resolution and data flow integrity

**Technical Challenges:**

- [ ] Investigate Python threading capabilities with PySpark
- [ ] Research PySpark's thread safety and session management
- [ ] Understand SparkSession behavior across multiple threads
- [ ] Evaluate performance implications of parallel vs sequential execution

**Investigation Areas:**

1. **Python Threading:**
    - ThreadPoolExecutor for factory execution
    - AsyncIO for I/O-bound operations
    - Multiprocessing for CPU-bound operations

2. **PySpark Considerations:**
    - SparkSession thread safety
    - DataFrame operations across threads
    - Memory and resource management
    - Job scheduling and resource allocation

3. **Dependency Management:**
    - Ensuring factories with dependencies wait for their inputs
    - Managing shared resources and state
    - Error handling and rollback mechanisms

**Potential Implementation Approaches:**

1. **Thread-based parallelism:** Use ThreadPoolExecutor for independent factories
2. **Async execution:** Implement async/await pattern for I/O operations
3. **Process-based parallelism:** Use multiprocessing for CPU-intensive operations
4. **Hybrid approach:** Combine different parallelism strategies based on factory type

**References:**
- [PySpark Threading Best Practices](https://spark.apache.org/docs/latest/rdd-programming-guide.html#parallelized-collections)
- [Python Threading Documentation](https://docs.python.org/3/library/threading.html)
- [SETL Framework Scala Implementation](https://github.com/SETL-Framework/setl/blob/master/src/main/scala/io/github/setl/workflow/Stage.scala)

## Comprehensive Pipeline Optimizers

**Status:** Planning
**Module:** `src/pysetl/workflow/optimizer/`

**Current Issue:**
- No automatic optimization of pipeline execution
- Suboptimal resource utilization and performance
- Manual optimization requires deep expertise

**Vision:**
Create an intelligent pipeline optimization system that analyzes DAGs and automatically applies performance improvements, going beyond the basic dependency optimization in the [SETL SimplePipelineOptimizer](https://github.com/SETL-Framework/setl/blob/master/src/main/scala/io/github/setl/workflow/SimplePipelineOptimizer.scala). This includes advanced caching strategies, lazy evaluation, and resource management.

**Enhancements Needed:**

- [ ] Dependency analysis and deduplication
- [ ] Execution path optimization
- [ ] Resource utilization optimization (memory, Spark partitions)
- [ ] Cost-based optimization strategies
- [ ] Performance prediction and monitoring
- [ ] Automatic optimization recommendations
- [ ] **Intelligent caching of intermediate results (e.g., based on reusability, cost, TTL)**
- [ ] **Lazy evaluation of factory dependencies to defer expensive computations**
- [ ] **Resource pooling and reuse (e.g., connection pooling for external systems)**
- [ ] **Algorithm optimization for common operations (e.g., joins, aggregations)**
- [ ] **Spark-specific optimizations (e.g., broadcast joins, shuffle tuning)**

**Implementation Approaches:**

1. **Dependency Analysis & Deduplication:**
    - Detect identical data transformations across factories
    - Merge redundant operations
    - Create shared intermediate results
    - Identify common subgraphs

2. **Execution Path Optimization:**
    - Reorder stages for better resource utilization
    - Parallel execution of independent branches
    - Smart caching of expensive computations
    - Dynamic execution plan adjustment

3. **Resource Optimization:**
    - Memory usage optimization
    - Spark partition optimization
    - Connection pooling and reuse
    - Resource allocation strategies

4. **Cost-Based Optimization:**
    - Estimate execution costs for different strategies
    - Choose optimal execution plans
    - Performance prediction and monitoring
    - A/B testing of optimization strategies

**Technical Investigation Areas:**

- [ ] Graph theory algorithms for DAG optimization
- [ ] Machine learning for performance prediction
- [ ] Cost modeling for different execution strategies
- [ ] Integration with existing DAG and Inspector systems
- [ ] Real-time optimization feedback loops

## Pandera Integration for Enhanced Data Validation

**Status:** Planning
**Module:** `src/pysetl/validation/`

**Current Issue:**
- Limited data validation capabilities beyond basic type checking with TypedSpark
- No comprehensive validation for data quality, business rules, or cross-engine compatibility
- Missing advanced features like schema inference, error reporting, and API integration

**Vision:**
Integrate [Pandera](https://pandera.readthedocs.io/en/stable/) as a
comprehensive data validation framework to replace and enhance the current
TypedSpark-based validation, providing production-ready data quality assurance
across multiple DataFrame engines.

### **Why Pandera Integration is Critical:**

#### **Current Limitations:**

- **TypedSpark**: Basic type safety only, limited to Spark DataFrames
- **No Business Rule Validation**: Cannot validate ranges, patterns, or custom logic
- **Single Engine**: Limited to Spark ecosystem
- **Poor Error Reporting**: Basic type errors without detailed context
- **No Schema Evolution**: Cannot handle schema changes or versioning

#### **Pandera's Capabilities:**

- ✅ **Multi-Engine Support**: pandas, polars, pyspark, modin, dask, geopandas
- ✅ **Rich Validation**: Range checks, regex patterns, custom functions, hypothesis testing
- ✅ **Schema Inference**: Automatic schema generation from existing data
- ✅ ✅ **Schema Persistence**: Save and version schemas for reproducibility
- ✅ **Lazy Validation**: Aggregate errors for comprehensive reporting
- ✅ **FastAPI Integration**: Perfect for web service validation
- ✅ **Data Synthesis**: Generate test data for validation scenarios
- ✅ **Production Ready**: Battle-tested in enterprise environments

### **Proposed Integration Architecture:**

#### **Core Components:**

```python
from pysetl.validation import PanderaValidator, ValidationConfig
import pandera as pa

# Define comprehensive schemas
class CitizenSchema(pa.DataFrameModel):
    name: pa.Field[str] = pa.Field(str_length=pa.Check.greater_than(0))
    age: pa.Field[int] = pa.Field(ge=0, le=150)
    city: pa.Field[str] = pa.Field(str_length=pa.Check.greater_than(0))
    email: pa.Field[str] = pa.Field(regex=r'^[^@]+@[^@]+\.[^@]+$')

# Enhanced Factory with Pandera validation
class CitizensFactory(Factory[DataSet[Citizen]]):
    def __init__(self):
        self.validator = PanderaValidator(CitizenSchema)
        self.validation_config = ValidationConfig(
            lazy=True,  # Aggregate errors
            drop_invalid_rows=False,  # Fail fast
            error_report_format="detailed"
        )

    def read(self):
        self.citizens = create_partially_filled_dataset(spark, Citizen, [...])
        # Validate with comprehensive Pandera checks
        validation_result = self.validator.validate(
            self.citizens,
            config=self.validation_config
        )
        if not validation_result.is_valid:
            raise ValidationError(validation_result.error_report)
        return self
```

#### **Validation Lifecycle Integration:**

1.  **Schema Definition**: Declarative schema models with business rules
2.  **Pre-Processing**: Validate input data before transformation
3.  **Post-Processing**: Validate output data after transformation
4.  **Error Handling**: Comprehensive error reporting and recovery
5.  **Schema Evolution**: Handle schema changes and versioning

### **Implementation Phases:**

#### **Phase 1: Core Integration (High Priority)**

  - [ ] Create `pysetl.validation` module with Pandera integration
  - [ ] Implement `PanderaValidator` wrapper class
  - [ ] Add validation hooks to Factory lifecycle (read, process, write)
  - [ ] Support for pandas, polars, and pyspark backends
  - [ ] Basic error reporting and exception handling
  - [ ] Integration with existing Pipeline error handling

#### **Phase 2: Enhanced Features (Medium Priority)**

  - [ ] Schema inference from existing data structures
  - [ ] Schema persistence and versioning system
  - [ ] Lazy validation with error aggregation
  - [ ] Custom validation check registration
  - [ ] Performance optimization for large datasets
  - [ ] Integration with Pipeline monitoring and logging

#### **Phase 3: Advanced Features (Low Priority)**

  - [ ] FastAPI integration for web service validation
  - [ ] Hypothesis testing for data quality assurance
  - [ ] Data synthesis strategies for testing
  - [ ] Schema migration and evolution tools
  - [ ] Real-time validation for streaming data
  - [ ] Integration with external validation services

### **Technical Investigation Areas:**

#### **Backend Compatibility:**

  - [ ] **Pandas Backend**: Full feature support, optimal performance
  - [ ] **Polars Backend**: High-performance validation for large datasets
  - [ ] **PySpark Backend**: Distributed validation for big data
  - [ ] **Modin Backend**: Parallel processing for medium datasets
  - [ ] **Dask Backend**: Distributed validation for very large datasets

#### **Performance Considerations:**

  - [ ] Validation overhead analysis for different dataset sizes
  - [ ] Lazy vs. eager validation performance trade-offs
  - [ ] Caching strategies for repeated validations
  - [ ] Parallel validation for independent columns
  - [ ] Memory usage optimization for large schemas

#### **Integration Points:**

  - [ ] **Factory Integration**: Pre/post processing validation hooks
  - [ ] **Pipeline Integration**: Validation at stage boundaries
  - [ ] **Error Handling**: Integration with PySetl's error recovery
  - [ ] **Monitoring**: Validation metrics and performance tracking
  - [ ] **Configuration**: Environment-specific validation rules

### **Migration Strategy:**

#### **Backward Compatibility:**

  - [ ] Maintain TypedSpark integration for existing code
  - [ ] Gradual migration path from TypedSpark to Pandera
  - [ ] Hybrid validation support during transition
  - [ ] Deprecation warnings for TypedSpark usage
  - [ ] Migration guide and examples

#### **Schema Migration:**

  - [ ] Automatic conversion from TypedSpark schemas to Pandera
  - [ ] Schema versioning and compatibility checking
  - [ ] Incremental schema updates and validation
  - [ ] Rollback capabilities for schema changes

### **Success Metrics:**

#### **Functionality:**

  - [ ] 100% feature parity with Pandera's core capabilities
  - [ ] Seamless integration with existing PySetl workflows
  - [ ] Comprehensive error reporting and debugging
  - [ ] Multi-engine validation support

#### **Performance:**

  - [ ] \<10% overhead for validation on typical datasets
  - [ ] Scalable validation for datasets \>1GB
  - [ ] Efficient memory usage for large schemas
  - [ ] Fast validation for real-time processing

#### **Developer Experience:**

  - [ ] Intuitive API for schema definition
  - [ ] Clear error messages and debugging information
  - [ ] Comprehensive documentation and examples
  - [ ] Easy migration from existing validation approaches

### **Dependencies and Requirements:**

#### **New Dependencies:**

  - [ ] `pandera>=0.24.0` for core validation functionality
  - [ ] `pydantic>=2.0` for schema model support (already present)
  - [ ] `hypothesis` for data synthesis and testing
  - [ ] `fastapi` for web service integration (optional)

#### **Configuration Updates:**

  - [ ] Update `pyproject.toml` with new dependencies
  - [ ] Add validation-specific environment variables
  - [ ] Update CI/CD for validation testing
  - [ ] Add validation examples to documentation

### **Risk Assessment:**

#### **Technical Risks:**

  - **Performance Impact**: Validation overhead on large datasets
  - **Complexity**: Learning curve for Pandera's advanced features
  - **Compatibility**: Integration issues with existing PySetl components
  - **Maintenance**: Keeping up with Pandera's rapid development

#### **Mitigation Strategies:**

  - **Performance Testing**: Comprehensive benchmarking before integration
  - **Gradual Rollout**: Phased implementation with fallback options
  - **Documentation**: Extensive guides and examples for adoption
  - **Community Feedback**: Early testing with PySetl users

### **Alternative Approaches:**

#### **Option 1: Full Pandera Integration**

  - **Pros**: Maximum feature set, production-ready, active community
  - **Cons**: Additional dependency, potential performance overhead
  - **Recommendation**: Primary approach

#### **Option 2: Lightweight Pandera Wrapper**

  - **Pros**: Minimal overhead, focused on core features
  - **Cons**: Limited functionality, maintenance burden
  - **Recommendation**: Fallback if full integration proves too heavy

#### **Option 3: Custom Validation Framework**

  - **Pros**: Tailored to PySetl's specific needs
  - **Cons**: Development overhead, limited community support
  - **Recommendation**: Not recommended given Pandera's maturity

**Assessment:** Pandera integration would significantly enhance PySetl's data
\*validation capabilities, making it more attractive for production use cases and
\*aligning with modern data quality practices. The integration is technically
\*feasible and would provide substantial value to users.

## Examples and Tutorials

**Status:** Planning
**Module:** Documentation

**Current Issue:**

  - Limited examples and tutorials for common use cases
  - Steep learning curve for new users
  - No comprehensive getting started guide

**Enhancements Needed:**

  - [ ] Comprehensive examples for common ETL patterns
  - [ ] Step-by-step tutorials for beginners
  - [ ] Best practices examples
  - [ ] Integration examples with common data sources
  - [ ] Performance optimization examples

**Implementation Areas:**

1.  **Getting Started:**

      - Quick start guide
      - Basic ETL examples
      - Common use case tutorials

2.  **Advanced Examples:**

      - Complex workflow patterns
      - Performance optimization examples
      - Integration with external systems

3.  **Best Practices:**

      - Code organization examples
      - Testing strategies
      - Deployment patterns

## Testing Coverage

**Status:** Planning
**Module:** Testing

**Areas for Improvement:**

  - [ ] Integration tests for complex workflows
  - [ ] Performance benchmarks (automated regression testing)
  - [ ] Error scenario testing
  - [ ] Cross-version compatibility testing
  - [ ] Mock factories for testing
  - [ ] Pipeline validation tests

## Full Connector Support for Database and Data Lake Storages

**Status:** Planning
**Module:** `src/pysetl/storage/connector/`, `src/pysetl/storage/repository/`

**Current Issue:**

  - `DbStorage` enum defines various database/data lake types, but full connector implementations are missing.
  - Users are limited to file-based storage or must implement custom connectors for common database/data lake sources.
  - High barrier to integrating PySetl with existing enterprise data landscapes.

**Vision:**
Provide robust, performant, and type-safe connector and repository implementations for key database and data lake storage options defined in `pysetl.enums.DbStorage`. This will significantly expand PySetl's applicability and ease of integration into diverse data ecosystems.

**Enhancements Needed:**

  - [ ] Implement `JdbcConnector` and `JdbcRepository` for generic JDBC support.
  - [ ] Implement `DeltaConnector` and `DeltaRepository` for Delta Lake.
  - [ ] Implement `CassandraConnector` and `CassandraRepository`.
  - [ ] Implement `DynamoDBConnector` and `DynamoDBRepository`.
  - [ ] Implement `HudiConnector` and `HudiRepository`.
  - [ ] Implement `SparkSqlConnector` for reading/writing to Spark SQL tables/views.
  - [ ] Implement `StructuredStreamingConnector` for streaming sources/sinks.
  - [ ] Comprehensive configuration options for each connector type.
  - [ ] Error handling specific to each database/data lake technology.

**Implementation Approaches:**

1.  **Leverage Spark's Built-in Connectors:** For JDBC, Delta, and possibly Cassandra/Hudi, Spark often has native or community-supported connectors that can be wrapped by PySetl's `Connector` interface.
2.  **External Libraries:** For others like DynamoDB, specific Python libraries (e.g., `boto3` for AWS) or PySpark packages might be needed.
3.  **Repository Pattern:** Ensure each connector has a corresponding `Repository` for schema-aware read/write operations.
4.  **Configuration:** Design flexible `Config` models for each connector type (e.g., `JdbcConfig`, `DeltaConfig`).

**Technical Investigation Areas:**

  - [ ] Specific Spark packages required for each storage type.
  - [ ] Performance characteristics and best practices for each connector.
  - [ ] Authentication and authorization mechanisms for different databases.
  - [ ] Schema evolution handling for data lake formats (Delta, Hudi).

**Success Criteria:**

  - [ ] Functional read/write operations for at least 3-4 key `DbStorage` types (e.g., JDBC, Delta, Spark SQL) in Phase 1.
  - [ ] Clear documentation and examples for each new connector.
  - [ ] Benchmarking to ensure performance is competitive with direct Spark usage.
  - [ ] Seamless integration with PySetl's `Factory` and `Pipeline` components.
