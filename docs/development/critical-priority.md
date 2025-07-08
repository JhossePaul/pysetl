# Critical Priority

*Items that are blocking production readiness or causing significant issues.*

## Error Handling and Recovery

**Status:** Planning
**Module:** `src/pysetl/workflow/`

**Current Issue:**
- Limited error recovery mechanisms - if a factory fails, the entire pipeline stops
- Poor fault tolerance for production environments
- No retry mechanisms or graceful degradation

**Enhancements Needed:**

- [ ] Comprehensive error handling across all modules
- [ ] Retry mechanisms for transient failures
- [ ] Graceful degradation strategies
- [ ] Error reporting and logging improvements
- [ ] Partial pipeline execution capabilities
- [ ] Circuit breaker patterns for external dependencies
- [ ] **Improved error messages and debugging context (e.g., for `InvalidDeliveryException`)**

**Investigation Areas:**

1. **Retry Strategies:**
    - Exponential backoff for transient failures
    - Configurable retry counts and timeouts
    - Different retry policies for different failure types

2. **Graceful Degradation:**
    - Skip failed factories when possible
    - Provide fallback data sources
    - Maintain pipeline integrity with partial failures

3. **Error Classification:**
    - Distinguish between transient and permanent failures
    - Categorize errors by severity and impact
    - Implement appropriate handling strategies

**Implementation Approaches:**

1. **Factory-Level Error Handling:**
    - Wrap factory execution in try-catch blocks
    - Implement retry logic for transient failures
    - Provide fallback mechanisms for critical failures

2. **Pipeline-Level Error Handling:**
    - Continue execution with failed factories skipped
    - Implement circuit breakers for external dependencies
    - Provide detailed error reporting and logging

3. **User Experience:**
    - Clear error messages and recovery suggestions
    - Partial results when possible
    - Comprehensive error reporting and monitoring

**Success Criteria:**

- [ ] Pipeline continues execution even with factory failures
- [ ] Automatic retry for transient failures
- [ ] Clear error reporting and logging
- [ ] Graceful degradation for non-critical failures
- [ ] Production-ready error handling patterns

## Robust SparkSession Management

**Status:** Planning
**Module:** `src/pysetl/utils/mixins/has_spark_session.py`, `src/pysetl/context/`

**Current Issue:**
- Implicit SparkSession creation (`getOrCreate()`) might lead to inconsistent configurations or resource issues in complex environments.
- Lack of explicit lifecycle management (e.g., proper shutdown in all scenarios).
- Limited control over SparkSession configuration beyond initial setup.

**Enhancements Needed:**

- [ ] Centralized and explicit SparkSession lifecycle management (creation, configuration, shutdown).
- [ ] Ability to inject pre-configured SparkSession instances.
- [ ] More robust handling of SparkContext and SparkSession across PySetl components.
- [ ] Clearer integration with PySetl's configuration system for Spark properties.
- [ ] Ensure proper resource release upon pipeline completion or failure.

**Investigation Areas:**

1. **SparkSession Provisioning:**
    - Options for users to provide their own SparkSession.
    - Autoconfiguration based on PySetl's overall configuration.

2. **Thread Safety and Concurrency:**
    - How SparkSession behaves in multi-threaded environments (relevant for parallel execution).
    - Ensuring correct SparkContext access from different factories/threads.

3. **Resource Management:**
    - Graceful shutdown of SparkSession and associated resources.
    - Monitoring Spark resource utilization within PySetl.

**Implementation Approaches:**

1. **`PySetlContext` Enhancement:**
    - Make `PySetlContext` responsible for SparkSession creation and management.
    - Provide methods to get/set/stop the global SparkSession.

2. **Factory/Mixin Integration:**
    - Ensure `HasSparkSession` mixin correctly accesses the managed SparkSession.
    - Provide clear guidelines for factories needing SparkContext/SparkSession.

**Success Criteria:**

- [ ] Predictable and consistent SparkSession behavior across all pipeline runs.
- [ ] No unexpected resource leaks related to SparkSession.
- [ ] Users can easily configure and inject their desired SparkSession.
- [ ] Framework handles SparkSession lifecycle robustly.
