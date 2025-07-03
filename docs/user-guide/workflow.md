# Workflow System

The PySetl workflow system enables the construction, validation, and execution of complex, type-safe, and modular data pipelines using a Directed Acyclic Graph (DAG) of stages and factories.

---

## Architecture & Philosophy

- **Type Safety:** All data dependencies are declared and validated at runtime.
- **Modularity:** Pipelines are built from reusable factories and stages.
- **DAG Execution:** The workflow is represented as a DAG, ensuring correct data flow and dependency resolution.
- **Extensibility:** Easily add new computation units (factories) and stages.
- **Validation:** All dependencies are checked before execution, preventing runtime errors.

---

## Core Components

### Pipeline
- The top-level object representing a complete data transformation workflow.
- Manages stages, deliverables, execution, validation, and benchmarking.
- Uses an Inspector to build a DAG and validate dependencies.

### Stage
- A collection of independent factories (nodes) that are executed sequentially.
- Each stage can have multiple factories, and all factories in a stage are run before moving to the next stage.

### Factory
- The core unit of computation in a pipeline.
- Produces a Deliverable and can consume Deliveries from other factories or external sources.

### Deliverable & Delivery
- **Deliverable:** The output of a factory, registered with the Dispatcher.
- **Delivery:** A typed container for results from another factory or external source, used to declare dependencies.

### Dispatcher
- Manages the pool of deliverables and routes them to factories as needed.

### Inspector & DAG
- Inspector builds a DAG from the pipeline, stages, and factories.
- Validates that all dependencies are satisfied and visualizes the workflow.

### Node & Flow
- **Node:** Represents a factory in the DAG, with metadata about inputs and outputs.
- **Flow:** Represents the transfer of data between nodes.

---

## Usage Examples

### 1. Define Factories
Factories are the computation units. Each factory declares its inputs (Deliveries) and produces a Deliverable.

```python
from pysetl.workflow import Factory, Delivery, Deliverable

class Citizen(Schema):
    name: Column[StringType]
    age: Column[IntegerType]
    city: Column[StringType]

class CitizensFactory(Factory[DataSet[Citizen]]):
    def read(self) -> Self:
        self.citizens = create_partially_filled_dataset(
            spark,
            Citizen,
            [
                {Citizen.name: "citizen1", Citizen.age: 28, Citizen.city: "Los Angeles"},
                {Citizen.name: "citizen2", Citizen.age: 31, Citizen.city: "Mexico City"}
            ]
        )
        return self

    def process(self) -> Self:
        # Example transformation
        return self

    def write(self):
        # Optionally persist or output data
        return self

    def get(self) -> DataSet[Citizen]:
        return self.citizens

citizens: DataSet[Citizen] = CitizensFactory().read().process().write().get()
```

### 2. Organize Factories into Stages
```python
from pysetl.workflow import Stage

stage = (
    Stage()
    .add_factory_from_type(CitizensFactory)
    .add_factory(...)
)

stage.run()
```

### 3. Build and Run a Pipeline
```python
from pysetl.workflow import Pipeline

pipeline = Pipeline()
pipeline.add_stage(stage1)
pipeline.add_stage(stage2)
pipeline.run()
```

### 4. Access Outputs
```python
# Get the output of the last factory
output = pipeline.get_last_output()

# Get the output of a specific factory type
output = pipeline.get_output(CitizensFactory)
```

---

## Advanced Features

### Benchmarking
- Collects timing information for each stage/factory.
- Enable by passing `benchmark=True` to Pipeline or Stage.

### Diagrams
- Generates Mermaid diagrams for workflow visualization.
- Use `pipeline.inspector.graph.to_diagram()` to get a Mermaid class diagram.

### External Inputs
- Support for external data sources as pipeline inputs via `External` producer in Deliveries.

### Validation
- Ensures all dependencies are met before execution.
- Raises clear exceptions if any deliverable is missing or ambiguous.

### Dependency Injection

By default, a Factory will produce a `Deliverable[T]` and the Pipeline will register each available deliverable produced by the factories. You can take advantage of this deliverable pool with a `Delivery[T]` declaration inside your factory and the Pipeline dispatcher will try to solve the dependency by searching for a `Deliverable` of the same type. If ambiguity occurs you can pass a deliverable_id or explicitly state the expected producer class. Finally, you can register external deliverables into the Pipeline.

```python
from pysetl.workflow import Delivery, Deliverable

class City(Schema):
    city: Column[StringType]
    country: Column[StringType]

class CitizenCountry(Citizen):
    country: Column[StringType]

class CitiesFactory(Factory[DataSet[City]]):
    ...

class CitizenCountryFactory(Factory[DataSet[CitizenCountry]]):
    output: DataSet[CitizenCountry]
    citizens_delivery = Delivery[DataSet[Citizen]]()
    states_delivery = Delivery[DataSet[City]](producer=CitiesFactory)

    def read(self) -> Self:
        self.citizens = self.citizens_delivery.get()
        self.states = self.states_delivery.get()
        return self

    def process(self) -> Self:
        self.output = DataSet[CitizenCountry](self.citizens.join(self.states, "city"))
        return self

    def write(self) -> Self:
        return self

    def get(self) -> DataSet[CitizenCountry]:
        return self.output

deliverable = Deliverable[DataSet[City]](fake_cities)

pipeline = (
    Pipeline()
    .set_input_from_deliverable(deliverable)
    .add_stage(stage)
    .add_stage_from_type(CitizenCountryFactory)
    .run()
)
```

---

## Best Practices
- Declare all dependencies explicitly using Deliveries.
- Organize related factories into stages for clarity and modularity.
- Use benchmarking and diagrams to optimize and document your workflow.
- Leverage external inputs for flexible pipeline entry points.
- Extend Factory, Stage, and Pipeline for custom logic as needed.

For more details, see the [API Reference](../api/workflow_api.md).
