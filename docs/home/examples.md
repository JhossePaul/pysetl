# Examples

This guide showcases real-world PySetl usage patterns and provides working
examples you can run.

---

## Available Examples

### Structured ETL Example

The main example demonstrates a complete ETL pipeline with multiple data sources
and transformations.

**Location**: `examples/structured_etl/`

**What it does**:

- Loads citizen and city data
- Performs data transformations
- Joins datasets
- Outputs enriched data

**Key Features Demonstrated**:

- Multiple data sources
- Complex transformations
- Data joining
- Pipeline orchestration

---

## Running the Examples

### Prerequisites

1. **Install PySetl**: Follow the [Installation Guide](installation.md)
2. **Set up Spark**: Ensure PySpark is working in your environment
3. **Clone the repository**: If running from source

### Running the Structured ETL Example

```bash
# Navigate to the example directory
cd examples/structured_etl

# Run the example
python main.py
```

---

## Example Breakdown

### 1. Data Schemas

```python
from typedspark import Schema, column, StringType, IntegerType

class Citizen(Schema):
    name = column(StringType())
    age = column(IntegerType())
    city = column(StringType())

class City(Schema):
    city = column(StringType())
    country = column(StringType())
```

### 2. Factory Implementation

```python
from pysetl.workflow import Factory, Delivery
from typedspark import DataSet

class CitizensFactory(Factory[DataSet[Citizen]]):
    input_data: Delivery[DataSet[Citizen]] = Delivery()

    def read(self):
        # Load data from input delivery
        self.citizens = self.input_data.get()
        return self

    def process(self):
        # Transform data (filter, enrich, etc.)
        self.citizens = self.citizens.filter(self.citizens.age > 18)
        return self

    def write(self):
        # Save or pass data to next stage
        return self

    def get(self):
        return self.citizens
```

### 3. Pipeline Assembly

```python
from pysetl.workflow import Stage, Pipeline, Deliverable

# Create stages
stage1 = Stage().add_factory_from_type(CitizensFactory)
stage2 = Stage().add_factory_from_type(CitiesFactory)

# Build pipeline
pipeline = (
    Pipeline()
    .add_stage(stage1)
    .add_stage(stage2)
    .set_input_from_deliverable(citizens_deliverable)
    .set_input_from_deliverable(cities_deliverable)
)
```

### 4. Execution

```python
# Run the pipeline
result = pipeline.run()

# Get results
output = result.get_last_output()
print(output.show())
```

---

## Common Patterns

### Pattern 1: Simple ETL

```python
class SimpleETLFactory(Factory[DataSet[MySchema]]):
    def read(self):
        # Load data
        return self

    def process(self):
        # Transform data
        return self

    def write(self):
        # Save data
        return self

pipeline = Pipeline().add_stage_from_factory(SimpleETLFactory())
```

### Pattern 2: Multi-Stage Pipeline

```python
# Stage 1: Data Loading
stage1 = Stage().add_factory_from_type(DataLoaderFactory)

# Stage 2: Data Processing
stage2 = Stage().add_factory_from_type(DataProcessorFactory)

# Stage 3: Data Output
stage3 = Stage().add_factory_from_type(DataWriterFactory)

pipeline = Pipeline().add_stage(stage1).add_stage(stage2).add_stage(stage3)
```

### Pattern 3: Conditional Processing

```python
class ConditionalFactory(Factory[DataSet[MySchema]]):
    def process(self):
        if self.should_transform():
            self.data = self.transform_data()
        return self
```

---

## Best Practices

### 1. Schema Design

- Use TypedSpark schemas for type safety
- Keep schemas simple and focused
- Document schema fields

### 2. Factory Structure

- Separate read, process, and write logic
- Use meaningful variable names
- Handle errors gracefully

### 3. Pipeline Design

- Group related operations in stages
- Use descriptive stage names
- Test individual stages

### 4. Data Flow

- Plan your data dependencies
- Use deliveries for data passing
- Consider data partitioning

---

## Extending Examples

### Adding New Examples

1. Create a new directory in `examples/`
2. Include a `main.py` file
3. Add a `README.md` explaining the example
4. Update this documentation

### Example Structure

```
examples/
└── my_example/
    ├── main.py
    ├── README.md
    ├── data/
    └── config/
    └── factories/
```

---

## Getting Help

- 📖 [User Guide](../user-guide/configuration.md) - Detailed concepts
- 🔧 [API Reference](../api/pysetl.md) - Complete API documentation
- 🐛 [GitHub Issues](https://github.com/JhossePaul/pysetl/issues) - Report problems
- 💬 [Discussions](https://github.com/JhossePaul/pysetl/discussions) - Ask questions

---

## Next Steps

- 🚀 [QuickStart](index.md) - Run your first pipeline
- 📚 [User Guide](../user-guide/index.md) - Learn PySetl concepts
- 🔧 [Configuration](../user-guide/configuration.md) - Configure your environment
