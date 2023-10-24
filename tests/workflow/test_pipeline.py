"""Test pysetl.workflow.pipeline module."""
from dataclasses import dataclass
from typing import TypeVar, Generic, Optional, ClassVar, Any
import pytest
from pydantic import BaseModel
from typedspark import DataSet, Schema, Column
from pyspark.sql import SparkSession, Row, functions as F
from pyspark.sql.types import StringType
from pysetl.utils import pretty
from pysetl.utils.exceptions import PipelineException, InvalidDeliveryException
from pysetl.workflow import Delivery, Factory, Pipeline, Stage, Deliverable


def test_pipeline_simple():
    """Test Pipeline basic functionalities."""
    deliverable = Deliverable[str]("id_of_product")
    factory1 = ProductFactory()
    factory2 = Product2Factory()
    factory3 = ContainerFactory()
    factory4 = Container2Factory()

    stage1 = Stage().add_factory(factory1).add_factory(factory2)
    stage2 = Stage().add_factory(factory3)
    stage3 = Stage().add_factory(factory4)

    pipeline = (
        Pipeline()
        .set_input_from_deliverable(deliverable)
        .add_stage(stage1)
        .add_stage(stage2)
        .add_stage(stage3)
        .run()
    )
    [found_node, *_] = list(filter(
        lambda _: pretty(_.factory_class) == "Container2Factory",
        pipeline.inspector.nodes
    ))
    result3 = factory3.get()

    assert pipeline.dispatcher.size
    assert (
        pipeline.get_deliverable(Container2[Product2])[0].payload ==
        Container2(Product2(x="a", y="b"))
    )
    assert len(pipeline.inspector.nodes) == 4
    assert len(pipeline.inspector.flows) == 4
    assert len(found_node.input) == 2
    assert result3 and result3.content.x == "id_of_product"
    assert pipeline.describe() == str(pipeline.inspector)
    assert pipeline.to_diagram() == pipeline.inspector.graph.to_diagram()

    with pytest.raises(NotImplementedError) as error:
        _ = pipeline.diagram_id

    assert str(error.value) == "Pipeline doesn't have a diagram id"


def test_pipline_builtins():
    """Pipeline should work with any built-in type."""
    deliverable = (
        Deliverable[str]("payload")
        .set_delivery_id("id")
        .set_consumers(set([FactoryWithInit]))
    )

    pipeline = (
        Pipeline(True)
        .set_input_from_deliverable(deliverable)
        .add_stage_from_type(
            FactoryWithInit,
            param_bool=True,
            param_byte=b"",
            param_string="",
            param_int=1,
            param_float=1.0,
            param_bool2=False,
            param_product2=Product2("a", "b")
        )
        .run()
    )

    last_output = pipeline.get_last_output()
    benchmarks = pipeline.get_benchmarks()

    assert len(benchmarks) == 1
    assert last_output.param_bool
    assert last_output.param_byte == b""
    assert last_output.param_string == ""
    assert last_output.param_int == 1
    assert last_output.param_float == 1.0
    assert not last_output.param_bool2
    assert last_output.param_product2 == Product2("a", "b")
    assert pipeline.get_output(FactoryWithInit) == last_output

    with pytest.raises(PipelineException) as error:
        pipeline.get_output(ProductFactory)

    assert str(error.value) == "There isn't any class ProductFactory"


def test_pipeline_single_factory():
    """Test benchmarks for pipeline."""
    factory = Product2Factory()
    stage = Stage().add_factory(factory)
    pipeline = Pipeline(True)

    [benchmark] = pipeline.execute_stage(stage)

    assert not pipeline.get_last_output()
    assert not pipeline.get_benchmarks()
    assert round(benchmark.read) == 0
    assert round(benchmark.process) == 0
    assert round(benchmark.write) == 0
    assert round(benchmark.total) == 0


def test_pipeline_no_solution():
    """Throw InvalidDeliveryException if no graph solution found."""
    deliverable = (
        Deliverable[str]("payload")
        .set_delivery_id("wront_id")
        .set_consumers(set([FactoryWithInit]))
    )
    factory = ProductFactory()
    pipeline = (
        Pipeline()
        .set_input_from_deliverable(deliverable)
        .add_stage_from_factory(factory)
    )

    with pytest.raises(InvalidDeliveryException) as error:
        pipeline.run()

    assert (
        str(error.value) ==
        "Deliverable of type str with deliveryId '' " +
        "produced by External is expected in ProductFactory."
    )


T = TypeVar("T")


def create_test_dataset(
        spark: SparkSession,
        schema: type[Schema],
        data: list[Any]) -> DataSet:
    """Help to create DataSets."""
    row_data = list(map(lambda _: Row(**_.__dict__), data))
    dataframe = spark.createDataFrame(
        row_data,
        schema.get_structtype()
    )
    return DataSet[schema](dataframe)  # type: ignore


@dataclass
class Container(Generic[T]):
    """Class for testing purposes."""

    content: T


@dataclass
class Container2(Generic[T]):
    """Class for testing purposes."""

    content: T


@dataclass
class Product:
    """Class for testing purposes."""

    x: str


@dataclass
class Product1:
    """Class for testing purposes."""

    x: str


@dataclass
class Product2:
    """Class for testing purposes."""

    x: str
    y: str


@dataclass
class Product23:
    """Class for testing purposes."""

    x: str


class Args(BaseModel):
    """Class for testing purposes."""

    param_bool: bool
    param_byte: bytes
    param_int: int
    param_float: float
    param_bool2: bool
    param_string: str
    param_product2: Product2
    param_input_string: str


@dataclass
class FactoryWithInit(Factory[Args]):
    """A Factory with init params."""

    param_bool: bool
    param_byte: bytes
    param_int: int
    param_float: float
    param_bool2: bool
    param_string: str
    param_product2: Product2
    output: Optional[Args] = None

    input_string_delivery: ClassVar[Delivery] = Delivery[str](delivery_id="id")

    def read(self):
        """Return itself."""
        self._input_string = self.input_string_delivery.get()

        return self

    def process(self):
        """Instanciate output."""
        self.output = Args(
            param_bool=self.param_bool,
            param_byte=self.param_byte,
            param_int=self.param_int,
            param_float=self.param_float,
            param_bool2=self.param_bool2,
            param_string=self.param_string,
            param_product2=self.param_product2,
            param_input_string=self._input_string
        )

        return self

    def write(self):
        """Return itself."""
        return self

    def get(self):
        """Return output."""
        return self.output


class ProductFactory(Factory[Product1]):
    """Class for testing purposes."""

    id_delivery: ClassVar[Delivery[str]] = Delivery[str]()
    id: str
    output: Optional[Product1] = None

    def read(self):
        """Test pipeline."""
        self.id = self.id_delivery.get()
        return self

    def process(self):
        """Test pipeline."""
        self.output = Product1(self.id)

        return self

    def write(self):
        """Test pipeline."""
        return self

    def get(self):
        """Test pipeline."""
        return self.output


class ProductFactoryBis(Factory[Product1]):
    """Class for testing purposes."""

    id_delivery: ClassVar[Delivery[str]] = Delivery[str]()
    id: str
    output: Optional[Product1] = None
    # consumers = set([DataSetFactoryBis])

    def read(self):
        """Test pipeline."""
        return self

    def process(self):
        """Test pipeline."""
        self.output = Product1(self.id)

        return self

    def write(self):
        """Test pipeline."""
        return self

    def get(self):
        """Test pipeline."""
        return self.output


class Product2Factory(Factory[Product2]):
    """Class for testing purposes."""

    output: Optional[Product2] = None

    def read(self):
        """Test pipeline."""
        return self

    def process(self):
        """Test pipeline."""
        self.output = Product2("a", "b")

        return self

    def write(self):
        """Test pipeline."""
        return self

    def get(self):
        """Test pipeline."""
        return self.output


class ContainerFactory(Factory[Container[Product1]]):
    """Class for testing purposes."""

    product1_delivery: ClassVar[Delivery[Product1]] = Delivery[Product1]()
    product1: Product1
    output: Optional[Container[Product1]] = None

    def read(self):
        """Test pipeline."""
        self.product1 = self.product1_delivery.get()

        return self

    def process(self):
        """Test pipeline."""
        self.output = Container(self.product1)

        return self

    def write(self):
        """Test pipeline."""
        return self

    def get(self):
        """Test pipeline."""
        return self.output


class Container2Factory(Factory[Container2[Product2]]):
    """Class for testing purposes."""

    p1_delivery: Delivery[Product1] = Delivery[Product1]()
    p2_delivery: Delivery[Product2] = Delivery[Product2]()
    p1: Product1
    p2: Product2
    output: Optional[Container2[Product2]] = None

    def read(self):
        """Test pipeline."""
        self.p1 = self.p1_delivery.get()
        self.p2 = self.p2_delivery.get()

        return self

    def process(self):
        """Test pipeline."""
        self.output = Container2(self.p2)

        return self

    def write(self):
        """Test pipeline."""
        return self

    def get(self):
        """Test pipeline."""
        return self.output


class Product1Schema(Schema):
    """Class for testing purposes."""

    x: Column[StringType]


class Product2Schema(Schema):
    """Class for testing purposes."""

    x: Column[StringType]


class DatasetFactory(Factory[DataSet[Product1Schema]]):
    """Class for testing purposes."""

    p1_delivery = Delivery[Product1]()

    p1: Product1
    output: Optional[DataSet[Product1Schema]] = None

    def __init__(self, spark):
        super().__init__()
        self.spark = spark

    def read(self):
        """Test pipeline."""
        self.p1 = self.p1_delivery.get()

        return self

    def process(self):
        """Test pipeline."""
        self.output = create_test_dataset(
            self.spark,
            Product1Schema,
            [self.p1, Product1("pd1")]
        )

        return self

    def write(self):
        """Test pipeline."""
        return self

    def get(self):
        """Test pipeline."""
        return self.output


class DatasetFactoryBis(Factory[DataSet[Product1Schema]]):
    """Class for testing purposes."""

    def __init__(self, spark: SparkSession):
        super().__init__()
        self.spark = spark

    p1_delivery = Delivery[Product1](producer=ProductFactoryBis)
    p1: Product1
    output: Optional[DataSet[Product1Schema]] = None

    def read(self):
        """Test pipeline."""
        self.p1 = self.p1_delivery.get()

    def process(self):
        """Test pipeline."""
        self.output = create_test_dataset(
            self.spark,
            Product1Schema,
            [self.p1, Product1("pd1")]
        )

        return self

    def write(self):
        """Test pipeline."""
        return self

    def get(self):
        """Test pipeline."""
        return self.output


class DatasetFactoryBis2(Factory[DataSet[Product1Schema]]):
    """Class for testing purposes."""

    p1_delivery = Delivery[Product1]()
    p1: Product1
    output: Optional[DataSet[Product1Schema]] = None

    def __init__(self, spark: SparkSession):
        super().__init__()
        self.spark = spark

    def read(self):
        """Test pipeline."""
        self.p1 = self.p1_delivery.get()
        return self

    def process(self):
        """Test pipeline."""
        self.output = create_test_dataset(
            self.spark,
            Product1Schema,
            [self.p1, Product1("pd1")]
        )

        return self

    def write(self):
        """Test pipeline."""
        return self

    def get(self):
        """Test pipeline."""
        return self.output


class DatasetFactory2(Factory[DataSet[Product2Schema]]):
    """Class for testing purposes."""

    p1_delivery = Delivery[Product1]()
    ds_delivery = Delivery[DataSet[Product1Schema]]()
    ds2_delivery = Delivery[DataSet[Product2Schema]]()

    p1: Product1
    ds: DataSet[Product1Schema]
    ds2: DataSet[Product2Schema]

    output: Optional[DataSet[Product2Schema]] = None

    def __init__(self, spark: SparkSession):
        """Test pipeline."""
        super().__init__()
        self.spark = spark

    def read(self):
        """Test pipeline."""
        self.p1 = self.p1_delivery.get()
        self.ds = self.ds_delivery.get()
        self.ds2 = self.ds2_delivery.get()

        return self

    def process(self):
        """Test pipeline."""
        first = (
            self.ds
            .withColumn("y", F.lit("produced_by_datasetFactory2"))
            .collect()[1]
        )
        second = self.ds2.collect()[-1]

        self.output = create_test_dataset(
            self.spark,
            Product2Schema,
            [first, second]
        )

        return self

    def write(self):
        """Test pipeline."""
        return self

    def get(self):
        """Test pipeline."""
        return self.output


class DatasetFactory3(Factory[DataSet[Product2Schema]]):
    """Class for testing purposes."""

    p1_delivery = Delivery[Product1]()
    ds_delivery = Delivery[DataSet[Product2Schema]](producer=DatasetFactory2)
    ds2_delivery = Delivery[DataSet[Product2Schema]]()

    p1: Product1
    ds: DataSet[Product2Schema]
    ds2: DataSet[Product2Schema]

    output: Optional[DataSet[Product2Schema]] = None

    def __init__(self, spark: SparkSession):
        super().__init__()
        self.spark = spark

    def read(self):
        """Test pipeline."""
        self.p1 = self.p1_delivery.get()
        self.ds = self.ds_delivery.get()
        self.ds2 = self.ds2_delivery.get()

        return self

    def process(self):
        """Test pipeline."""
        self.output = DataSet[Product2Schema](self.ds.union(self.ds2))

        return self

    def write(self):
        """Test pipeline."""
        return self

    def get(self):
        """Test pipeline."""
        return self


class DatasetFactory4(Factory[int]):
    """Class for testing purposes."""

    ds1_delivery = Delivery[DataSet[Product1Schema]]()
    ds1: DataSet[Product1Schema]

    def __init__(self, spark):
        super().__init__()
        self.spark = spark

    def read(self):
        """Test pipeline."""
        self.ds1 = self.ds1_delivery.get()

        return self

    def process(self):
        """Test pipeline."""
        return self

    def write(self):
        """Test pipeline."""
        return self

    def get(self):
        """Test pipeline."""
        return self.ds1.count()
