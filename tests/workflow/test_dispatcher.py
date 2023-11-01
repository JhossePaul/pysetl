"""Test pysetl.workflow.dispatcher module."""
from dataclasses import dataclass
from typing import Generic, TypeVar, Any
import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StringType
from typedspark import DataSet, Schema, Column
from pysetl.workflow.dispatcher import Dispatcher
from pysetl.workflow import Factory, Delivery, Deliverable, Stage
from pysetl.utils.exceptions import (
    AlreadyExistsException, InvalidDeliveryException
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


class ContainerSchema(Schema):
    """Schema for testing purposes."""

    content: Column[StringType]


@dataclass
class Container2(Generic[T]):
    """Class for testing purposes."""

    content: T


@dataclass
class Product:
    """Class for testing purposes."""

    x: str


class ProductSchema(Schema):
    """Class for testing purposes."""

    x: Column[StringType]


@dataclass
class Product1:
    """Class for testing purposes."""

    x: str


@dataclass
class Product2:
    """Class for testing purposes."""

    x: str
    y: str


class Product2Schema(Schema):
    """Schema for testing purposes."""

    x: Column[StringType]
    y: Column[StringType]


@dataclass
class Product23:
    """Class for testing purposes."""

    x: str


class Product23Schema(Schema):
    """Schema for testing purposes."""

    x: Column[StringType]


class MultipleInputFactory(Factory[list[str]]):
    """Factory for testing purposes."""

    array1 = Delivery[list[str]](delivery_id="first")
    array2 = Delivery[list[str]](delivery_id="second")
    array3 = Delivery[list[str]]()

    def read(self):
        """Return itself."""
        return self

    def process(self):
        """Return itself."""
        return self

    def write(self):
        """Return itself."""
        return self

    def get(self):
        """Return output."""
        x = self.array1.get()
        y = self.array2.get()
        z = self.array3.get()

        return x + y + z


class MyFactory(Factory[Container[Product23]]):
    """Factory for testing purposes."""

    input_delivery = Delivery[Container2[Product]]()
    output_delivery = Delivery[Container[Product23]]()

    def __init__(self):
        super().__init__()
        self.input = None
        self.output = None

    def read(self):
        """Set deliveries."""
        self.input = self.input_delivery.get()
        self.output = self.output_delivery.get()

        return self

    def process(self):
        """Return itself."""
        return self

    def write(self):
        """Return itself."""
        return self

    def get(self):
        """Return ouput."""
        return self.output


class MyFactory2(Factory[DataSet[Product23Schema]]):
    """Class for testing purpoese."""

    input_delivery = Delivery[DataSet[ProductSchema]]()
    output_delivery = Delivery[DataSet[Product23Schema]]()

    def __init__(self):
        super().__init__()
        self.input = None
        self.output = None

    def read(self):
        """Set deliveries."""
        self.input = self.input_delivery.get()
        self.output = self.output_delivery.get()

    def process(self):
        """Return itself."""
        return self

    def write(self):
        """Return itself."""
        return self

    def get(self):
        """Return ouput."""
        return self.output


class StringFactory(Factory[str]):
    """Class for testing purposes."""

    def read(self):
        """Return itself."""
        return self

    def process(self):
        """Return itself."""
        return self

    def write(self):
        """Return itself."""
        return self

    def get(self):
        """Return itself."""
        return "factory 1"


class MultipleStringInputsFactory(Factory[str]):
    """Class for testing purposes."""

    input1_delivery = Delivery[str](producer=StringFactory)
    input2_delivery = Delivery[str](producer=StringFactory)

    def read(self):
        """Return itself."""
        return self

    def process(self):
        """Return itself."""
        return self

    def write(self):
        """Return itself."""
        return self

    def get(self):
        """Return output."""
        return "factory 2"


class BuiltinsFactory(Factory[str]):
    """Test built-ins types deliveries."""

    int_delivery = Delivery[int]()
    float_delivery = Delivery[float]()
    complex_delivery = Delivery[complex]()
    bool_delivery = Delivery[bool]()
    str_delivery = Delivery[str]()
    bytes_delivery = Delivery[bytes]()
    list_delivery = Delivery[list[int]]()

    def read(self):
        """Return itself."""
        return self

    def process(self):
        """Return itself."""
        return self

    def write(self):
        """Return itself."""
        return self

    def get(self):
        """Return output."""
        return "factory 2"


cont_prod = Container[Product](Product("1"))
cont2_prod = Container2[Product](Product("1"))
cont_prod2 = Container[Product23](Product23("1"))
cont2_prod2 = Container2[Product23](Product23("1"))


def test_dispatcher():
    """Test Dispatcher class."""
    factory = MyFactory()
    dispatcher = (
        Dispatcher()
        .add_deliverable(Deliverable[Container[Product]](cont_prod))
        .add_deliverable(Deliverable[Container2[Product]](cont2_prod))
        .add_deliverable(Deliverable[Container[Product23]](cont_prod2))
        .add_deliverable(Deliverable[Container2[Product23]](cont2_prod2))
    )
    dispatcher.dispatch(factory)

    assert len(dispatcher.find_deliverable_by_type(Container[Product])) == 1
    assert len(dispatcher.find_deliverable_by_name("Container[Product]")) == 1
    assert factory.input_delivery.get() == cont2_prod
    assert factory.output_delivery.get() == cont_prod2

    stage = Stage().add_factory(factory).run()
    dispatcher2 = Dispatcher()

    assert not dispatcher2.find_deliverable_by_type(Container[Product23])

    dispatcher2.collect_stage_deliverables(stage)
    assert dispatcher2.find_deliverable_by_type(Container[Product23])


def test_dispatcher_dataset():
    """Test with DataSet."""
    spark = SparkSession.builder.getOrCreate()

    dataset_products = create_test_dataset(
        spark,
        ProductSchema,
        [Product("a"), Product("b")]
    )

    dataset_product2 = create_test_dataset(
        spark,
        Product23Schema,
        [Product23("a2"), Product23("b2")]
    )

    dataset_container = create_test_dataset(
        spark,
        ContainerSchema,
        [Product23("a2"), Product23("b2")]
    )

    factory = MyFactory2()
    _ = (
        Dispatcher()
        .add_deliverable(
            Deliverable[DataSet[ProductSchema]](dataset_products)
        )
        .add_deliverable(
            Deliverable[DataSet[Product23Schema]](dataset_product2)
        )
        .add_deliverable(
            Deliverable[DataSet[ContainerSchema]](dataset_container)
        )
        .dispatch(factory)
    )

    assert factory.input_delivery.get().count() == 2
    assert factory.input_delivery.get().filter("x = 'a'").count() == 1
    assert factory.input_delivery.get().filter("x = 'a2'").count() == 0
    assert factory.output_delivery.get().count() == 2
    assert factory.output_delivery.get().filter("x = 'a2'").count() == 1
    assert factory.output_delivery.get().filter("x = 'b2'").count() == 1
    assert factory.output_delivery.get().filter("x = 'a'").count() == 0


def test_dispatcher_same_deliverable_exception():
    """Dispatcher raise AlreadyExistsException if deliverable passed twice."""
    str_del = Deliverable[str]("String")

    dispatcher = Dispatcher().add_deliverable(str_del)

    with pytest.raises(AlreadyExistsException) as error:
        dispatcher.add_deliverable(str_del)

    assert "type Deliverable already exists" in str(error.value)


def test_dispatcher_multiple_matches():
    """Dispatcher InvalidDeliveryException when multiple matches."""
    deliverable = (
        Deliverable[str]("String")
        .set_producer(StringFactory)
        .set_consumers(set([MultipleStringInputsFactory]))
    )
    deliverable2 = (
        Deliverable[str]("String")
        .set_producer(StringFactory)
        .set_consumers(set([MultipleStringInputsFactory]))
    )
    factory = MultipleStringInputsFactory()
    dispatcher = (
        Dispatcher()
        .add_deliverable(deliverable)
        .add_deliverable(deliverable2)
    )

    with pytest.raises(InvalidDeliveryException) as error:
        dispatcher.dispatch(factory)

    assert (
        "Find multiple deliveries having the sametype, producer and consumer"
        == str(error.value)
    )


def test_dispatcher_no_matched_producer():
    """Dispatch throws NoDeliveryException if no producer matches."""
    deliverable = (
        Deliverable[str]("String")
        .set_producer(MultipleStringInputsFactory)
        .set_consumers(set([MultipleStringInputsFactory]))
    )
    factory = MultipleInputFactory()
    dispatcher = Dispatcher().add_deliverable(deliverable)

    with pytest.raises(InvalidDeliveryException) as error:
        dispatcher.dispatch(factory)

    assert str(error.value) == "Cannot find type list[str]"


def test_dispatcher_builtins():
    """Test built-ins types."""
    factory = BuiltinsFactory()
    dispatcher = (
        Dispatcher()
        .add_deliverable(Deliverable[int](1))
        .add_deliverable(Deliverable[float](1.0))
        .add_deliverable(Deliverable[complex](complex(1, 2)))
        .add_deliverable(Deliverable[bool](True))
        .add_deliverable(Deliverable[str]("str"))
        .add_deliverable(Deliverable[bytes](b"str"))
        .add_deliverable(Deliverable[list[int]]([1]))
    )
    dispatcher.dispatch(factory)

    assert factory.int_delivery.get() == 1
    assert factory.float_delivery.get() == 1.0
    assert factory.complex_delivery.get() == complex(1, 2)
    assert factory.bool_delivery.get()
    assert factory.str_delivery.get() == "str"
    assert factory.bytes_delivery.get() == b"str"
    assert factory.list_delivery.get() == [1]


def test_dispatcher_with_delivery_id():
    """
    Test Dispatcher with delivery id.

    Dispatcher should handle delivery ID with multiple deliveries of the
    same type.
    """
    array1 = Deliverable[list[str]](["a", "b"],).set_delivery_id("first")
    array2 = Deliverable[list[str]](["1", "2"]).set_delivery_id("second")
    array3 = Deliverable[list[str]](["x", "y"])
    array4 = Deliverable[list[str]](["m", "n"]).set_delivery_id("fourth")

    factory = MultipleInputFactory()
    dispatcher = (
        Dispatcher()
        .add_deliverable(array1)
        .add_deliverable(array2)
        .add_deliverable(array3)
        .add_deliverable(array4)
        .dispatch(factory)
    )

    output = factory.read().process().write().get()
    dispatcher.collect_factory_deliverable(factory)

    [deliverable_from_dispatcher] = dispatcher.find_deliverable_by(
        lambda _: _.payload_type == list[str] and
        _.producer == MultipleInputFactory
    )

    assert output == ["a", "b", "1", "2", "x", "y"]
    assert deliverable_from_dispatcher.payload == output


def test_dispatcher_delivery_id_exception():
    """Dispatcher should throw exception when ID not matching."""
    array1 = Deliverable[list[str]](["a", "b"],).set_delivery_id("first")
    array2 = Deliverable[list[str]](["1", "2"]).set_delivery_id("second")
    array3 = Deliverable[list[str]](["x", "y"])
    array4 = Deliverable[list[str]](["m", "n"])

    factory = MultipleInputFactory()
    dispatcher = (
        Dispatcher()
        .add_deliverable(array1)
        .add_deliverable(array2)
        .add_deliverable(array3)
        .add_deliverable(array4)
    )

    with pytest.raises(InvalidDeliveryException) as error:
        dispatcher.dispatch(factory)

    assert str(error.value) == "Cannot find type list[str]"


def test_dispatcher_consumer_not_in_deliverable_list():
    """
    Test if consumer not in deliverable consumer list.

    Dispatcher throws InvalidDeliveryException if consumer not in deliverable
    property.
    """
    array1 = Deliverable[list[str]](["a", "b"],).set_delivery_id("first")
    array2 = Deliverable[list[str]](["1", "2"]).set_delivery_id("second")
    array3 = Deliverable[list[str]](["x", "y"], consumers=set([MyFactory]))
    factory = MultipleInputFactory()
    dispatcher = (
        Dispatcher()
        .add_deliverable(array1)
        .add_deliverable(array2)
        .add_deliverable(array3)
    )

    with pytest.raises(InvalidDeliveryException) as error:
        dispatcher.dispatch(factory)

    assert (
        str(error.value) ==
        "Can't find MultipleInputFactory in consumer list"
    )


def test_dispatcher_multiple_matches_no_producer_matches():
    """
    Test multiple delivery type matches but no producer matches.

    Dispatcher returns None if no producer matches
    """
    array1 = Deliverable[list[str]](["a", "b"],).set_delivery_id("first")
    array2 = Deliverable[list[str]](["1", "2"]).set_delivery_id("second")
    array3 = Deliverable[list[str]](["x", "y"], producer=MultipleInputFactory)
    array4 = Deliverable[list[str]](["m", "n"], producer=MultipleInputFactory)
    factory = MultipleInputFactory()
    dispatcher = (
        Dispatcher()
        .add_deliverable(array1)
        .add_deliverable(array2)
        .add_deliverable(array3)
        .add_deliverable(array4)
    )

    with pytest.raises(InvalidDeliveryException) as error:
        dispatcher.dispatch(factory)

    assert str(error.value)


def test_dispatcher_matches_by_producer():
    """Test multiple delivery type matches by producer."""
    array1 = Deliverable[list[str]](["a", "b"],).set_delivery_id("first")
    array2 = Deliverable[list[str]](["1", "2"]).set_delivery_id("second")
    array3 = Deliverable[list[str]](["x", "y"])
    array4 = Deliverable[list[str]](["m", "n"], producer=MultipleInputFactory)
    factory = MultipleInputFactory()
    _ = (
        Dispatcher()
        .add_deliverable(array1)
        .add_deliverable(array2)
        .add_deliverable(array3)
        .add_deliverable(array4)
        .dispatch(factory)
    )

    assert factory.array3.get() == ["x", "y"]


def test_dispatcher_matches_by_consumer():
    """Test multiple delivery type matches by consumer."""
    array1 = Deliverable[list[str]](["a", "b"],).set_delivery_id("first")
    array2 = Deliverable[list[str]](["1", "2"]).set_delivery_id("second")
    array3 = Deliverable[list[str]](
        ["x", "y"],
        consumers=set([MultipleInputFactory])
    )
    array4 = Deliverable[list[str]](["m", "n"])
    factory = MultipleInputFactory()
    _ = (
        Dispatcher()
        .add_deliverable(array1)
        .add_deliverable(array2)
        .add_deliverable(array3)
        .add_deliverable(array4)
        .dispatch(factory)
    )

    assert factory.array3.get() == ["x", "y"]
