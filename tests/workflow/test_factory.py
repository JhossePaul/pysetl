"""Unit tests for pyset.workflow module."""
from typing import Optional
import pytest
from pydantic import BaseModel
from typing_extensions import Self, TypeVar
from typedspark import DataSet, Column, Schema
from pyspark.sql.types import StringType, IntegerType
from pysetl.workflow import Factory, Delivery
from pysetl.workflow.external import External
from pysetl.utils.mixins import HasSparkSession, HasBenchmark, HasDiagram


T = TypeVar("T", bound=DataSet)


class CitizenModel(BaseModel):
    """Model for a Citizen."""

    name: str
    age: int


class Citizen(Schema):
    """Schema for a citizens table."""

    name: Column[StringType]
    age: Column[IntegerType]


class FirstFactory(Factory[CitizenModel]):
    """First test factory."""

    output = None

    def read(self: Self) -> Self:
        """Return itself."""
        return self

    def process(self: Self) -> Self:
        """Create a citizen."""
        self.output = CitizenModel(name="Gerardo", age=28)

        return self

    def write(self: Self) -> Self:
        """Return itself."""
        return self

    def get(self: Self):
        """Return a citizen."""
        return self.output


class SecondFactory(Factory[DataSet[Citizen]]):
    """Second test factory."""

    first_citizen = Delivery[CitizenModel](FirstFactory)
    external_citizen = Delivery[CitizenModel](External, "the good citizen")
    output = None

    def read(self: Self) -> Self:
        """Return itself."""
        return self

    def process(self: Self) -> Self:
        """Take citizens and create a dataaset."""
        first_citizen = self.first_citizen.get()

        __unsafe_data = [first_citizen, CitizenModel(name="Gerardo", age=28)]
        __data: list[CitizenModel] = [
            c for c in __unsafe_data if isinstance(c, CitizenModel)
        ]

        return self

    def write(self: Self) -> Self:
        """Return itself."""
        return self

    def get(self: Self) -> Optional[DataSet[Citizen]]:
        """Return a DataSet[Citizen]."""
        return self.output


class CustomFactory(Factory[T]):
    """Custom factory should return a deilvery_type."""


class InheritedCustomFactory(CustomFactory[DataSet[Citizen]]):
    """Inherited custom factory should return a deilvery_type."""

    output: Optional[DataSet[Citizen]] = None

    def read(self: Self) -> Self:
        return self

    def process(self: Self) -> Self:
        return self

    def write(self: Self) -> Self:
        return self

    def get(self: Self) -> Optional[DataSet[Citizen]]:
        return self.output


class DeepInheritedFactory(InheritedCustomFactory):
    """Test deeper inheritance chain."""

    pass


class MalformedFactory(Factory):
    """A factory that inherits from Factory without type parameters - should cause issues."""

    def read(self: Self) -> Self:
        return self

    def process(self: Self) -> Self:
        return self

    def write(self: Self) -> Self:
        return self

    def get(self: Self):
        return None


class MalformedInheritedFactory(MalformedFactory):
    """This should trigger the infinite recursion at line 147."""

    pass


class TrickyFactory(Factory[DataSet[Citizen]]):
    """A factory that has proper type parameters but will cause issues in inheritance."""

    def read(self: Self) -> Self:
        return self

    def process(self: Self) -> Self:
        return self

    def write(self: Self) -> Self:
        return self

    def get(self: Self):
        return None


class TrickyInheritedFactory(TrickyFactory):
    """This should trigger the infinite recursion at line 147 because it's a concrete class."""

    pass


# Multiple inheritance test cases
class SparkFactory(Factory[CitizenModel], HasSparkSession):
    """Factory with HasSparkSession mixin."""

    output = None

    def read(self: Self) -> Self:
        """Return itself."""
        return self

    def process(self: Self) -> Self:
        """Create a citizen using spark session."""
        # Access spark session from mixin
        self.output = CitizenModel(name="SparkUser", age=30)
        return self

    def write(self: Self) -> Self:
        """Return itself."""
        return self

    def get(self: Self):
        """Return a citizen."""
        return self.output


class BenchmarkFactory(Factory[DataSet[Citizen]], HasBenchmark):
    """Factory with HasBenchmark mixin."""

    output = None

    def read(self: Self) -> Self:
        """Return itself."""
        return self

    def process(self: Self) -> Self:
        """Process with benchmarking."""
        return self

    def write(self: Self) -> Self:
        """Return itself."""
        return self

    def get(self: Self) -> Optional[DataSet[Citizen]]:
        """Return a DataSet[Citizen]."""
        return self.output

    def get_benchmarks(self):
        """Return execution benchmarks."""
        return []


class DiagramFactory(Factory[CitizenModel], HasDiagram):
    """Factory with HasDiagram mixin."""

    output = None

    def read(self: Self) -> Self:
        """Return itself."""
        return self

    def process(self: Self) -> Self:
        """Process with diagram generation."""
        return self

    def write(self: Self) -> Self:
        """Return itself."""
        return self

    def get(self: Self):
        """Return a citizen."""
        return self.output

    @property
    def diagram_id(self: Self) -> str:
        """Return diagram id."""
        return "diagram_factory"

    def to_diagram(self) -> str:
        """Generate diagram representation."""
        return "DiagramFactory -> CitizenModel"


class MultiMixinFactory(
    Factory[DataSet[Citizen]], HasSparkSession, HasBenchmark, HasDiagram
):
    """Factory with multiple mixins."""

    output = None

    def read(self: Self) -> Self:
        """Return itself."""
        return self

    def process(self: Self) -> Self:
        """Process with all mixin capabilities."""
        return self

    def write(self: Self) -> Self:
        """Return itself."""
        return self

    def get(self: Self) -> Optional[DataSet[Citizen]]:
        """Return a DataSet[Citizen]."""
        return self.output

    def get_benchmarks(self):
        """Return execution benchmarks."""
        return []

    @property
    def diagram_id(self: Self) -> str:
        """Return diagram id."""
        return "multi_mixin_factory"

    def to_diagram(self) -> str:
        """Generate diagram representation."""
        return "MultiMixinFactory -> DataSet[Citizen]"


class InheritedSparkFactory(SparkFactory):
    """Inherited factory with HasSparkSession mixin."""

    def process(self: Self) -> Self:
        """Override process method."""
        self.output = CitizenModel(name="InheritedSparkUser", age=25)
        return self


class DeepMultiInheritanceFactory(MultiMixinFactory):
    """Deep inheritance with multiple mixins."""

    def process(self: Self) -> Self:
        """Override process method."""
        return self


class MixinFirstFactory(HasSparkSession, HasBenchmark, Factory[CitizenModel]):
    """Factory with mixins before Factory in MRO."""

    output = None

    def read(self: Self) -> Self:
        """Return itself."""
        return self

    def process(self: Self) -> Self:
        """Process with mixin capabilities."""
        self.output = CitizenModel(name="MixinFirstUser", age=35)
        return self

    def write(self: Self) -> Self:
        """Return itself."""
        return self

    def get(self: Self):
        """Return a citizen."""
        return self.output

    def get_benchmarks(self):
        """Return execution benchmarks."""
        return []


class ComplexInheritanceChain(MixinFirstFactory):
    """Complex inheritance chain with mixins and Factory."""

    def process(self: Self) -> Self:
        """Override process method."""
        self.output = CitizenModel(name="ComplexUser", age=40)
        return self


def test_factory():
    """Test a Factory."""
    first_factory = FirstFactory().read().process().write()
    second_factory = SecondFactory()

    _t1 = first_factory.delivery_type()
    _t2 = second_factory.delivery_type()

    assert _t1 == CitizenModel
    assert _t2 == DataSet[Citizen]
    assert first_factory.uuid
    assert str(first_factory) == "FirstFactory -> CitizenModel"
    assert str(second_factory) == "SecondFactory -> DataSet[Citizen]"
    assert repr(first_factory) == "FirstFactory will produce a CitizenModel"
    assert repr(second_factory) == "SecondFactory will produce a DataSet[Citizen]"
    assert first_factory.deliverable.payload == CitizenModel(name="Gerardo", age=28)
    assert [
        delivery.payload_type for delivery in second_factory.expected_deliveries()
    ] == [CitizenModel, CitizenModel]
    assert [delivery.producer for delivery in second_factory.expected_deliveries()] == [
        External,
        FirstFactory,
    ]


class NoTypeFactory(Factory):
    """Factory with not type should fail to deliver."""

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
        """Return nothing."""


def test_no_type_factory():
    """A factory should fail if no type parameter passed."""
    no_type_factory = NoTypeFactory()

    with pytest.raises(NotImplementedError) as error:
        no_type_factory.delivery_type()

    assert str(error.value) == "NoTypeFactory factory does not have a type parameter"


def test_inherited_custom_factory():
    """Test that inherited custom factories can extract their type parameter correctly."""
    inherited_factory = InheritedCustomFactory()

    delivery_type = inherited_factory.delivery_type()

    assert delivery_type == DataSet[Citizen]


def test_deep_inherited_factory():
    """Test that deeply inherited factories can extract their type parameter correctly."""
    deep_factory = DeepInheritedFactory()

    # This should work with deeper inheritance
    delivery_type = deep_factory.delivery_type()

    # Check that we get the correct type (DataSet[Citizen])
    assert delivery_type == DataSet[Citizen]


# Multiple inheritance test cases
def test_spark_factory_delivery_type():
    """Test that Factory with HasSparkSession mixin correctly extracts delivery type."""
    spark_factory = SparkFactory()

    delivery_type = spark_factory.delivery_type()

    assert delivery_type == CitizenModel
    # Verify mixin functionality still works
    assert hasattr(spark_factory, "spark")
    assert spark_factory.spark is not None


def test_benchmark_factory_delivery_type():
    """Test that Factory with HasBenchmark mixin correctly extracts delivery type."""
    benchmark_factory = BenchmarkFactory()

    delivery_type = benchmark_factory.delivery_type()

    assert delivery_type == DataSet[Citizen]
    # Verify mixin functionality still works
    assert hasattr(benchmark_factory, "get_benchmarks")
    assert hasattr(benchmark_factory, "benchmarked")


def test_diagram_factory_delivery_type():
    """Test that Factory with HasDiagram mixin correctly extracts delivery type."""
    diagram_factory = DiagramFactory()

    delivery_type = diagram_factory.delivery_type()

    assert delivery_type == CitizenModel
    # Verify mixin functionality still works
    assert hasattr(diagram_factory, "diagram_id")
    assert hasattr(diagram_factory, "to_diagram")
    assert diagram_factory.diagram_id == "diagram_factory"


def test_multi_mixin_factory_delivery_type():
    """Test that Factory with multiple mixins correctly extracts delivery type."""
    multi_factory = MultiMixinFactory()

    delivery_type = multi_factory.delivery_type()

    assert delivery_type == DataSet[Citizen]
    # Verify all mixin functionality still works
    assert hasattr(multi_factory, "spark")
    assert hasattr(multi_factory, "get_benchmarks")
    assert hasattr(multi_factory, "diagram_id")
    assert hasattr(multi_factory, "to_diagram")


def test_inherited_spark_factory_delivery_type():
    """Test that inherited Factory with HasSparkSession mixin correctly extracts delivery type."""
    inherited_spark_factory = InheritedSparkFactory()

    delivery_type = inherited_spark_factory.delivery_type()

    assert delivery_type == CitizenModel
    # Verify mixin functionality still works
    assert hasattr(inherited_spark_factory, "spark")
    assert inherited_spark_factory.spark is not None


def test_deep_multi_inheritance_factory_delivery_type():
    """Test that deeply inherited Factory with multiple mixins correctly extracts delivery type."""
    deep_factory = DeepMultiInheritanceFactory()

    delivery_type = deep_factory.delivery_type()

    assert delivery_type == DataSet[Citizen]
    # Verify all mixin functionality still works
    assert hasattr(deep_factory, "spark")
    assert hasattr(deep_factory, "get_benchmarks")
    assert hasattr(deep_factory, "diagram_id")
    assert hasattr(deep_factory, "to_diagram")


def test_mixin_first_factory_delivery_type():
    """Test that Factory with mixins before Factory in MRO correctly extracts delivery type."""
    mixin_first_factory = MixinFirstFactory()

    delivery_type = mixin_first_factory.delivery_type()

    assert delivery_type == CitizenModel
    # Verify mixin functionality still works
    assert hasattr(mixin_first_factory, "spark")
    assert hasattr(mixin_first_factory, "get_benchmarks")
    assert mixin_first_factory.spark is not None


def test_complex_inheritance_chain_delivery_type():
    """Test that complex inheritance chain with mixins correctly extracts delivery type."""
    complex_factory = ComplexInheritanceChain()

    delivery_type = complex_factory.delivery_type()

    assert delivery_type == CitizenModel
    # Verify mixin functionality still works
    assert hasattr(complex_factory, "spark")
    assert hasattr(complex_factory, "get_benchmarks")
    assert complex_factory.spark is not None


def test_factory_with_mixins_string_representation():
    """Test that Factory with mixins has correct string representation."""
    spark_factory = SparkFactory()
    multi_factory = MultiMixinFactory()

    assert str(spark_factory) == "SparkFactory -> CitizenModel"
    assert str(multi_factory) == "MultiMixinFactory -> DataSet[Citizen]"
    assert repr(spark_factory) == "SparkFactory will produce a CitizenModel"
    assert repr(multi_factory) == "MultiMixinFactory will produce a DataSet[Citizen]"


def test_factory_with_mixins_deliverable():
    """Test that Factory with mixins produces correct deliverable."""
    spark_factory = SparkFactory().read().process().write()

    deliverable = spark_factory.deliverable

    assert deliverable.payload == CitizenModel(name="SparkUser", age=30)
    assert deliverable.producer == SparkFactory
    assert deliverable.consumers == set()
    assert deliverable.delivery_id == ""


def test_factory_with_mixins_expected_deliveries():
    """Test that Factory with mixins correctly identifies expected deliveries."""
    spark_factory = SparkFactory()

    expected_deliveries = spark_factory.expected_deliveries()

    # This factory has no deliveries, so should return empty list
    assert expected_deliveries == []
