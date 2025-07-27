"""Unit tests for pyset.workflow module."""
from typing import Optional
import pytest
from pydantic import BaseModel
from typing_extensions import Self, TypeVar
from typedspark import DataSet, Column, Schema
from pyspark.sql.types import StringType, IntegerType
from pysetl.workflow import Factory, Delivery
from pysetl.workflow.external import External


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
