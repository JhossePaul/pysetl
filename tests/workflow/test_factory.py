"""Unit tests for pyset.workflow module."""
from typing import Optional
import pytest
from pydantic import BaseModel
from typing_extensions import Self
from typedspark import DataSet, Column, Schema
from pyspark.sql.types import StringType, IntegerType
from pysetl.workflow import Factory, Delivery
from pysetl.workflow.external import External


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

        __unsafe_data = [
            first_citizen,
            CitizenModel(name="Gerardo", age=28)
        ]
        __data: list[CitizenModel] = [
            c
            for c
            in __unsafe_data
            if isinstance(c, CitizenModel)
        ]

        return self

    def write(self: Self) -> Self:
        """Return itself."""
        return self

    def get(self: Self) -> Optional[DataSet[Citizen]]:
        """Return a DataSet[Citizen]."""
        return self.output


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
    assert (
        repr(second_factory) ==
        "SecondFactory will produce a DataSet[Citizen]"
    )
    assert (
        first_factory.deliverable.payload ==
        CitizenModel(name="Gerardo", age=28)
    )
    assert [
        delivery.payload_type
        for delivery
        in second_factory.expected_deliveries()
    ] == [CitizenModel, CitizenModel]
    assert [
        delivery.producer
        for delivery
        in second_factory.expected_deliveries()
    ] == [External, FirstFactory]


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

    assert str(error.value) == "Factory has no type parameter"
