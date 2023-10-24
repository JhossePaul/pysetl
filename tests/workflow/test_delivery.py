"""Test pysetl.workflow.delivery module."""
from abc import ABC
from dataclasses import dataclass
import pytest
from pysetl.workflow import Delivery, Factory
from pysetl.utils.exceptions import InvalidDeliveryException


@dataclass
class Citizen:
    """Class for testing purposes."""

    name: str
    age: int


class CitizenFactory(Factory[Citizen], ABC):
    """Class for testing purposes."""


def test_delivery():
    """Test a Delivery."""
    citizen = Citizen(name="Paul", age=31)
    delivery = Delivery[Citizen](CitizenFactory).set(citizen)

    assert str(delivery) == "Delivery from CitizenFactory with type Citizen"
    assert repr(delivery) == str(delivery)
    assert delivery.get() == citizen
    assert delivery.payload_type is Citizen


def test_delivery_exceptions():
    """Delivery should throw InvalidDeliveryException when get but not set."""
    delivery = Delivery()

    with pytest.raises(InvalidDeliveryException) as error:
        delivery.get()

    assert str(error.value) == "Delivery with no value"
