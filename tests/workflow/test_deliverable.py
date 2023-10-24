"""Test pysetl.workflow.deliverable module."""
from abc import ABC
from dataclasses import dataclass
from pysetl.workflow import Deliverable, Factory


@dataclass
class Citizen:
    """Class for testing purposes."""

    name: str
    age: int


class ProducerFactory(Factory[Citizen], ABC):
    """Class for testing purposes."""


class ConsumerFactory(Factory[Citizen], ABC):
    """Class for testing purposes."""


def test_deliverable():
    """Test a Deliverable."""
    deliverable = Deliverable[Citizen](
        payload=Citizen(name="Paul", age=31),
        producer=ProducerFactory,
        consumers=set([ConsumerFactory])
    )

    deliverable2 = Deliverable[Citizen](
        payload=Citizen(name="Gerardo", age=28)
    ).set_producer(ProducerFactory).set_consumers(set([ConsumerFactory]))

    deliverable3 = Deliverable[str](
        payload="string"
    ).set_delivery_id("a simple string")

    assert deliverable.payload_type is Citizen
    assert deliverable.same_deliverable(deliverable2)
    assert not deliverable.same_deliverable(deliverable3)
    assert deliverable.has_same_payload_type(deliverable2)
    assert not deliverable.has_same_payload_type(deliverable3)
    assert repr(deliverable) == "Deliverable[Citizen]"
    assert (
        str(deliverable) ==
        "Deliverable: Citizen\n  From: ProducerFactory\n  To: ConsumerFactory"
    )
    assert deliverable.has_same_payload_type_name("Citizen")
    assert deliverable.is_payload_type(Citizen)
    assert deliverable3.delivery_id == "a simple string"
