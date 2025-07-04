"""
Deliverable module for PySetl.

Defines the Deliverable class, which is a concrete, runtime data container representing
the output of a factory or external input registered to the pipeline. Deliverables are
produced after a factory runs, and are used by the dispatcher to transfer actual data
between pipeline stages. They are the runtime artifacts that fulfill the dependency slots
declared by Delivery instances.

In summary: Deliverable is the produced value (output), Delivery is the declared dependency (input slot).
"""
from __future__ import annotations
from textwrap import dedent
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic, TypeVar, get_args, Union
from typing_extensions import Self
from pysetl.utils import pretty
from pysetl.utils.mixins import IsIdentifiable
from pysetl.workflow.external import External
from pysetl.workflow.delivery_type import DeliveryType


if TYPE_CHECKING:
    from .factory import Factory  # pragma: no cover


T = TypeVar("T")


@dataclass
class Deliverable(Generic[T], IsIdentifiable):
    """
    Concrete, runtime data container representing the output of a factory or external input.

    A Deliverable is created after a factory (or external source) produces a value. It holds
    the actual payload, tracks its producer, and the set of consumer factories that depend on it.
    Deliverables are registered to the pipeline and are used by the dispatcher to transfer data
    between pipeline stages, fulfilling the dependency slots declared by Delivery instances.

    In summary: Deliverable is the produced value (output), Delivery is the declared dependency (input slot).

    Attributes:
        payload (T): The data being delivered.
        producer (Union[type[Factory], type[External]]): The class that produced the deliverable.
        consumers (set[type[Factory]]): Set of factories that consume this deliverable.
        delivery_id (str): Optional identifier for the deliverable.
    """

    payload: T
    producer: Union[type[Factory], type[External]] = External
    consumers: set[type[Factory]] = field(default_factory=set)
    delivery_id: str = ""

    def __post_init__(self: Self) -> None:
        """
        Initialize the Deliverable and its unique identifier (for tracking in the pipeline).
        """
        IsIdentifiable.__init__(self)

    @property
    def payload_type(self: Self) -> DeliveryType:
        """
        Return the type of the payload as a DeliveryType (for dependency matching).

        Returns:
            DeliveryType: The type of the payload contained in this deliverable.
        """
        original_class = getattr(self, "__orig_class__")
        contained_type, *_ = get_args(original_class)
        return DeliveryType(contained_type)

    def same_deliverable(self: Self, other: Deliverable) -> bool:
        """
        Compare this deliverable to another for equivalence (for dispatcher matching).

        Two deliverables are considered the same if they have the same payload type,
        at least one common consumer (unless both have no consumers), and the same producer.

        Args:
            other (Deliverable): The deliverable to compare against.

        Returns:
            bool: True if the deliverables are considered equivalent, False otherwise.
        """
        _intersection = self.consumers.intersection(other.consumers)
        same_consumer = (
            len(_intersection) != 0
            if len(self.consumers) != 0 or other.consumers != 0
            else True
        )
        return (
            self.has_same_payload_type(other)
            and same_consumer
            and self.producer == other.producer
        )

    def __str__(self) -> str:
        """
        Return a human-readable string representation of the deliverable.

        Returns:
            str: A pretty-printed summary of the deliverable's type, producer, and consumers.
        """
        consumers_string = ", ".join([pretty(x) for x in self.consumers])
        producer_str = pretty(self.producer)
        payload_type_str = pretty(self.payload_type.tp)
        return dedent(
            f"""
        Deliverable: {payload_type_str}
          From: {producer_str}
          To: {consumers_string}
        """
        ).strip()

    def __repr__(self: Self) -> str:
        """
        Return the string representation for debugging.

        Returns:
            str: The type of the deliverable payload.
        """
        payload_type_str = pretty(self.payload_type.tp)
        return f"Deliverable[{payload_type_str}]"

    def has_same_payload_type(self: Self, that: Deliverable) -> bool:
        """
        Compare the payload types of two deliverables (for dispatcher matching).

        Args:
            that (Deliverable): The deliverable to compare against.

        Returns:
            bool: True if the payload types are the same, False otherwise.
        """
        return self.payload_type == that.payload_type

    def has_same_payload_type_name(self: Self, name: str) -> bool:
        """
        Compare the payload type name to a string (for dispatcher matching).

        Args:
            name (str): The name to compare against.

        Returns:
            bool: True if the payload type name matches, False otherwise.
        """
        return pretty(self.payload_type.tp) == name

    def is_payload_type(self: Self, __payload_type: type) -> bool:
        """
        Check if the payload type matches a given type (for dispatcher matching).

        Args:
            __payload_type (type): The type to check against.

        Returns:
            bool: True if the payload type matches, False otherwise.
        """
        return self.payload_type == __payload_type

    def set_producer(self: Self, producer: type) -> Self:
        """
        Set the producer for this deliverable (for dispatcher or test setup).

        Args:
            producer (type): The producer class.

        Returns:
            Self: The deliverable instance (for chaining).
        """
        self.producer = producer
        return self

    def set_consumers(self: Self, consumers: set[type[Factory]]) -> Self:
        """
        Set the consumers for this deliverable (for dispatcher or test setup).

        Args:
            consumers (set[type[Factory]]): The set of consumer factories.

        Returns:
            Self: The deliverable instance (for chaining).
        """
        self.consumers = consumers
        return self

    def set_delivery_id(self: Self, delivery_id: str) -> Self:
        """
        Set the delivery_id for this deliverable (for dispatcher or test setup).

        Args:
            delivery_id: The identifier to set.

        Returns:
            Self: The deliverable instance (for chaining).
        """
        self.delivery_id = delivery_id
        return self
