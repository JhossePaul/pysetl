"""Deliverable class."""
from __future__ import annotations
from textwrap import dedent
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic, TypeVar, get_args, Union
from typing_extensions import Self
from pysetl.utils import pretty
from pysetl.utils.mixins import IsIdentifiable
from .external import External


if TYPE_CHECKING:
    from .factory import Factory  # pragma: no cover


T = TypeVar("T")


@dataclass
class Deliverable(Generic[T], IsIdentifiable):
    """
    Deliverable is a container for data.

    A deliverable will transfer data through a Dispatcher.
    @param payload data that will be transferred
    """

    payload: T
    producer: Union[type[Factory], type[External]] = External
    consumers: set[type[Factory]] = field(default_factory=set)
    delivery_id: str = ""

    def __post_init__(self: Self) -> None:
        """Initialize UUID from IsIdentifiable."""
        IsIdentifiable.__init__(self)

    @property
    def payload_type(self: Self) -> type:
        """Returns Deliverable payload type."""
        original_class = getattr(self, "__orig_class__")
        (contained_type, *_) = get_args(original_class)

        return contained_type

    def same_deliverable(self: Self, other: Deliverable) -> bool:
        """Compare two deliverables."""
        _intersection = self.consumers.intersection(other.consumers)
        same_consumer = len(_intersection) != 0 \
            if len(self.consumers) != 0 or other.consumers != 0 \
            else True

        return self.has_same_payload_type(other) and \
            same_consumer and \
            self.producer == other.producer

    def __str__(self) -> str:
        """Pretty print for a deliverable."""
        consumers_string = ", ".join([pretty(x) for x in self.consumers])
        producer_str = pretty(self.producer)
        payload_type_str = pretty(self.payload_type)

        return dedent(f"""
        Deliverable: {payload_type_str}
          From: {producer_str}
          To: {consumers_string}
        """).strip()

    def __repr__(self: Self) -> str:
        """Customize repr method."""
        payload_type_str = pretty(self.payload_type)

        return f"Deliverable[{payload_type_str}]"

    def has_same_payload_type(self: Self, that: Deliverable) -> bool:
        """Compare types bewteen deliverable payloads."""
        return self.payload_type is that.payload_type

    def has_same_payload_type_name(self: Self, name: str) -> bool:
        """Compare types bewteen deliverable payloads."""
        return pretty(self.payload_type) == name

    def is_payload_type(self: Self, __payload_type: type) -> bool:
        """Check payload type of Deliverable."""
        return self.payload_type == __payload_type

    def set_producer(self: Self, producer: type) -> Self:
        """Setter method for producer private property."""
        self.producer = producer

        return self

    def set_consumers(self: Self, consumers: set[type[Factory]]) -> Self:
        """Setter method for consumers private property."""
        self.consumers = consumers

        return self

    def set_delivery_id(self: Self, delivery_id) -> Self:
        """Setter method for delivery_id private property."""
        self.delivery_id = delivery_id

        return self
