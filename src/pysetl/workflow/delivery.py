"""Delivery module."""
from __future__ import annotations
from typing import TYPE_CHECKING, Generic, TypeVar, Optional, Union, get_args
from typing_extensions import Self
from pysetl.utils.exceptions import InvalidDeliveryException
from pysetl.utils import pretty
from .external import External


if TYPE_CHECKING:  # pragma: no cover
    from .factory import Factory


T = TypeVar("T")


class Delivery(Generic[T]):
    """
    Container for results from another factory.

    Class designed to retrive data from the Deliverable pool
    """

    def __init__(
        self: Self,
        producer: type[Union[Factory, External]] = External,
        delivery_id: str = "",
        consumer: Optional[type[Factory]] = None
    ) -> None:
        self.producer = producer
        self.delivery_id = delivery_id
        self.consumer = consumer
        self.__value: Optional[T] = None

    def __str__(self: Self) -> str:
        """Customize str method."""
        _producer = pretty(self.producer)
        _type = pretty(self.payload_type)

        return f"Delivery from {_producer} with type {_type}"

    def __repr__(self: Self) -> str:
        """Customize repr method."""
        return str(self)

    def set(self: Self, __value: T) -> Self:
        """Set the value of the delivery."""
        self.__value = __value

        return self

    def get(self: Self) -> T:
        """Get delivery."""
        if not self.__value:
            raise InvalidDeliveryException("Delivery with no value")

        return self.__value

    @property
    def payload_type(self: Self) -> type:
        """Return payload type."""
        instance_type = getattr(self, "__orig_class__")
        (type_param,) = get_args(instance_type)

        return type_param

    def set_consumer(self: Self, __value: type[Factory]) -> Self:
        """Set Delivery consumer."""
        self.consumer = __value

        return self
