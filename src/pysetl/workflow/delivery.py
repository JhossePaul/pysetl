"""
Delivery module for PySetl.

Defines the Delivery class, which represents a typed, declarative input slot for a factory.
A Delivery is not a data container, but a placeholder for a value of type T that will be
injected at runtime by the dispatcher. Deliveries are used to declare dependencies between
factories, enabling the construction of the DAG by searching for type dependencies.
"""
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
    Declarative input slot for a factory, representing a dependency to be injected at runtime.

    A Delivery is used to declare that a factory expects a value of type T from another factory
    (the producer) or from an external source. At pipeline construction time, Deliveries are
    used to build the dependency graph (DAG) by matching types and delivery IDs. At runtime,
    the dispatcher fills each Delivery with the actual value from the corresponding Deliverable.

    Unlike Deliverable, Delivery does not own or transfer data; it is a placeholder for a value
    to be injected. Its lifecycle is: declared as a class attribute on a Factory, matched by the
    dispatcher, and filled with a value before the factory's logic is executed.

    Attributes:
        producer (type[Union[Factory, External]]): The class that is expected to produce the value.
        delivery_id (str): Optional identifier for the delivery (for disambiguation).
        consumer (Optional[type[Factory]]): The class that will consume the value (usually set automatically).
    """

    def __init__(
        self: Self,
        producer: type[Union[Factory, External]] = External,
        delivery_id: str = "",
        consumer: Optional[type[Factory]] = None,
    ) -> None:
        """
        Initialize a Delivery placeholder for a dependency.

        Args:
            producer (type[Union[Factory, External]]): The class expected to produce the value (default: External).
            delivery_id (str): Optional identifier for the delivery (for disambiguation).
            consumer (Optional[type[Factory]]): The class that will consume the value (usually set by the framework).
        """
        self.producer = producer
        self.delivery_id = delivery_id
        self.consumer = consumer
        self.__value: Optional[T] = None

    def __str__(self: Self) -> str:
        """
        Return a human-readable string representation of the delivery.

        Returns:
            str: Description of the delivery's producer and payload type.
        """
        _producer = pretty(self.producer)
        _type = pretty(self.payload_type)

        return f"Delivery from {_producer} with type {_type}"

    def __repr__(self: Self) -> str:
        """
        Return the string representation for debugging.

        Returns:
            str: Description of the delivery's producer and payload type.
        """
        return str(self)

    def set(self: Self, __value: T) -> Self:
        """
        Fill the Delivery with a value at runtime (called by the dispatcher).

        Args:
            __value (T): The value to inject into the delivery.

        Returns:
            Self: The delivery instance (for chaining).
        """
        self.__value = __value
        return self

    def get(self: Self) -> T:
        """
        Retrieve the value injected into this Delivery.

        Returns:
            T: The value injected by the dispatcher.

        Raises:
            InvalidDeliveryException: If the delivery has not been filled at runtime.
        """
        if not self.__value:
            raise InvalidDeliveryException("Delivery with no value")
        return self.__value

    @property
    def payload_type(self: Self) -> type:
        """
        Return the type of value this Delivery expects.

        Returns:
            type: The type parameter T for this delivery (used for dependency resolution).
        """
        instance_type = getattr(self, "__orig_class__")
        (type_param,) = get_args(instance_type)
        return type_param

    def set_consumer(self: Self, __value: type[Factory]) -> Self:
        """
        Set the consumer (factory) for this delivery (used by the framework).

        Args:
            __value (type[Factory]): The consumer class.

        Returns:
            Self: The delivery instance (for chaining).
        """
        self.consumer = __value
        return self
