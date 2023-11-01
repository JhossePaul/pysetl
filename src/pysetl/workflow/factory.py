"""Factory class definition."""
from __future__ import annotations
from abc import abstractmethod, ABC
from inspect import getmembers
from typing import TypeVar, Generic, Optional, get_args, get_origin
from typing_extensions import Self
from pysetl.utils import pretty
from pysetl.utils.mixins import (
    HasLogger, IsIdentifiable, IsWritable
)
from pysetl.workflow.deliverable import Deliverable
from pysetl.workflow.delivery import Delivery


T = TypeVar("T")


class Factory(
    IsIdentifiable,
    Generic[T],
    HasLogger,
    IsWritable,
    ABC
):
    """
    A Factory is a structured object to ETL data.

    A Factory could be used to manipulate data.
    A Factory is able to read data from a data source, process/transform it
    and write it back to the storage
    """

    consumers: set[type[Factory]] = set()
    delivery_id: str = ""

    def __post_init__(self: Self) -> None:
        """Initialize UUID from IsIdentifiable."""
        IsIdentifiable.__init__(self)  # pragma: no cover

    def not_aliased_delivery_type(
            self: Self
            ) -> tuple[Optional[type], Optional[type]]:
        """
        Not aliased delivery type.

        Delivery type can be a _GenericAlias. This method always returns the
        expected type and the type argument if exists.
        """
        if isinstance(self.delivery_type(), type):
            return self.delivery_type(), None

        origin = get_origin(self.delivery_type())
        args = get_args(self.delivery_type())
        arg = None if len(args) == 0 else args[0]

        return origin, arg

    def __str__(self: Self) -> str:
        """Customize str method."""
        self_name = type(self).__name__
        delivery_type_str = pretty(self.delivery_type())

        return f"{self_name} -> {delivery_type_str}"

    def __repr__(self: Self) -> str:
        """Customize repr method."""
        self_name = type(self).__name__
        delivery_type_str = pretty(self.delivery_type())

        return f"{self_name} will produce a {delivery_type_str}"

    @abstractmethod
    def read(self) -> Self:
        """Encapsulate reading operations."""

    @abstractmethod
    def process(self) -> Self:
        """Encapsulate business rules into data operations."""

    @abstractmethod
    def write(self) -> Self:
        """Persist data on disk."""

    @abstractmethod
    def get(self) -> Optional[T]:
        """Get returns output data to recicycle in other factories."""

    @classmethod
    def delivery_type(cls) -> type:
        """Return delivery type."""
        bases = getattr(cls, "__orig_bases__")
        base_factories = [
            base
            for base
            in bases
            if get_origin(base) is Factory
        ]

        if len(base_factories) == 0:
            raise NotImplementedError("Factory has no type parameter")

        base_factory = base_factories[0] if len(base_factories) != 0 else None
        factory_args = get_args(base_factory)

        return factory_args[0]

    @property
    def deliverable(self: Self) -> Deliverable:
        """Return the deliverable from this factory."""
        __type = self.delivery_type()

        return (
            Deliverable[__type](  # type: ignore
                payload=self.get(),
                producer=type(self),
                consumers=self.consumers,
                delivery_id=self.delivery_id
            )
        )

    @classmethod
    def expected_deliveries(cls) -> list[Delivery]:
        """Return a list of the expected deliveries for the factory."""
        return [
            delivery.set_consumer(cls)
            for (_, delivery)
            in list(getmembers(cls, lambda _: isinstance(_, Delivery)))
        ]
