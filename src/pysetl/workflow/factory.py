"""Factory class definition."""
from __future__ import annotations
from abc import abstractmethod, ABC
from inspect import getmembers
from typing import TypeVar, Generic, Optional, get_args, get_origin
from typing_extensions import Self, get_original_bases
from pysetl.utils.mixins import HasLogger, IsIdentifiable, IsWritable
from pysetl.workflow.deliverable import Deliverable
from pysetl.workflow.delivery import Delivery
from pysetl.workflow.delivery_type import DeliveryType
from pysetl.utils.pretty import pretty


T = TypeVar("T")


class Factory(IsIdentifiable, Generic[T], HasLogger, IsWritable, ABC):
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

    def __str__(self: Self) -> str:
        """Customize str method."""
        self_name = type(self).__name__
        delivery_type_str = pretty(self.delivery_type().tp)

        return f"{self_name} -> {delivery_type_str}"

    def __repr__(self: Self) -> str:
        """Customize repr method."""
        self_name = type(self).__name__
        delivery_type_str = pretty(self.delivery_type().tp)

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
    def delivery_type(cls) -> DeliveryType:
        """Return delivery type."""
        bases = get_original_bases(cls)
        base_factories = [base for base in bases if get_origin(base) is Factory]

        if not base_factories:
            raise NotImplementedError("Factory has no type parameter")

        base_factory, *_ = base_factories
        factory_delivery_type, *_ = get_args(base_factory)

        return DeliveryType(factory_delivery_type)

    @property
    def deliverable(self: Self) -> Deliverable:
        """Return the deliverable from this factory."""
        __type = self.delivery_type().tp

        return Deliverable[__type](  # type: ignore
            payload=self.get(),
            producer=type(self),
            consumers=self.consumers,
            delivery_id=self.delivery_id,
        )

    @classmethod
    def expected_deliveries(cls) -> list[Delivery]:
        """Return a list of the expected deliveries for the factory."""
        return [
            delivery.set_consumer(cls)
            for (_, delivery) in list(
                getmembers(cls, lambda _: isinstance(_, Delivery))
            )
        ]
