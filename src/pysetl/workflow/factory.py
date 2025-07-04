"""
Factory module for PySetl.

Defines the Factory abstraction, the core building block for ETL workflows in PySetl.
Users subclass Factory to implement business logic, structuring their project as a set of
modular, reusable, and maintainable ETL components. Factories are passed to stages and
assembled into pipelines, providing a natural and tidy project structure for large ETL processes.
"""
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
    Core abstraction for ETL logic in PySetl.

    A Factory encapsulates a unit of ETL business logic, abstracting the process into
    three main steps: read, process, and write. Users subclass Factory to implement
    their own data transformations, and then compose these factories into stages and
    pipelines. This approach encourages a modular, maintainable, and reusable project
    structure, especially valuable for large ETL processes.

    Factories declare their dependencies using Delivery instances as class attributes.
    At runtime, the pipeline and dispatcher resolve these dependencies, inject the
    required data, and orchestrate the execution of each factory.

    The get method is used by the pipeline to extract the output of the factory and
    register it as a Deliverable in the dispatcher registry, making it available to
    downstream consumers.

    To use PySetl, users subclass Factory, implement the abstract methods, and pass
    their factories to stages and pipelines. This results in a tidy, testable, and
    scalable ETL codebase.

    Attributes:
        consumers (set[type[Factory]]): Set of factories that consume this factory's output.
        delivery_id (str): Optional identifier for the factory's output (for disambiguation).
    """

    consumers: set[type[Factory]] = set()
    delivery_id: str = ""

    def __post_init__(self: Self) -> None:
        """
        Initialize the Factory and its unique identifier (for tracking in the pipeline).
        """
        IsIdentifiable.__init__(self)  # pragma: no cover

    def __str__(self: Self) -> str:
        """
        Return a human-readable string representation of the factory.

        Returns:
            str: The factory's class name and its output type.
        """
        self_name = type(self).__name__
        delivery_type_str = pretty(self.delivery_type().tp)

        return f"{self_name} -> {delivery_type_str}"

    def __repr__(self: Self) -> str:
        """
        Return the string representation for debugging.

        Returns:
            str: The factory's class name and its output type.
        """
        self_name = type(self).__name__
        delivery_type_str = pretty(self.delivery_type().tp)

        return f"{self_name} will produce a {delivery_type_str}"

    @abstractmethod
    def read(self) -> Self:
        """
        Encapsulate reading operations.

        This method should implement the logic to read input data or dependencies.
        Returns self for method chaining.
        """

    @abstractmethod
    def process(self) -> Self:
        """
        Encapsulate business rules and data transformations.

        This method should implement the core transformation logic of the factory.
        Returns self for method chaining.
        """

    @abstractmethod
    def write(self) -> Self:
        """
        Persist data on disk or to an external system.

        This method should implement the logic to write the output of the factory.
        Returns self for method chaining.
        """

    @abstractmethod
    def get(self) -> Optional[T]:
        """
        Return the output data produced by this factory.

        This method is called by the pipeline to extract the result of the factory's
        computation and register it as a Deliverable for downstream consumers.

        Returns:
            Optional[T]: The output data to be delivered to other factories.
        """

    @classmethod
    def delivery_type(cls) -> DeliveryType:
        """
        Return the declared output type of this factory (for dependency resolution).

        Returns:
            DeliveryType: The type of the output produced by this factory.

        Raises:
            NotImplementedError: If the factory does not declare a type parameter.
        """
        bases = get_original_bases(cls)
        base_factories = [base for base in bases if get_origin(base) is Factory]
        if not base_factories:
            raise NotImplementedError("Factory has no type parameter")
        base_factory, *_ = base_factories
        factory_delivery_type, *_ = get_args(base_factory)
        return DeliveryType(factory_delivery_type)

    @property
    def deliverable(self: Self) -> Deliverable:
        """
        Return the Deliverable produced by this factory (for dispatcher registration).

        Returns:
            Deliverable: The output of this factory, wrapped for pipeline transfer.
        """
        __type = self.delivery_type().tp
        return Deliverable[__type](  # type: ignore
            payload=self.get(),
            producer=type(self),
            consumers=self.consumers,
            delivery_id=self.delivery_id,
        )

    @classmethod
    def expected_deliveries(cls) -> list[Delivery]:
        """
        Return a list of the Delivery dependencies declared by this factory.

        Returns:
            list[Delivery]: The declared input slots (dependencies) for this factory.
        """
        return [
            delivery.set_consumer(cls)
            for (_, delivery) in list(
                getmembers(cls, lambda _: isinstance(_, Delivery))
            )
        ]
