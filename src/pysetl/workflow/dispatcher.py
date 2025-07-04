"""
Dispatcher module for PySetl.

Defines the Dispatcher class, which is the runtime dependency injection system for the pipeline.
The Dispatcher manages a pool of Deliverables (produced values) and injects them into Delivery
slots (declared dependencies) at runtime. It acts as the bridge between the declarative dependency
system and the actual data flow during pipeline execution.

The Dispatcher is responsible for:
- Collecting Deliverables from factories after they execute
- Managing a registry of available Deliverables
- Finding the correct Deliverable for each Delivery dependency
- Injecting values into Delivery slots before factory execution
- Resolving ambiguous dependencies using producer and consumer information
"""
from __future__ import annotations
from typing import TYPE_CHECKING, Callable, Any, Optional, Union
from typing_extensions import Self
from pysetl.utils.mixins import HasRegistry, HasLogger
from pysetl.utils.exceptions import InvalidDeliveryException
from .deliverable import Deliverable
from .stage import Stage
from .delivery import Delivery
from .external import External


if TYPE_CHECKING:  # pragma: no cover
    from .factory import Factory


class Dispatcher(HasRegistry[Deliverable], HasLogger):
    """
    Runtime dependency injection system for the pipeline.

    The Dispatcher manages the runtime data flow between pipeline stages by collecting
    Deliverables from factories and injecting them into Delivery slots. It maintains
    a registry of available Deliverables and resolves dependencies using type matching,
    producer information, and consumer constraints.

    The Dispatcher is the core component that enables the declarative dependency system
    to work at runtime, ensuring that each factory receives its required inputs before
    execution.

    Key responsibilities:
    - Collect Deliverables from factories and stages
    - Manage a registry of available Deliverables
    - Resolve dependencies by matching types, producers, and consumers
    - Inject values into Delivery slots before factory execution
    - Handle ambiguous dependencies with intelligent matching
    """

    def add_deliverable(self: Self, __deliverable: Deliverable) -> Self:
        """
        Add a Deliverable to the dispatcher's registry.

        Registers a Deliverable in the dispatcher's pool, making it available for
        dependency injection. The dispatcher only guarantees that each Deliverable
        has a unique UUID, but does not check for duplicate types, producers, or consumers.

        Args:
            __deliverable (Deliverable): The deliverable to register.

        Returns:
            Self: This dispatcher instance for method chaining.
        """
        self.register(__deliverable)

        return self

    def get_deliverable(
        self: Self,
        available_deliverables: list[Deliverable[Any]],
        consumer: type[Factory],
        producer: Union[type[Factory], type[External]],
    ) -> Optional[Deliverable]:
        """
        Find the best matching Deliverable from a list of candidates.

        Uses a multi-step matching strategy to find the correct Deliverable:
        1. If only one candidate exists, validate consumer constraints
        2. Filter by producer to narrow down candidates
        3. If multiple producers match, filter by consumer constraints
        4. Raise exception if multiple ambiguous matches remain

        Args:
            available_deliverables (list[Deliverable[Any]]): List of candidate deliverables.
            consumer (type[Factory]): The factory that will consume the deliverable.
            producer (Union[type[Factory], type[External]]): The expected producer.

        Returns:
            Optional[Deliverable]: The best matching deliverable, or None if no match found.

        Raises:
            InvalidDeliveryException: If consumer constraints are violated or multiple ambiguous matches exist.
        """
        if len(available_deliverables) == 0:
            self.log_debug("Can't find a deliverable")
            return None

        if len(available_deliverables) == 1:
            deliverable = available_deliverables[0]
            if deliverable.consumers and consumer not in deliverable.consumers:
                raise InvalidDeliveryException(
                    f"Can't find {consumer.name()} in consumer list"
                )
            self.log_debug("Find deliverable")

            return deliverable

        matched_by_producer = [
            d for d in available_deliverables if d.producer == producer
        ]

        if len(matched_by_producer) == 0:
            self.log_warning("Can't find any deliverable that matches the producer")
            return None

        if len(matched_by_producer) == 1:
            self.log_debug("Find deliverable")
            return matched_by_producer[0]
        self.log_info("Multiple deliverables with same type and same producer")
        self.log_info("Matching by consumer")
        matched_by_consumer = [
            d
            for d in matched_by_producer
            if len(d.consumers) != 0 and consumer in d.consumers
        ]

        if len(matched_by_consumer) == 0:
            self.log_warning("Can't find any deliverable that matches consumer")
            return None

        if len(matched_by_consumer) == 1:
            self.log_debug("Find deliverable")
            return matched_by_consumer[0]

        raise InvalidDeliveryException(
            "".join(
                [
                    "Find multiple deliveries having the same"
                    "type, producer and consumer"
                ]
            )
        )

    def find_deliverable_by(
        self: Self, predicate: Callable[[Deliverable], bool]
    ) -> list[Deliverable]:
        """
        Find all deliverables that match a given predicate.

        Args:
            predicate (Callable[[Deliverable], bool]): Function to test each deliverable.

        Returns:
            list[Deliverable]: List of deliverables that match the predicate.
        """
        return [v for _, v in self.get_registry().items() if predicate(v)]

    def find_deliverable_by_type(self: Self, delivery_type: type) -> list[Deliverable]:
        """
        Find all deliverables with a specific payload type.

        Args:
            delivery_type (type): The type to match against.

        Returns:
            list[Deliverable]: List of deliverables with the specified type.
        """
        return self.find_deliverable_by(lambda x: x.is_payload_type(delivery_type))

    def find_deliverable_by_name(self: Self, name: str) -> list[Deliverable]:
        """
        Find all deliverables matching a payload type name.

        Args:
            name (str): The type name to match against.

        Returns:
            list[Deliverable]: List of deliverables with the specified type name.
        """
        return self.find_deliverable_by(lambda x: x.has_same_payload_type_name(name))

    def collect_factory_deliverable(self: Self, factory: Factory) -> Self:
        """
        Collect and register the Deliverable from a factory.

        Args:
            factory (Factory): The factory whose deliverable to collect.

        Returns:
            Self: This dispatcher instance for method chaining.
        """
        self.add_deliverable(factory.deliverable)

        return self

    def collect_stage_deliverables(self: Self, stage: Stage) -> Self:
        """
        Collect and register all Deliverables from a stage.

        Args:
            stage (Stage): The stage whose deliverables to collect.

        Returns:
            Self: This dispatcher instance for method chaining.
        """
        list(map(self.add_deliverable, stage.deliverable))

        return self

    def dispatch_delivery(self: Self, consumer: Factory, delivery: Delivery) -> None:
        """
        Inject a value into a specific Delivery slot.

        Finds the appropriate Deliverable from the registry and injects its payload
        into the specified Delivery slot. This is the core dependency injection
        mechanism that enables factories to receive their required inputs.

        Args:
            consumer (Factory): The factory that owns the delivery slot.
            delivery (Delivery): The delivery slot to inject a value into.

        Raises:
            InvalidDeliveryException: If no matching deliverable is found.
        """
        self.log_debug(f"Look for available {delivery.payload_type} in delivery pool")

        available_deliverables = self.find_deliverable_by(
            lambda _: _.is_payload_type(delivery.payload_type)
            and _.delivery_id == delivery.delivery_id
        )

        deliverable: Optional[Deliverable] = self.get_deliverable(
            available_deliverables, consumer=type(consumer), producer=delivery.producer
        )

        if deliverable is None:
            raise InvalidDeliveryException(f"Cannot find type {delivery.payload_type}")

        delivery.set(deliverable.payload)

    def dispatch(self: Self, consumer: Factory) -> Self:
        """
        Inject values into all Delivery slots of a factory.

        This is the main entry point for dependency injection. It processes all
        Delivery dependencies declared by the factory and injects the appropriate
        values from the dispatcher's registry.

        Args:
            consumer (Factory): The factory whose dependencies to inject.

        Returns:
            Self: This dispatcher instance for method chaining.
        """
        list(
            map(
                lambda _: self.dispatch_delivery(consumer, _),
                consumer.expected_deliveries(),
            )
        )

        return self
