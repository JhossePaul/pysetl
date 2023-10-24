"""Deliverable Dispatcher module."""
from __future__ import annotations
from typing import TYPE_CHECKING, Callable, Any, Optional, Union
from typing_extensions import Self
from pysetl.utils.mixins import HasRegistry, HasLogger
from pysetl.utils.exceptions import (
    InvalidDeliveryException, NoDeliverableException
)
from .deliverable import Deliverable
from .stage import Stage
from .delivery import Delivery
from .external import External


if TYPE_CHECKING:  # pragma: no cover
    from .factory import Factory


class Dispatcher(HasRegistry[Deliverable], HasLogger):
    """
    Transfer data between different stages.

    Relies on a Directed Acyclic Graph (DAG). It can:
    - collect a Deliverable from a Factory
    - find the right Deliverable from its pool and send it to a Factory
    """

    def add_deliverable(self: Self, __deliverable: Deliverable) -> Self:
        """
        Add a new Deliverable object into Dispatcher's delivery pool.

        Attention: Dispatcher only guarantee that each deliverable object has
        different UUID. In other words, cannot call twice set_delivery with
        the same argument. However, it doesn't check if multiple deliverables
        have the same data type with the same producer and the same consumer
        """
        self.register(__deliverable)

        return self

    def get_deliverable(
            self: Self,
            available_deliverables: list[Deliverable[Any]],
            consumer: type[Factory],
            producer: Union[type[Factory], type[External]]
            ) -> Optional[Deliverable]:
        """
        Find the Deliverable in pool with the given type.

        @param available_deliverables an array of available deliverable
        """
        if len(available_deliverables) == 0:
            self.log_debug("Can't find a deliverable")
            return None
        elif len(available_deliverables) == 1:
            deliverable = available_deliverables[0]
            if (
                deliverable.consumers and
                consumer not in deliverable.consumers
            ):
                raise InvalidDeliveryException(
                    f"Can't find {consumer.name()} in consumer list"
                )
            self.log_debug("Find deliverable")

            return deliverable
        else:
            matched_by_producer = [
                d
                for d
                in available_deliverables
                if d.producer == producer
            ]

            if len(matched_by_producer) == 0:
                self.log_warning(
                    "Can't find any deliverable that matches the producer"
                )
                return None
            elif len(matched_by_producer) == 1:
                self.log_debug("Find deiliverable")
                return matched_by_producer[0]
            else:
                self.log_info(
                    "Multiple deliverables with same type and same producer"
                )
                self.log_info("Matching by consumer")
                matched_by_consumer = [
                    d
                    for d
                    in matched_by_producer
                    if len(d.consumers) != 0 and
                    consumer in d.consumers
                ]

                if len(matched_by_consumer) == 0:
                    self.log_warning(
                        "Can't find any deliverable that matches consumer"
                    )
                    return None
                elif len(matched_by_consumer) == 1:
                    self.log_debug("Find deliverable")
                    return matched_by_consumer[0]
                else:
                    raise InvalidDeliveryException("".join([
                        "Find multiple deliveries having the same"
                        "type, producer and consumer"
                    ]))

    def find_deliverable_by(
            self: Self,
            predicate: Callable[[Deliverable], bool]) -> list[Deliverable]:
        """Find all deliverables that passes a given predicate."""
        return [
            v
            for _, v
            in self.get_registry().items()
            if predicate(v)
        ]

    def find_deliverable_by_type(
            self: Self,
            delivery_type: type) -> list[Deliverable]:
        """
        Get all Deliverable by payload type.

        @param delivery_type of data
        """
        return self.find_deliverable_by(
            lambda x: x.is_payload_type(delivery_type)
        )

    def find_deliverable_by_name(
            self: Self,
            name: str) -> list[Deliverable]:
        """Find all deliverables matching payload type name."""
        return self.find_deliverable_by(
            lambda x: x.has_same_payload_type_name(name)
        )

    def collect_factory_deliverable(self: Self, factory: Factory) -> Self:
        """Collect and registers the Deliverable from a Factory."""
        self.add_deliverable(factory.deliverable)

        return self

    def collect_stage_deliverables(self: Self, stage: Stage) -> Self:
        """Collect and registers the Deliverable from a Stage."""
        list(map(self.add_deliverable, stage.deliverable))

        return self

    def dispatch_delivery(
            self: Self,
            consumer: Factory,
            delivery: Delivery) -> None:
        """
        Find delivery by type and id.

        Given a consumer and a delivery, finds a delivery from deliverable pool
        with same type and id.
        """
        self.log_debug(
            f"Look for available {delivery.payload_type} in delivery pool"
        )

        available_deliverables = self.find_deliverable_by(
            lambda _: _.is_payload_type(delivery.payload_type) and
            _.delivery_id == delivery.delivery_id
        )

        deliverable: Optional[Deliverable] = self.get_deliverable(
            available_deliverables,
            consumer=type(consumer),
            producer=delivery.producer
        )

        if deliverable is None:
            raise NoDeliverableException(
                f"Cannot find type {delivery.payload_type}"
            )
        else:
            delivery.set(deliverable.payload)

    def dispatch(self: Self, consumer: Factory) -> Self:
        """
        Dispath deliverable.

        Dispatch the right deliverable object to the corresponding Delivery
        instances of a factory.
        """
        list(map(
            lambda _: self.dispatch_delivery(consumer, _),
            consumer.expected_deliveries()
        ))

        return self
