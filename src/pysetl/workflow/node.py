"""Node class."""
from __future__ import annotations
from dataclasses import dataclass
from uuid import UUID
from typing import TYPE_CHECKING, Union
from typing_extensions import Self
from pysetl.utils.mixins import (
    HasDiagram, HasLogger,
    IsIdentifiable
)
from pysetl.utils.exceptions import InvalidDeliveryException
from pysetl.utils import pretty
from .delivery import Delivery
from .node_output import NodeOutput
from .external import External


if TYPE_CHECKING:  # pragma: no cover
    from .factory import Factory


@dataclass
class Node(IsIdentifiable, HasDiagram, HasLogger):
    """
    Node is a representation of Factory in the DAG.

    One node could have multiple inputs and one single output.
    """

    factory_class: Union[type[Factory], type[External]]
    factory_uuid: UUID
    stage: int
    expected_deliveries: list[Delivery]
    output: NodeOutput

    def __post_init__(self: Self) -> None:
        """Initialize UUID from IsIdentifiable."""
        super().__init__()

    def __hash__(self: Self) -> int:
        """Hash for set operations."""
        return self.uuid.int

    @classmethod
    def from_factory(
            cls,
            factory: Factory,
            stage: int,
            final: bool) -> Node:
        """Create a node from a Factory."""
        return Node(
            factory_class=type(factory),
            factory_uuid=factory.uuid,
            stage=stage,
            expected_deliveries=factory.expected_deliveries(),
            output=NodeOutput(
                delivery_type=factory.delivery_type(),
                consumer=factory.consumers,
                delivery_id=factory.delivery_id,
                final_output=final
            )
        )

    @property
    def input(self) -> list[Delivery]:
        """
        List of NodeInputs.

        This should be metadata of input factories.
        """
        return self.expected_deliveries

    @property
    def diagram_id(self: Self) -> str:
        """Return string with diagram id."""
        return pretty(self.factory_class).replace("[]", "_")

    @property
    def producers(self: Self) -> list[Union[type[Factory], type[External]]]:
        """Return a list of input producer factories."""
        return [delivery.producer for delivery in self.input]

    def find_input_by_type(
            self,
            payload_type: type,
            delivery_id: str) -> list[Delivery]:
        """Search inside input nodes for a given factory class."""
        return [
            delivery
            for delivery
            in self.input
            if delivery.producer is not None and
            payload_type == delivery.payload_type and
            delivery_id == delivery.delivery_id
        ]

    def __format_delivery_id(self: Self, delivery_id) -> str:
        """Format Delivery ID to print in diagram."""
        return f" (delivery id: {delivery_id})" if delivery_id != "" else ""

    def __format_delivery(self: Self, delivery: Delivery) -> str:
        delivery_type_str = (
            pretty(delivery.producer.delivery_type())
            if delivery.producer
            else "External"
        )
        delivery_id_str = self.__format_delivery_id(delivery.delivery_id)

        return f"Input: {delivery_type_str}{delivery_id_str}"

    def __str__(self) -> str:
        """Describe the node contents."""
        node_name = pretty(self.factory_class)
        formated_inputs = [
            self.__format_delivery(delivery)
            for delivery
            in self.input
        ]
        input_lines = "\n".join(formated_inputs)
        output_type = pretty(self.output.delivery_type)
        output_delivery_id = self.__format_delivery_id(
            self.output.delivery_id
        )
        output_line = f"Output: {output_type} {output_delivery_id}"

        return "\n".join([
            f"Factory: {node_name}",
            f"Stage: {self.stage}",
            input_lines,
            output_line,
            "----------------------------------------------------------"
        ])

    def __repr__(self: Self) -> str:
        """Cutomize repr method."""
        return str(self)

    def to_diagram(self) -> str:
        """Create mermaid diagram."""
        input_types = map(
            lambda i: f"    +{pretty(i.payload_type)}",
            self.input
        )
        input_types_str = "\n".join(input_types)

        this_diagram_string = "\n".join([
            f"class {self.diagram_id} {{",
            f"     <<Factory[{pretty(self.output.delivery_type)}]>>",
            input_types_str,
            "}"
        ])

        output_str = self.output.to_diagram()
        link = f"{self.output.diagram_id} <|.. {self.diagram_id} : Output"

        return f"{this_diagram_string}\n{output_str}\n{link}"

    def target_node(self: Self, next_node: Node) -> bool:
        """
        Return true if it is a target node of the current node.

        False otherwise.
        """
        valid_node_uuid = self.uuid != next_node.uuid
        valid_class_uuid = self.factory_uuid != next_node.factory_uuid
        valid_stage = self.stage < next_node.stage

        filtered_inputs: list[Delivery] = next_node.find_input_by_type(
            self.output.delivery_type,
            self.output.delivery_id
        )

        if len(filtered_inputs) == 0:
            valid_target = False
        elif len(filtered_inputs) == 1:
            valid_target = self.__handle_one_matches(
                filtered_inputs[0],
                next_node
            )
        else:
            valid_target = self.__handle_multiple_matches(
                filtered_inputs,
                next_node
            )

        return (
            valid_node_uuid and
            valid_class_uuid and
            valid_stage and
            valid_target
        )

    def __handle_one_matches(
            self: Self,
            delivery: Delivery,
            next_node: Node) -> bool:
        """Define helper to validate target when one delivery matches."""
        valid_producer = (
            delivery.producer is External or
            delivery.producer is self.factory_class
        )
        valid_consumer = (
            len(self.output.consumer) == 0 or
            next_node.factory_class in self.output.consumer
        )

        return valid_producer and valid_consumer

    def __handle_multiple_matches(
            self: Self,
            deliveries: list[Delivery],
            next_node: Node) -> bool:
        """Define helper to validate target when multiple deliveries match."""
        valid_consumer = (
            len(self.output.consumer) == 0 or
            next_node.factory_class in self.output.consumer
        )

        exact_produced_match = any(
            delivery.producer == self.factory_class
            for delivery
            in deliveries
        )
        non_explicitly_defined_producers = [
            delivery
            for delivery
            in deliveries
            if delivery.producer is External
        ]

        if (
            not exact_produced_match and
            len(non_explicitly_defined_producers) > 1
        ):
            raise InvalidDeliveryException(f"""
            Multiple inputs in {pretty(type(next_node))} are of type
            {self.output.delivery_type}. You may have to explicitly declare
            their producer in the Delivery annotation, otherwise this may cause
            unexpected pipeline result.
            """)

        valid_producer = (
            exact_produced_match or
            len(non_explicitly_defined_producers) == 1
        )

        return valid_consumer and valid_producer

    @classmethod
    def external(cls, output: NodeOutput) -> Node:
        """Create an external source node."""
        return Node(
            External,
            UUID("{00000000-0000-0000-0000-000000000000}"),
            -1,
            [],
            output
        )

    def find_input_by_delivery_id(
            self: Self,
            delivery_id: str) -> list[Delivery]:
        """Search inside input nodes for a given delivery id."""
        return [
            delivery
            for delivery
            in self.input
            if delivery.delivery_id == delivery_id
        ]
