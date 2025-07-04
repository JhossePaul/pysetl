"""
Node module for PySetl.

Defines the Node abstraction, representing a node in the workflow DAG. Each Node
corresponds to a Factory (or external input), tracks its unique id, stage, input
dependencies, and output.

Nodes are used for graph construction, dependency resolution, and visualization
of the ETL pipeline.
"""
from __future__ import annotations
from dataclasses import dataclass
from uuid import UUID
from typing import TYPE_CHECKING, Union
from typing_extensions import Self
from pysetl.utils.mixins import HasDiagram, HasLogger, IsIdentifiable
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
    Node in the workflow DAG, representing a Factory or external input.

    Each Node tracks a factory (or external), its unique id, stage, input dependencies (Deliveries),
    and output (NodeOutput). Nodes are used for graph construction, dependency resolution, and
    visualization of the ETL pipeline. One node may have multiple input dependencies and a single output.

    Attributes:
        factory_class (Union[type[Factory], type[External]]): The class of the factory or external input.
        factory_uuid (UUID): Unique identifier for the factory instance.
        stage (int): The stage number in the pipeline.
        expected_deliveries (list[Delivery]): List of input dependencies for this node.
        output (NodeOutput): Output metadata for this node.
    """

    factory_class: Union[type[Factory], type[External]]
    factory_uuid: UUID
    stage: int
    expected_deliveries: list[Delivery]
    output: NodeOutput

    def __post_init__(self: Self) -> None:
        """
        Initialize the Node and its unique identifier (for graph tracking).
        """
        super().__init__()

    def __hash__(self: Self) -> int:
        """
        Return a hash for set operations (based on UUID).

        Returns:
            int: Hash value for the node.
        """
        return self.uuid.int

    @classmethod
    def from_factory(cls, factory: "Factory", stage: int, final: bool) -> "Node":
        """
        Create a Node from a Factory instance.

        Args:
            factory (Factory): The factory instance to represent.
            stage (int): The stage number in the pipeline.
            final (bool): Whether this is the final output node.

        Returns:
            Node: The constructed node representing the factory.
        """
        return Node(
            factory_class=type(factory),
            factory_uuid=factory.uuid,
            stage=stage,
            expected_deliveries=factory.expected_deliveries(),
            output=NodeOutput(
                delivery_type=factory.delivery_type().tp,
                consumer=factory.consumers,
                delivery_id=factory.delivery_id,
                final_output=final,
            ),
        )

    @property
    def input(self) -> list[Delivery]:
        """
        List of input Delivery dependencies for this node.

        Returns:
            list[Delivery]: The declared input dependencies for this node.
        """
        return self.expected_deliveries

    @property
    def diagram_id(self: Self) -> str:
        """
        Return a string identifier for diagram rendering.

        Returns:
            str: Diagram id for this node.
        """
        return pretty(self.factory_class).replace("[]", "_")

    @property
    def producers(self: Self) -> list[Union[type["Factory"], type[External]]]:
        """
        Return a list of input producer factories for this node.

        Returns:
            list[Union[type[Factory], type[External]]]: The producer classes for each input.
        """
        return [delivery.producer for delivery in self.input]

    def find_input_by_type(
        self, payload_type: type, delivery_id: str
    ) -> list[Delivery]:
        """
        Search for input deliveries matching a given type and delivery id.

        Args:
            payload_type (type): The type to match.
            delivery_id (str): The delivery id to match.

        Returns:
            list[Delivery]: Matching input deliveries.
        """
        return [
            delivery
            for delivery in self.input
            if delivery.producer is not None
            and payload_type == delivery.payload_type
            and delivery_id == delivery.delivery_id
        ]

    def __format_delivery_id(self: Self, delivery_id) -> str:
        """
        Format a delivery id for diagram output.

        Args:
            delivery_id (str): The delivery id to format.

        Returns:
            str: Formatted delivery id string.
        """
        return f" (delivery id: {delivery_id})" if delivery_id != "" else ""

    def __format_delivery(self: Self, delivery: Delivery) -> str:
        """
        Format a delivery for diagram or string output.

        Args:
            delivery (Delivery): The delivery to format.

        Returns:
            str: Formatted delivery string.
        """
        delivery_type_str = (
            pretty(delivery.producer.delivery_type())
            if delivery.producer
            else "External"
        )
        delivery_id_str = self.__format_delivery_id(delivery.delivery_id)
        return f"Input: {delivery_type_str}{delivery_id_str}"

    def __str__(self) -> str:
        """
        Return a human-readable string representation of the node.

        Returns:
            str: Description of the node's factory, stage, inputs, and output.
        """
        node_name = pretty(self.factory_class)
        formated_inputs = [self.__format_delivery(delivery) for delivery in self.input]
        input_lines = "\n".join(formated_inputs)
        output_type = pretty(self.output.delivery_type)
        output_delivery_id = self.__format_delivery_id(self.output.delivery_id)
        output_line = f"Output: {output_type} {output_delivery_id}"
        return "\n".join(
            [
                f"Factory: {node_name}",
                f"Stage: {self.stage}",
                input_lines,
                output_line,
                "----------------------------------------------------------",
            ]
        )

    def __repr__(self: Self) -> str:
        """
        Return the string representation for debugging.

        Returns:
            str: Description of the node's factory, stage, inputs, and output.
        """
        return str(self)

    def to_diagram(self) -> str:
        """
        Create a Mermaid diagram representation of the node.

        Returns:
            str: Mermaid diagram string for this node.
        """
        input_types = map(lambda i: f"    +{pretty(i.payload_type)}", self.input)
        input_types_str = "\n".join(input_types)
        this_diagram_string = "\n".join(
            [
                f"class {self.diagram_id} {{",
                f"     <<Factory[{pretty(self.output.delivery_type)}]>>",
                input_types_str,
                "}",
            ]
        )
        output_str = self.output.to_diagram()
        link = f"{self.output.diagram_id} <|.. {self.diagram_id} : Output"
        return f"{this_diagram_string}\n{output_str}\n{link}"

    def target_node(self: Self, next_node: "Node") -> bool:
        """
        Determine if the given node is a valid target (consumer) of this node's output.

        Args:
            next_node (Node): The candidate target node.

        Returns:
            bool: True if the next_node is a valid target, False otherwise.
        """
        valid_node_uuid = self.uuid != next_node.uuid
        valid_class_uuid = self.factory_uuid != next_node.factory_uuid
        valid_stage = self.stage < next_node.stage
        filtered_inputs: list[Delivery] = next_node.find_input_by_type(
            self.output.delivery_type, self.output.delivery_id
        )
        if len(filtered_inputs) == 0:
            valid_target = False
        elif len(filtered_inputs) == 1:
            valid_target = self.__handle_one_matches(filtered_inputs[0], next_node)
        else:
            valid_target = self.__handle_multiple_matches(filtered_inputs, next_node)
        return valid_node_uuid and valid_class_uuid and valid_stage and valid_target

    def __handle_one_matches(self: Self, delivery: Delivery, next_node: "Node") -> bool:
        """
        Helper to validate target when one delivery matches.

        Args:
            delivery (Delivery): The matching delivery.
            next_node (Node): The candidate target node.

        Returns:
            bool: True if the target is valid, False otherwise.
        """
        valid_producer = (
            delivery.producer is External or delivery.producer is self.factory_class
        )
        valid_consumer = (
            len(self.output.consumer) == 0
            or next_node.factory_class in self.output.consumer
        )
        return valid_producer and valid_consumer

    def __handle_multiple_matches(
        self: Self, deliveries: list[Delivery], next_node: "Node"
    ) -> bool:
        """
        Helper to validate target when multiple deliveries match.

        Args:
            deliveries (list[Delivery]): The matching deliveries.
            next_node (Node): The candidate target node.

        Returns:
            bool: True if the target is valid, False otherwise.

        Raises:
            InvalidDeliveryException: If multiple ambiguous matches are found.
        """
        valid_consumer = (
            len(self.output.consumer) == 0
            or next_node.factory_class in self.output.consumer
        )
        exact_produced_match = any(
            delivery.producer == self.factory_class for delivery in deliveries
        )
        non_explicitly_defined_producers = [
            delivery for delivery in deliveries if delivery.producer is External
        ]
        if not exact_produced_match and len(non_explicitly_defined_producers) > 1:
            raise InvalidDeliveryException(
                f"""
            Multiple inputs in {pretty(type(next_node))} are of type
            {self.output.delivery_type}. You may have to explicitly declare
            their producer in the Delivery annotation, otherwise this may cause
            unexpected pipeline result.
            """
            )
        valid_producer = (
            exact_produced_match or len(non_explicitly_defined_producers) == 1
        )
        return valid_consumer and valid_producer

    @classmethod
    def external(cls, output: NodeOutput) -> "Node":
        """
        Create a Node representing an external source.

        Args:
            output (NodeOutput): The output metadata for the external node.

        Returns:
            Node: The constructed external node.
        """
        return Node(
            External, UUID("{00000000-0000-0000-0000-000000000000}"), -1, [], output
        )

    def find_input_by_delivery_id(self: Self, delivery_id: str) -> list[Delivery]:
        """
        Search for input deliveries matching a given delivery id.

        Args:
            delivery_id (str): The delivery id to match.

        Returns:
            list[Delivery]: Matching input deliveries.
        """
        return [
            delivery for delivery in self.input if delivery.delivery_id == delivery_id
        ]
