"""
Flow module for PySetl.

Defines the Flow class, which represents a data transfer connection between two nodes
in the pipeline DAG. A Flow connects a source node's output to a destination node's
input, enabling the visualization and understanding of data flow patterns in the ETL pipeline.

Flows are created by the inspector during DAG construction and are used for both
visualization (Mermaid diagrams) and understanding the data transformation pipeline.
"""
from __future__ import annotations
import textwrap
from dataclasses import dataclass
from typing_extensions import Self
from pysetl.utils.mixins import HasDiagram, IsIdentifiable
from pysetl.utils import pretty
from .node import Node


@dataclass
class Flow(IsIdentifiable, HasDiagram):
    """
    Representation of data transfer between two nodes in the pipeline DAG.

    A Flow connects a source node's output to a destination node's input, representing
    the transfer of data from one factory to another. Flows are created by the inspector
    during DAG construction and are used for visualization and understanding data flow patterns.

    Each Flow has a unique identifier and contains references to both the source and
    destination nodes, along with metadata about the stage, delivery ID, and payload type.

    Attributes:
        from_node (Node): The source node that produces the data.
        to_node (Node): The destination node that consumes the data.
    """

    from_node: Node
    to_node: Node

    def __post_init__(self: Self) -> None:
        """
        Initialize the Flow and its unique identifier for tracking.
        """
        IsIdentifiable.__init__(self)

    def __hash__(self: Self) -> int:
        """
        Return a hash for set operations (based on UUID).

        Returns:
            int: Hash value for the flow.
        """
        return self.uuid.int

    @property
    def stage(self: Self) -> int:
        """
        Get the stage number from the source node.

        Returns:
            int: The stage number where this flow originates.
        """
        return self.from_node.stage

    @property
    def delivery_id(self: Self) -> str:
        """
        Get the delivery ID from the source node's output.

        Returns:
            str: The delivery ID for this flow, or empty string if none.
        """
        return self.from_node.output.delivery_id

    def to_diagram(self: Self) -> str:
        """
        Create a Mermaid diagram representation of this flow.

        Generates a relationship diagram showing the connection from the destination
        node to the source node's output.

        Returns:
            str: Mermaid diagram string for this flow.
        """
        return (
            f"{self.to_node.diagram_id}<|--"
            + f"{self.from_node.output.diagram_id}:Input"
        )

    @property
    def diagram_id(self: Self) -> str:
        """
        Flow has no diagram ID (it's a relationship, not a class).

        Returns:
            str: Empty string since flows don't have diagram IDs.
        """
        return ""

    def __str__(self: Self) -> str:
        """
        Get a concise string representation of this flow.

        Returns:
            str: Simple flow description showing source and destination factories.
        """
        from_factory = pretty(self.from_node.factory_class)
        to_factory = pretty(self.to_node.factory_class)

        return f"Flow[{from_factory} -> {to_factory}]"

    def __repr__(self: Self) -> str:
        """
        Get a detailed string representation of this flow.

        Returns:
            str: Detailed flow description including delivery ID, stage, direction, and payload.
        """
        delivery_id_string = (
            f"Delivery id : {self.delivery_id}" if self.delivery_id != "" else ""
        )
        from_factory = pretty(self.from_node.factory_class)
        to_factory = pretty(self.to_node.factory_class)

        return textwrap.dedent(
            f"""
        {delivery_id_string}
        Stage       : {self.stage}
        Direction   : {from_factory} ==> {to_factory}
        PayLoad     : {pretty(self.payload)}
        ----------------------------------------------------------
        """
        ).strip()

    @property
    def payload(self: Self) -> type:
        """
        Get the payload type from the source node's output.

        Returns:
            type: The type of data being transferred in this flow.
        """
        return self.from_node.output.delivery_type
