"""Flow module."""
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
    Flow is a representation of the data transfer in a Pipeline.

    @param from origin node of the transfer
    @param to   destination node of the transfer
    """

    from_node: Node
    to_node: Node

    def __post_init__(self: Self) -> None:
        IsIdentifiable.__init__(self)

    def __hash__(self: Self) -> int:
        return self.uuid.int

    @property
    def stage(self: Self) -> int:
        """Expose stage from NodeInput."""
        return self.from_node.stage

    @property
    def delivery_id(self: Self) -> str:
        """Expose delivery_id from NodeInput."""
        return self.from_node.output.delivery_id

    def to_diagram(self: Self) -> str:
        """Create mermaid diagram."""
        return (
            f"{self.to_node.diagram_id}<|--" +
            f"{self.from_node.output.diagram_id}:Input"
        )

    @property
    def diagram_id(self: Self) -> str:
        """Flow has no diagram id."""
        return ""

    def __str__(self: Self) -> str:
        """Customize str method."""
        from_factory = pretty(self.from_node.factory_class)
        to_factory = pretty(self.to_node.factory_class)

        return f"Flow[{from_factory} -> {to_factory}]"

    def __repr__(self: Self) -> str:
        """Customize repr method."""
        delivery_id_string = (
            f"Delivery id : {self.delivery_id}"
            if self.delivery_id != ""
            else ""
        )
        from_factory = pretty(self.from_node.factory_class)
        to_factory = pretty(self.to_node.factory_class)

        return textwrap.dedent(f"""
        {delivery_id_string}
        Stage       : {self.stage}
        Direction   : {from_factory} ==> {to_factory}
        PayLoad     : {pretty(self.payload)}
        ----------------------------------------------------------
        """).strip()

    @property
    def payload(self: Self) -> type:
        """Expose payload output from parent node."""
        return self.from_node.output.delivery_type
