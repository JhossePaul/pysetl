"""DAG module."""
from __future__ import annotations
from dataclasses import dataclass
from typing_extensions import Self, TYPE_CHECKING
from pysetl.utils.mixins import HasDiagram
from .flow import Flow
from .node import Node
from .delivery import Delivery


if TYPE_CHECKING:
    from .factory import Factory  # pragma: no cover


@dataclass
class DAG(HasDiagram):
    """
    A Directed Acyclig Graph.

    A DAG is a way to represent the flow of information in a process. It should
    be acyclic and direct in order to ensure that information flow will solve
    correctly. Each node in the graph represents a Factory and each Factory
    returns a Deliverable. Factories can recycle information from parent nodes
    and dispatch the Deliverable as a Delivery to child nodes.
    """

    nodes: set[Node]
    flows: set[Flow]

    def find_deliveries(self: Self, factory: Factory) -> list[Delivery]:
        """Find deliveries from a given Factory in the nodes of the DAG."""
        return [
            delivery
            for node
            in self.nodes
            for delivery
            in node.expected_deliveries
            if node.factory_uuid == factory.uuid
        ]

    def to_diagram(self: Self) -> str:
        """Create mermaid diagram for DAG."""
        node_diagrams = "\n".join([n.to_diagram() for n in self.nodes])
        flow_diagrams = "\n".join([f.to_diagram() for f in self.flows])

        return "\n".join([
            "classDiagram",
            node_diagrams,
            flow_diagrams
        ])

    @property
    def diagram_id(self: Self) -> str:
        """Raise an error beacause it is not implemented."""
        raise NotImplementedError("DAG doesn't have diagram id")

    def __str__(self: Self) -> str:
        """Customize str method."""
        empty_nodes_string = "Empty\n" if len(self.nodes) == 0 else ""
        empty_flows_string = "Empty\n" if len(self.flows) == 0 else ""
        nodes_string = "\n".join([
            str(n)
            for n
            in sorted(list(self.nodes), key=lambda x: x.stage)
        ])
        flows_string = "\n".join([
            str(f)
            for f
            in sorted(list(self.flows), key=lambda x: x.stage)
        ])

        return "\n".join([
            "-------------   Data Transformation Summary  -------------",
            empty_nodes_string,
            nodes_string,
            "------------------   Data Flow Summary  ------------------",
            empty_flows_string,
            flows_string,
        ])

    def __repr__(self: Self) -> str:
        """Customize repr method."""
        return self.__str__()
