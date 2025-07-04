"""
DAG module for PySetl.

Defines the DAG (Directed Acyclic Graph) class, which is a mathematical representation
of the workflow structure. The DAG ensures that information flow is acyclic and directed,
guaranteeing that the pipeline can execute correctly without circular dependencies.

The DAG consists of nodes (factories) and flows (data transfers), providing both
structural validation and visualization capabilities for the ETL pipeline.
"""
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
    A Directed Acyclic Graph representing the workflow structure.

    A DAG is a mathematical structure that represents the flow of information in a process.
    It must be acyclic (no circular dependencies) and directed (clear flow direction)
    to ensure that information flow can be resolved correctly during pipeline execution.

    Each node in the graph represents a Factory, and each flow represents a data transfer
    between factories. The DAG structure enables dependency resolution, execution ordering,
    and visualization of the complete ETL pipeline.

    Attributes:
        nodes (set[Node]): Set of nodes representing factories in the workflow.
        flows (set[Flow]): Set of flows representing data transfers between nodes.
    """

    nodes: set[Node]
    flows: set[Flow]

    def find_deliveries(self: Self, factory: Factory) -> list[Delivery]:
        """
        Find all deliveries from a given factory in the DAG nodes.

        Searches through all nodes to find deliveries that belong to the specified factory,
        identified by the factory's UUID.

        Args:
            factory (Factory): The factory whose deliveries to find.

        Returns:
            list[Delivery]: List of deliveries from the specified factory.
        """
        return [
            delivery
            for node in self.nodes
            for delivery in node.expected_deliveries
            if node.factory_uuid == factory.uuid
        ]

    def to_diagram(self: Self) -> str:
        """
        Create a Mermaid diagram representation of the complete DAG.

        Generates a comprehensive diagram showing all nodes and their relationships,
        formatted for Mermaid diagram rendering.

        Returns:
            str: Complete Mermaid diagram string for the DAG.
        """
        node_diagrams = "\n".join([n.to_diagram() for n in self.nodes])
        flow_diagrams = "\n".join([f.to_diagram() for f in self.flows])

        return "\n".join(["classDiagram", node_diagrams, flow_diagrams])

    @property
    def diagram_id(self: Self) -> str:
        """
        DAG doesn't have a diagram ID (it's the container, not a diagram element).

        Raises:
            NotImplementedError: DAG doesn't have a diagram ID.
        """
        raise NotImplementedError("DAG doesn't have diagram id")

    def __str__(self: Self) -> str:
        """
        Get a comprehensive string representation of the DAG.

        Returns:
            str: Detailed summary showing all nodes and flows in the DAG.
        """
        empty_nodes_string = "Empty\n" if len(self.nodes) == 0 else ""
        empty_flows_string = "Empty\n" if len(self.flows) == 0 else ""
        nodes_string = "\n".join(
            [str(n) for n in sorted(list(self.nodes), key=lambda x: x.stage)]
        )
        flows_string = "\n".join(
            [str(f) for f in sorted(list(self.flows), key=lambda x: x.stage)]
        )

        return "\n".join(
            [
                "-------------   Data Transformation Summary  -------------",
                empty_nodes_string,
                nodes_string,
                "------------------   Data Flow Summary  ------------------",
                empty_flows_string,
                flows_string,
            ]
        )

    def __repr__(self: Self) -> str:
        """
        Get the string representation for debugging.

        Returns:
            str: Detailed summary of the DAG structure.
        """
        return self.__str__()
