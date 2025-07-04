"""Inspector module for PySetl.

Defines the Inspector class, which is the sophisticated DAG construction and validation system
for pipelines. The Inspector analyzes a pipeline's structure, constructs a Directed Acyclic Graph
(DAG) with nodes (factories) and flows (data transfers), and validates that the execution process
is valid and can complete successfully.

The Inspector performs complex analysis to distinguish between internal flows (between factories)
and external flows (from external sources), ensuring that all dependencies can be satisfied and
that the pipeline structure is acyclic and executable.

Key responsibilities:
- Construct nodes from pipeline stages and factories
- Create internal flows between factories across stages
- Identify and create external flows for external dependencies
- Validate that the resulting DAG is acyclic and executable
- Provide visualization and analysis capabilities
"""
from __future__ import annotations
from uuid import UUID
from itertools import groupby
from typing import TYPE_CHECKING, Optional
from typing_extensions import Self
from pysetl.utils.mixins import HasLogger
from pysetl.utils.exceptions import PipelineException
from .dag import DAG
from .node import Node
from .flow import Flow
from .delivery import Delivery
from .external import External
from .node_output import NodeOutput


if TYPE_CHECKING:
    from .factory import Factory  # pragma: no cover
    from .pipeline import Pipeline  # pragma: no cover


class Inspector(HasLogger):
    """
    Sophisticated DAG construction and validation system for pipelines.

    The Inspector analyzes pipeline structure and constructs a Directed Acyclic Graph (DAG)
    that represents the complete data flow and dependency relationships. It performs complex
    validation to ensure the pipeline can execute successfully by distinguishing between
    internal flows (between factories) and external flows (from external sources).

    The Inspector's core functionality involves:

    1. **Node Construction**: Creates nodes from all factories in the pipeline stages
    2. **Internal Flow Detection**: Identifies data flows between factories across stages
    3. **External Flow Resolution**: Creates flows from external sources when internal flows
       cannot satisfy all dependencies
    4. **DAG Validation**: Ensures the resulting graph is acyclic and executable
    5. **Execution Validation**: Verifies that all dependencies can be satisfied

    The Inspector uses sophisticated logic to handle ambiguous dependencies and ensure
    that external inputs are only used when internal flows cannot provide the required data.

    Attributes:
        pipeline (Pipeline): The pipeline to inspect and analyze.
        nodes (set[Node]): Set of nodes representing factories in the DAG.
        flows (set[Flow]): Set of flows representing data transfers in the DAG.
    """

    def __init__(self: Self, pipeline: Pipeline):
        """
        Initialize the Inspector with a pipeline to analyze.

        Args:
            pipeline (Pipeline): The pipeline to inspect and construct a DAG for.
        """
        self.pipeline = pipeline
        self.nodes: set[Node] = self.create_nodes()
        self.flows: set[Flow] = self.create_flows()

    @property
    def graph(self: Self) -> DAG:
        """
        Get the constructed DAG representing the pipeline structure.

        Returns:
            DAG: The directed acyclic graph with nodes and flows.
        """
        return DAG(self.nodes, self.flows)

    def find_node(self: Self, factory: Factory) -> Optional[Node]:
        """
        Find a node by its associated factory UUID.

        Args:
            factory (Factory): The factory to find the node for.

        Returns:
            Optional[Node]: The node representing the factory, or None if not found.

        Raises:
            PipelineException: If multiple nodes are found for the same factory UUID.
        """
        found_nodes = [n for n in self.nodes if n.factory_uuid == factory.uuid]

        if len(found_nodes) == 0:
            return None

        if len(found_nodes) == 1:
            return found_nodes[0]

        raise PipelineException("Multiple factories found")

    def create_nodes(self: Self) -> set[Node]:
        """
        Create nodes from all factories in the pipeline stages.

        Iterates through all stages and creates nodes for each factory, establishing
        the basic structure of the DAG.

        Returns:
            set[Node]: Set of nodes representing all factories in the pipeline.
        """
        return {n for s in self.pipeline.stages for n in s.create_nodes()}

    def find_internal_flows(self: Self, factory: Factory) -> list[Flow]:
        """
        Find internal flows from a factory to downstream consumers.

        Identifies all valid data flows from the given factory to other factories
        in later stages that can consume its output. This establishes the internal
        data flow network within the pipeline.

        Args:
            factory (Factory): The source factory to find flows from.

        Returns:
            list[Flow]: List of flows from the factory to its consumers.
        """
        parent_node = self.find_node(factory=factory)

        return [
            Flow(parent_node, target_node)
            for target_node in self.nodes
            if parent_node is not None
            and target_node.stage > parent_node.stage
            and parent_node.target_node(target_node)
        ]

    def create_internal_flows(self: Self) -> set[Flow]:
        """
        Create all internal flows between factories across stages.

        Generates the complete set of internal flows by examining all factories
        in all stages (except the final stage) and creating flows to their
        downstream consumers.

        Returns:
            set[Flow]: Set of all internal flows between factories.
        """
        return {
            flow
            for stage in self.pipeline.stages
            for factory in stage.factories
            for flow in self.find_internal_flows(factory)
            if not stage.end
        }

    def possible_internal(
        self: Self, internal_flows: set[Flow], delivery: Delivery, factory_uuid: UUID
    ) -> int:
        """
        Count potential internal flows that could satisfy a delivery dependency.

        This method is crucial for determining whether a delivery dependency can be
        satisfied by internal flows or requires an external source. It counts how
        many internal flows could potentially provide the required data type and
        delivery ID to the specified factory.

        Args:
            internal_flows (set[Flow]): Set of all internal flows in the pipeline.
            delivery (Delivery): The delivery dependency to check.
            factory_uuid (UUID): UUID of the factory that needs the delivery.

        Returns:
            int: Number of potential internal flows that could satisfy the dependency.
        """
        return len(
            [
                f
                for f in internal_flows
                if f.payload == delivery.payload_type
                and f.to_node.factory_uuid == factory_uuid
                and f.delivery_id == delivery.delivery_id
            ]
        )

    def deliveries_with_same_id(
        self: Self, deliveries: list[Delivery], delivery_id: str
    ) -> int:
        """
        Count deliveries with the same delivery ID.

        Used to understand how many dependencies exist for a specific delivery ID,
        which helps determine whether external flows are needed.

        Args:
            deliveries (list[Delivery]): List of deliveries to check.
            delivery_id (str): The delivery ID to count.

        Returns:
            int: Number of deliveries with the specified delivery ID.
        """
        return len([d for d in deliveries if d.delivery_id == delivery_id])

    def find_external_flows(
        self: Self, node: Node, internal_flows: set[Flow]
    ) -> list[Flow]:
        """
        Find flows from external sources that are needed to satisfy dependencies.

        This is the most complex part of the Inspector's logic. It determines when
        external flows are necessary by analyzing whether internal flows can satisfy
        all dependencies. The key insight is that external flows are only created
        when internal flows cannot provide all the required data.

        The logic works as follows:

        1. Group deliveries by payload type to understand dependencies
        2. For each group, check if any deliveries have External as producer
        3. For external deliveries, verify that internal flows cannot satisfy all
           dependencies with the same delivery ID
        4. Create external flows only when internal flows are insufficient

        Args:
            node (Node): The node to find external flows for.
            internal_flows (set[Flow]): Set of all internal flows for comparison.

        Returns:
            list[Flow]: List of external flows needed for this node.
        """
        grouped_deliveries = [
            list(deliveries)
            for _, deliveries in groupby(node.input, lambda _: _.payload_type)
        ]

        return [
            Flow(
                Node.external(
                    output=NodeOutput(
                        delivery_type=delivery.payload_type,
                        consumer=set(),
                        delivery_id=delivery.delivery_id,
                        external=True,
                    )
                ),
                node,
            )
            for deliveries in grouped_deliveries
            for delivery in deliveries
            if (delivery.producer is External)
            and (
                self.possible_internal(internal_flows, delivery, node.factory_uuid)
                == (self.deliveries_with_same_id(deliveries, delivery.delivery_id) - 1)
            )
        ]

    def create_external_flows(self: Self, internal_flows: set[Flow]) -> set[Flow]:
        """
        Create all external flows needed across the entire pipeline.

        Iterates through all nodes and creates external flows where internal flows
        cannot satisfy all dependencies. This ensures that all dependencies can be
        satisfied either internally or externally.

        Args:
            internal_flows (set[Flow]): Set of all internal flows for comparison.

        Returns:
            set[Flow]: Set of all external flows needed in the pipeline.
        """
        return {
            flow
            for node in self.nodes
            for flow in self.find_external_flows(node, internal_flows)
        }

    def create_flows(self: Self) -> set[Flow]:
        """
        Create the complete set of flows (internal + external) for the DAG.

        This is the main method that orchestrates flow creation. It first creates
        all internal flows, then determines what external flows are needed to
        satisfy remaining dependencies.

        Returns:
            set[Flow]: Complete set of flows representing all data transfers in the pipeline.
        """
        internal_flows = self.create_internal_flows()
        external_flows = self.create_external_flows(internal_flows)

        return internal_flows.union(external_flows)

    def inspect(self: Self) -> Self:
        """
        Perform complete inspection and DAG construction.

        This is the main entry point that reconstructs the nodes and flows,
        ensuring the DAG is up-to-date with the current pipeline state.

        Returns:
            Self: This inspector instance for method chaining.
        """
        self.nodes = self.create_nodes()
        self.flows = self.create_flows()

        return self

    def __str__(self: Self) -> str:
        """
        Get a comprehensive string representation of the pipeline analysis.

        Returns:
            str: Detailed summary of the pipeline structure and data flows.
        """
        return f"========== Pipeline Summary ==========\n\n{self.graph}"

    def __repr__(self: Self) -> str:
        """
        Get the string representation for debugging.

        Returns:
            str: Detailed summary of the pipeline structure and data flows.
        """
        return str(self)
