"""Inspector module."""
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
    Inspector for a Pipeline.

    Inspector will inspect a given Pipeline and create a Directed Acyclic Graph
    (DAG) with nodes (factories) and flows (data transfer flows)
    """

    def __init__(self: Self, pipeline: Pipeline):
        self.pipeline = pipeline
        self.nodes: set[Node] = self.create_nodes()
        self.flows: set[Flow] = self.create_flows()

    @property
    def graph(self: Self) -> DAG:
        return DAG(self.nodes, self.flows)

    def find_node(self: Self, factory: Factory) -> Optional[Node]:
        found_nodes = [n for n in self.nodes if n.factory_uuid == factory.uuid]

        if len(found_nodes) == 0:
            return None
        elif len(found_nodes) == 1:
            return found_nodes[0]
        else:
            raise PipelineException("Multiple factories found")

    def create_nodes(self: Self) -> set[Node]:
        return set([n for s in self.pipeline.stages for n in s.create_nodes()])

    def find_internal_flows(self: Self, factory: Factory) -> list[Flow]:
        parent_node = self.find_node(factory=factory)

        return [
            Flow(parent_node, target_node)
            for target_node
            in self.nodes
            if parent_node is not None and
            target_node.stage > parent_node.stage and
            parent_node.target_node(target_node)
        ]

    def create_internal_flows(self: Self) -> set[Flow]:
        return set([
            flow
            for stage
            in self.pipeline.stages
            for factory
            in stage.factories
            for flow
            in self.find_internal_flows(factory)
            if not stage.end
        ])

    def possible_internal(
            self: Self,
            internal_flows: set[Flow],
            delivery: Delivery,
            factory_uuid: UUID
            ) -> int:
        return len([
            f
            for f
            in internal_flows
            if f.payload == delivery.payload_type and
            f.to_node.factory_uuid == factory_uuid and
            f.delivery_id == delivery.delivery_id
        ])

    def deliveries_with_same_id(
            self: Self,
            deliveries: list[Delivery],
            delivery_id: str) -> int:
        return len([d for d in deliveries if d.delivery_id == delivery_id])

    def find_external_flows(
            self: Self,
            node: Node,
            internal_flows: set[Flow]) -> list[Flow]:
        grouped_deliveries = [
            list(deliveries)
            for _, deliveries
            in groupby(node.input, lambda _: _.payload_type)
        ]

        return [
            Flow(
                Node.external(output=NodeOutput(
                    delivery_type=delivery.payload_type,
                    consumer=set(),
                    delivery_id=delivery.delivery_id,
                    external=True
                )),
                node
            )
            for deliveries
            in grouped_deliveries
            for delivery
            in deliveries
            if (delivery.producer is External) and (
                self.possible_internal(
                    internal_flows,
                    delivery,
                    node.factory_uuid
                ) == (self.deliveries_with_same_id(
                    deliveries,
                    delivery.delivery_id
                ) - 1)
            )
        ]

    def create_external_flows(
            self: Self,
            internal_flows: set[Flow]) -> set[Flow]:
        return set([
            flow
            for node
            in self.nodes
            for flow
            in self.find_external_flows(node, internal_flows)
        ])

    def create_flows(self: Self) -> set[Flow]:
        internal_flows = self.create_internal_flows()
        external_flows = self.create_external_flows(internal_flows)

        return internal_flows.union(external_flows)

    def inspect(self: Self) -> Self:
        self.nodes = self.create_nodes()
        self.flows = self.create_flows()

        return self

    def __str__(self: Self) -> str:
        return f"========== Pipeline Summary ==========\n\n{self.graph}"

    def __repr__(self: Self) -> str:
        return str(self)
