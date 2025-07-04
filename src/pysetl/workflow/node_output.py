"""
NodeOutput module for PySetl.

Defines the NodeOutput class, which represents the output metadata of a node in
the workflow DAG.  NodeOutput is used to build flows between nodes, enabling the
inspector to create the DAG and the dispatcher to understand data flow patterns.
It contains type information, consumer details, and metadata for visualization
and dependency resolution.

NodeOutput is a lightweight representation that doesn't contain actual data, but
rather describes what type of data a node produces and who can consume it.
"""
from __future__ import annotations
from dataclasses import dataclass, is_dataclass, fields
from typing import (  # type: ignore
    TYPE_CHECKING,
    get_origin,
    get_args,
    TypeVar,
    _GenericAlias,  # type: ignore
)
from typing_extensions import Self
from typedspark import DataSet
from pydantic import BaseModel
from pysetl.utils.mixins import HasDiagram
from pysetl.utils.pretty import pretty
from pysetl.utils.exceptions import BasePySetlException
from pysetl.workflow.delivery_type import DeliveryType


if TYPE_CHECKING:  # pragma: no cover
    from .factory import Factory


@dataclass
class NodeOutput(HasDiagram):
    """
    Representation of a node's output for building flows in the DAG.

    NodeOutput contains metadata about what a node produces, including the delivery type,
    potential consumers, and additional flags for special cases (final output, external input).
    This information is used by the inspector to build the DAG and by the dispatcher to
    understand data flow patterns.

    NodeOutput is a lightweight representation that doesn't contain actual data, but rather
    describes what type of data a node produces and who can consume it.

    Attributes:
        delivery_type (type): The type of data this node produces.
        consumer (set[type[Factory]]): Set of factory classes that can consume this output.
        delivery_id (str): Optional identifier for disambiguation when multiple outputs have the same type.
        final_output (bool): Whether this is the final output of the pipeline.
        external (bool): Whether this represents an external input source.
    """

    delivery_type: type
    consumer: set[type[Factory]]
    delivery_id: str = ""
    final_output: bool = False
    external: bool = False

    @property
    def wrapped_delivery_type(self) -> DeliveryType:
        """
        Get a wrapped delivery type for easy comparison.

        Returns:
            DeliveryType: Wrapped delivery type for standardized comparison operations.
        """
        return DeliveryType(self.delivery_type)

    @property
    def diagram_id(self) -> str:
        """
        Generate a unique identifier for diagram rendering.

        Creates an identifier that includes type information and special flags
        (final output, external input) for proper visualization.

        Returns:
            str: Unique diagram identifier for this node output.
        """
        final_suffix = "Final" if self.final_output else ""
        external_suffix = "External" if self.external else ""

        return self.format_diagram_id(
            pretty(self.delivery_type), self.delivery_id, final_suffix + external_suffix
        )

    def get_fields(self: Self) -> list[str]:
        """
        Extract field information from the delivery type for diagram generation.

        Analyzes the delivery type to extract field names and types, supporting
        various data structures including dataclasses, Pydantic models, and TypedSpark DataSets.

        Returns:
            list[str]: List of formatted field strings for diagram rendering.

        Raises:
            BasePySetlException: If a DataSet has no schema annotations.
        """

        def _pretty(arg, tpe):
            return pretty(arg) if isinstance(tpe, TypeVar) else pretty(tpe)

        origin = get_origin(self.delivery_type)
        args = get_args(self.delivery_type)

        if isinstance(self.delivery_type, _GenericAlias) and is_dataclass(origin):
            (arg,) = args

            return [
                f"    >{field.name}: {_pretty(arg, field.type)}"
                for field in fields(origin)
            ]

        if issubclass(self.delivery_type, DataSet):
            schema = getattr(self.delivery_type, "_schema_annotations", None)

            if not schema:
                raise BasePySetlException("DataSet has no schema.")

            return [
                f"    >{field.name}: {field.dataType.simpleString()}"
                for field in schema.get_structtype().fields
            ]

        if is_dataclass(self.delivery_type):
            return [
                f"    >{field.name}: {pretty(field.type)}"
                for field in fields(self.delivery_type)
            ]

        if issubclass(self.delivery_type, BaseModel):
            return [
                f"    >{key}: {pretty(value.annotation)}"
                for key, value in self.delivery_type.model_fields.items()
            ]

        return []

    def to_diagram(self) -> str:
        """
        Create a Mermaid diagram representation of this node output.

        Generates a class diagram showing the delivery type and its fields,
        formatted for Mermaid diagram rendering.

        Returns:
            str: Mermaid diagram string for this node output.
        """
        fields_str: str = "\n".join(self.get_fields())

        return "\n".join(
            [
                f"class {self.diagram_id} {{",
                f"    <<{pretty(self.delivery_type)}>>",
                fields_str,
                "}",
            ]
        )
