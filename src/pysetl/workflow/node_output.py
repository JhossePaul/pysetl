"""NodeOutput module."""
from __future__ import annotations
from importlib import import_module
from dataclasses import dataclass, is_dataclass, fields
from typing import (  # type: ignore
    TYPE_CHECKING, get_origin, get_args, TypeVar,
    _GenericAlias  # type: ignore
)
from typing_extensions import Self
from typedspark import DataSet
from pydantic import BaseModel
from pysetl.utils.mixins import HasDiagram
from pysetl.utils import pretty


if TYPE_CHECKING:  # pragma: no cover
    from .factory import Factory


@dataclass
class NodeOutput(HasDiagram):
    """Node output representation."""

    delivery_type: type
    consumer: set[type[Factory]]
    delivery_id: str = ""
    final_output: bool = False
    external: bool = False

    @property
    def diagram_id(self) -> str:
        """Digram identifier."""
        final_suffix = "Final" if self.final_output else ""
        external_suffix = "External" if self.external else ""

        return self.format_diagram_id(
            pretty(self.delivery_type),
            self.delivery_id,
            final_suffix + external_suffix
        )

    def get_fields(self: Self) -> list[str]:
        """Get arguments from payload signature."""
        def _pretty(tpe):
            return pretty(arg) if isinstance(tpe, TypeVar) else pretty(tpe)

        origin = get_origin(self.delivery_type)
        args = get_args(self.delivery_type)

        if origin is DataSet and len(args) == 1:
            (arg,) = args
            module = import_module(arg.__module__)
            real_targ = getattr(module, arg.__name__)

            return [
                f"    >{field.name}: {field.dataType.simpleString()}"
                for field
                in real_targ.get_structtype().fields
            ]

        if (
            isinstance(self.delivery_type, _GenericAlias) and
            is_dataclass(origin)
        ):
            (arg,) = args

            return [
                f"    >{field.name}: {_pretty(field.type)}"
                for field
                in fields(origin)
            ]

        if is_dataclass(self.delivery_type):
            return [
                f"    >{field.name}: {pretty(field.type)}"
                for field
                in fields(self.delivery_type)
            ]

        if issubclass(self.delivery_type, BaseModel):
            return [
                f"    >{key}: {pretty(value.annotation)}"
                for key, value
                in self.delivery_type.model_fields.items()
            ]

        return []

    def to_diagram(self) -> str:
        """Create mermaid diagram for a NodeOutput."""
        fields_str: str = "\n".join(self.get_fields())

        return "\n".join([
            f"class {self.diagram_id} {{",
            f"    <<{pretty(self.delivery_type)}>>",
            fields_str,
            "}"
        ])
