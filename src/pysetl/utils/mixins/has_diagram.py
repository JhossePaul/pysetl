"""HasDiagram module."""

from abc import abstractmethod, ABC
from inspect import signature
from typing_extensions import Self


class HasDiagram(ABC):
    """Mixin for objects that can be represented as a mermaid diagram."""

    @property
    @abstractmethod
    def diagram_id(self: Self) -> str:
        """Return diagram id."""

    @abstractmethod
    def to_diagram(self: Self) -> str:
        """Generate the diagram."""

    def get_signature(self):
        """Return constructor signature."""
        return list(signature(self.__init__).parameters)

    def format_diagram_id(
            self: Self,
            name: str,
            diagram_id: str,
            suffix: str) -> str:
        """Return formatted diagram ID."""
        return (
            name.replace("[", "").replace("]", "") +
            diagram_id.capitalize() +
            suffix
        )
