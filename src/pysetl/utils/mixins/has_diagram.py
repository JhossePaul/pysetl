"""
HasDiagram mixin for PySetl.

Provides an abstract mixin to add diagram generation functionality to classes.
"""

from abc import abstractmethod, ABC
from inspect import signature
from typing_extensions import Self


class HasDiagram(ABC):
    """
    Abstract mixin to add diagram generation functionality to a class.

    This class cannot be instantiated directly.
    """

    @property
    @abstractmethod
    def diagram_id(self: Self) -> str:
        """Return diagram id."""

    @abstractmethod
    def to_diagram(self) -> str:
        """
        Abstract method to generate a diagram representation of the class or
        workflow.
        """

    def get_signature(self):
        """Return constructor signature."""
        return list(signature(self.__init__).parameters)

    def format_diagram_id(self: Self, name: str, diagram_id: str, suffix: str) -> str:
        """Return formatted diagram ID."""
        return name.replace("[", "").replace("]", "") + diagram_id.capitalize() + suffix
