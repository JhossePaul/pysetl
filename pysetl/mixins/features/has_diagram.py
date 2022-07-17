"""
HasDiagramMixin module
"""

from abc import abstractmethod
from inspect import signature
from typing import Self


class HasDiagramMixin:
    """Unique diagram identifier"""
    diagram_id: str

    """
    Generate the diagram
    """
    @abstractmethod
    def toDiagram(self: Self) -> str:
        pass

    """
    Returns constructor signature
    """
    def get_signature(self):
        return [x for x in signature(self.__init__).parameters]

    """
    Returns formatted diagram ID
    """
    def format_diagram_id(self: Self, name: str, id: str, suffix: str) -> str:
        return name.replace("[]", "") + id.capitalize() + suffix

    """
    Displays diagram and return instance
    """
    def show_diagram(self: Self) -> Self:
        print(self.toDiagram())

        return(self)
