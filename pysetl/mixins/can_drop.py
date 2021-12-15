"""
CanDrop module
"""
from abc import abstractmethod


class CanDropMixin:
    """Abstract Mixin to add delete functionality to a connector"""
    @abstractmethod
    def drop(self, query: str) -> None:
        """Drop a table from de storage"""
