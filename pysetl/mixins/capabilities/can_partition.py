"""
CanPartitionMixin module
"""
from abc import abstractmethod


class CanPartitionMixin:
    """Abstract mixin to add partition_by functionality to a connector"""
    @abstractmethod
    def partition_by(self, *columns: str) -> None:
        """Drop a table from de storage"""
