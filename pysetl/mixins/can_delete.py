"""
CanCreateMixin module
"""
from abc import abstractmethod


class CanDeleteMixin:
    """Deletes rows from a table"""
    @abstractmethod
    def delete(self, query: str) -> None:
        "Deletes object from storage"
