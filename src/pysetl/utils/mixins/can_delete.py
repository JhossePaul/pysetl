"""CanCreateMixin module."""
from abc import abstractmethod


class CanDelete:
    """Delete rows from a table."""

    @abstractmethod
    def delete(self, query: str) -> None:
        """Delete object from storage."""
