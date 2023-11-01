"""CanCreateMixin module."""
from abc import abstractmethod, ABC


class CanDelete(ABC):
    """Delete rows from a table."""

    @abstractmethod
    def delete(self, query: str) -> None:
        """Delete object from storage."""
