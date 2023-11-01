"""CanDrop module."""
from abc import abstractmethod, ABC


class CanDrop(ABC):
    """Abstract Mixin to add delete functionality to a connector."""

    @abstractmethod
    def drop(self) -> None:
        """Drop a table from de storage."""
