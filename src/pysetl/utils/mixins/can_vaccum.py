"""CanUpdateMixin module."""
from abc import abstractmethod
from typing import Optional


class CanVaccum:
    """Abstract mixin to add update functionality to a connector."""

    @abstractmethod
    def vaccum(self, retention_hours: Optional[int]) -> None:
        """Delete files older than retention_hours."""
