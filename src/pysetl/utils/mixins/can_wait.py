"""CanWait module."""
from abc import abstractmethod
from typing import Optional


class CanWait:
    """
    CanWait mixin.

    Abstract mixin to add wait functionality to a connector. This methods
    should asynchronously await for result to finish
    """

    @abstractmethod
    def await_termination(self, timeout: Optional[float]) -> None:
        """Await until process finishes."""

    @abstractmethod
    def stop(self) -> None:
        """Stop execution of a process if running."""
