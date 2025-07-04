"""
CanWait mixin for PySetl.

Provides an abstract mixin to add wait functionality to connectors or workflows.
"""
from abc import abstractmethod, ABC
from typing import Optional


class CanWait(ABC):
    """
    Abstract mixin to add wait functionality to a connector or workflow.

    Abstract mixin to add wait functionality to a connector. This methods
    should asynchronously await for result to finish

    This class cannot be instantiated directly.
    """

    @abstractmethod
    def await_termination(self, timeout: Optional[float]) -> None:
        """Await until process finishes."""

    @abstractmethod
    def stop(self) -> None:
        """Stop execution of a process if running."""

    def wait(self, timeout: int = 60) -> None:
        """
        Abstract method to wait for a condition or resource with a timeout.

        Args:
            timeout (int): The maximum time to wait in seconds (default: 60).

        Raises:
            NotImplementedError: If not implemented in a subclass.
        """
