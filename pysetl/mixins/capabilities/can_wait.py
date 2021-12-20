"""
CanWait module
"""
from abc import abstractmethod
from typing import Optional


class CanWaitMixin:
    """
    Abstract mixin to add wait functionality to a connector. This methods
    should asynchronously await for result to finish
    """
    @abstractmethod
    def await_termination(self, timeout: Optional[float]) -> None:
        """Await until process finishes"""

    @abstractmethod
    def stop(self) -> None:
        """Stops execution of a process if running"""
