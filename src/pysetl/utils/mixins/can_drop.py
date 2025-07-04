"""
CanDrop mixin for PySetl.

Provides an abstract mixin to add drop functionality to connectors.
"""
from abc import abstractmethod, ABC


class CanDrop(ABC):
    """
    Abstract mixin to add drop functionality to a connector.

    This class cannot be instantiated directly.
    """

    @abstractmethod
    def drop(self) -> None:
        """
        Abstract method to drop a table or data from storage.

        Raises:
            NotImplementedError: If not implemented in a subclass.
        """
