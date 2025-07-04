"""
CanUpdate mixin for PySetl.

Provides an abstract mixin to add update functionality to connectors.
"""
from abc import abstractmethod, ABC


class CanUpdate(ABC):
    """
    Abstract mixin to add update functionality to a connector.

    This class cannot be instantiated directly.
    """

    @abstractmethod
    def update(self, query: str) -> None:
        """
        Abstract method to update objects in storage based on a query.

        Args:
            query (str): The query or condition for updating.

        Raises:
            NotImplementedError: If not implemented in a subclass.
        """
