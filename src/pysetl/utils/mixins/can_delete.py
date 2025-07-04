"""
CanDelete mixin for PySetl.

Provides an abstract mixin to add delete functionality to connectors.
"""
from abc import abstractmethod, ABC


class CanDelete(ABC):
    """
    Abstract mixin to add delete functionality to a connector.

    This class cannot be instantiated directly.
    """

    @abstractmethod
    def delete(self, query: str) -> None:
        """
        Abstract method to delete objects from storage based on a query.

        Args:
            query (str): The query or condition for deletion.

        Raises:
            NotImplementedError: If not implemented in a subclass.
        """
