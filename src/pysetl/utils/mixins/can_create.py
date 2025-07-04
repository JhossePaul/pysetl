"""
CanCreate mixin for PySetl.

Provides an abstract mixin to add create functionality to connectors.
"""
from abc import abstractmethod, ABC
from pyspark.sql import DataFrame


class CanCreate(ABC):
    """
    Abstract mixin to add create functionality to a connector.

    This class cannot be instantiated directly.
    """

    @abstractmethod
    def create(self, dataframe: DataFrame) -> None:
        """
        Abstract method to create a DataFrame on storage with optional suffix.

        Args:
            dataframe (DataFrame): The Spark DataFrame to create on storage.

        Raises:
            NotImplementedError: If not implemented in a subclass.
        """
