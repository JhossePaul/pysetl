"""CanCreate module."""
from abc import abstractmethod
from pyspark.sql import DataFrame


class CanCreate:
    """Abstract Mixin to add create functionality to a connector."""

    @abstractmethod
    def create(
        self,
        dataframe: DataFrame
    ) -> None:
        """Create DataFrame on storage with optional suffix."""
