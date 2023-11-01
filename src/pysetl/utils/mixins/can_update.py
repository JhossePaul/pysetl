"""CanUpdateMixin module."""
from abc import abstractmethod, ABC
from pyspark.sql import DataFrame


class CanUpdate(ABC):
    """Abstract mixin to add update functionality to a connector."""

    @abstractmethod
    def update(self, dataframe: DataFrame, columns) -> None:
        """
        Update storage data.

        All matching data given some columns will be updated. Otherwise, will
        be created
        """
