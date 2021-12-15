"""
CanCreateMixin module
"""
from typing import overload, Optional
from abc import abstractmethod

from pyspark.sql import DataFrame


class CanCreateMixin:
    """Abstract Mixin to add create functionality to a connector"""
    @overload
    def create(self, dataframe: DataFrame) -> None:
        """Creates DataFrame on storage"""

    @overload
    def create(self, dataframe: DataFrame, suffix: str) -> None:
        """Creates Dataframe on storage with a suffix"""

    @abstractmethod
    def create(
        self,
        dataframe: DataFrame,
        suffix: Optional[str] = None
    ) -> None:
        """Creates DataFrame on storage with optional suffix"""
