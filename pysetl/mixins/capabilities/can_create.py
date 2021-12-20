"""
CanCreateMixin module
"""
from typing import Optional
from abc import abstractmethod

from pyspark.sql import DataFrame


class CanCreateMixin:
    """Abstract Mixin to add create functionality to a connector"""
    @abstractmethod
    def create(
        self,
        dataframe: DataFrame,
        suffix: Optional[str] = None
    ) -> None:
        """Creates DataFrame on storage with optional suffix"""
