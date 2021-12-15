from typing import overload, Optional
from abc import abstractmethod

from pyspark.sql import DataFrame

class CanCreateMixin:
    @overload
    def create(dt: DataFrame) -> None:
        "Creates DataFrame on storage"

    @overload
    def create(dt: DataFrame, suffix: str) -> None:
        "Creates Dataframe on storage with a suffix"

    @abstractmethod
    def create(dt: DataFrame, suffix: Optional[str]) -> None:
        "Creates DataFrame on storage with optional suffix"
