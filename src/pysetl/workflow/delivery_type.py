"""
DeliveryType module for PySetl.

Provides a utility wrapper to simplify and standardize type comparison for
pipeline delivery types.

This is used internally to make type-based dependency resolution and DAG
construction easier.
"""
from __future__ import annotations
from types import GenericAlias
from typing import get_args, get_origin, _GenericAlias  # type: ignore
from typedspark import DataSet


class DeliveryType:
    """
    Utility wrapper for comparing delivery types in the pipeline.

    This class exists solely to make type comparison (including generics and
    typedspark DataSet subclasses) easier and more robust for pipeline
    dependency resolution. It is not intended for any other use.
    """

    def __init__(self, tp):
        """
        Initialize a DeliveryType wrapper.

        Args:
            tp: Any Python type representation, including:
                - Common classes (int, str, MyClass)
                - Typing generics (List[int], Dict[str, float], ...)
                - GenericAlias (list[int], dict[str, int], ...)
                - Subclass of typedspark.DataSet (DataSet[Citizen], etc.)
        """
        self.tp = tp if not isinstance(tp, DeliveryType) else tp.tp

    def __eq__(self, other) -> bool:
        """
        Compare this DeliveryType to another for equality.

        Args:
            other: Another DeliveryType or type to compare against.

        Returns:
            bool: True if the types are considered equivalent for delivery purposes, False otherwise.
        """
        other_delivery_type = (
            other if isinstance(other, DeliveryType) else DeliveryType(other)
        )
        t1 = self.tp
        t2 = other_delivery_type.tp

        t1_alias = self.is_alias(t1)
        t2_alias = self.is_alias(t2)

        # 1) Both types are GenericAlias (e.g. list[int], dict[str, float], ...)
        if t1_alias and t2_alias:
            o1, a1 = get_origin(t1), get_args(t1)
            o2, a2 = get_origin(t2), get_args(t2)

            return (o1 is o2) and (a1 == a2)

        # 2) One type is alias and the other is not.
        if t1_alias or t2_alias:
            return False

        # 3) Both types are subclasses of DataSet (typedspark)
        if issubclass(t1, DataSet) and issubclass(t2, DataSet):
            schema1 = getattr(t1, "_schema_annotations", None)
            schema2 = getattr(t2, "_schema_annotations", None)

            return schema1 is schema2

        # 4) Basic non parameterized types
        return t1 is t2

    def __repr__(self):
        """
        Return the string representation of the wrapped type.

        Returns:
            str: The repr of the underlying type.
        """
        return repr(self.tp)

    def is_alias(self, tp: type):
        """
        Check if a type is a generic alias (e.g., list[int], dict[str, float]).

        Args:
            tp (type): The type to check.

        Returns:
            bool: True if the type is a generic alias, False otherwise.
        """
        return isinstance(tp, (GenericAlias, _GenericAlias))
