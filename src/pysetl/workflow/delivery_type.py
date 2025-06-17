"""
Wrapper for factory type comparison.
"""
from __future__ import annotations
from types import GenericAlias
from typing import get_args, get_origin, _GenericAlias  # type: ignore
from typedspark import DataSet


class DeliveryType:
    """
    A wrapper to easily compare delivery types from factories.
    """
    def __init__(self, tp):
        """
        tp: any Python type representaion:
            - common classes (int, str, MyClass)
            - typing (List[int], Dict[str, float], ...)
            - GenericAlias (list[int], dict[str, int], ...)
            - subclass of typedspark.DataSet (DataSet[Citizen], etc.)
        """
        self.tp = tp if not isinstance(tp, DeliveryType) else tp.tp

    def __eq__(self, other) -> bool:
        other_delivery_type = (
            other
            if isinstance(other, DeliveryType)
            else DeliveryType(other)
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
        if (t1_alias or t2_alias):
            return False

        # 3) Both types are subclasses of DataSet (typedspark)
        if issubclass(t1, DataSet) and issubclass(t2, DataSet):
            schema1 = getattr(t1, "_schema_annotations", None)
            schema2 = getattr(t2, "_schema_annotations", None)

            return schema1 is schema2

        # 4) Basic non parameterized types
        return t1 is t2

    def __repr__(self):
        return repr(self.tp)

    def __str__(self) -> str:
        return str(self.tp)

    def is_alias(self, tp: type):
        """
        Check if type is an alias.
        """
        return isinstance(tp, (GenericAlias, _GenericAlias))
