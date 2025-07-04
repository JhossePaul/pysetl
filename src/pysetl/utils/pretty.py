"""
Pretty-printing utility for PySetl.

Provides a singledispatch function to format types and generics for
human-readable output.
"""
from functools import singledispatch
from typing import _GenericAlias, get_args, get_origin  # type: ignore
from types import GenericAlias


@singledispatch
def pretty(obj) -> str:
    """
    Pretty-print a type or object for human-readable output.

    Args:
        obj: The object or type to pretty-print.

    Returns:
        str: A human-readable string representation.
    """
    return repr(obj)


@pretty.register(type(None))
def _(tpe):
    """
    Pretty-print None as an empty string.

    Args:
        tpe: None

    Returns:
        str: An empty string.
    """
    return str("")


@pretty.register(str)
def _(type_name):
    """
    Pretty-print a type name string by stripping module prefixes.

    Args:
        type_name (str): The type name string.

    Returns:
        str: The base type name.
    """
    return type_name.split(".")[-1]


@pretty.register(_GenericAlias)
def _(alias):
    """
    Pretty-print a typing._GenericAlias (e.g., List[int], Dict[str, float]).

    Args:
        alias (_GenericAlias): The generic alias type.

    Returns:
        str: Human-readable generic type string.
    """
    origin = get_origin(alias)
    origin_str = pretty(origin)
    args = get_args(alias)
    args_str = ", ".join(pretty(a) for a in args)
    return f"{origin_str}[{args_str}]"


@pretty.register(GenericAlias)
def _(tpe):  # pragma: py-lt-311
    """
    Pretty-print a types.GenericAlias (Python <3.9 compatibility).

    Args:
        tpe (GenericAlias): The generic alias type.

    Returns:
        str: Human-readable generic type string.
    """
    origin = get_origin(tpe)
    args = get_args(tpe)
    origin_str = pretty(origin)
    args_str = ", ".join(pretty(a) for a in args)
    return f"{origin_str}[{args_str}]"


@pretty.register(type)
def _(tpe):
    """
    Pretty-print a type object, including generic types for Python >=3.9.

    Args:
        tpe (type): The type object.

    Returns:
        str: Human-readable type string.
    """
    if isinstance(tpe, GenericAlias):  # pragma: py-gt-310
        origin = get_origin(tpe)
        args = get_args(tpe)
        origin_str = pretty(origin)
        args_str = ", ".join(pretty(a) for a in args)
        return f"{origin_str}[{args_str}]"
    return tpe.__name__
