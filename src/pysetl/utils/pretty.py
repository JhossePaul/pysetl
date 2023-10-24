"""Utility function to format multiple types for printing."""
from functools import singledispatch
from typing import _GenericAlias, get_args, get_origin
from types import GenericAlias


@singledispatch
def pretty(obj):
    """Pretty prints types."""
    raise NotImplementedError


@pretty.register
def _(tpe: None):
    return ""


@pretty.register
def _(type_name: str) -> str:
    return type_name.split(".")[-1]


@pretty.register
def _(alias: _GenericAlias):
    origin = get_origin(alias)
    args = get_args(alias)
    arg = None if len(args) == 0 else args[0]

    origin_str = pretty(origin)
    arg_str = f"[{pretty(arg)}]" if arg else ""

    return f"{origin_str}{arg_str}"


@pretty.register
def _(tpe: type) -> str:
    if isinstance(tpe, GenericAlias):
        origin = get_origin(tpe)
        args = get_args(tpe)
        [arg, *_] = args

        origin_str = pretty(origin)
        arg_str = f"[{pretty(arg)}]"

        return f"{origin_str}{arg_str}"
    else:
        return tpe.__name__
