"""Utility function to format multiple types for printing."""
from functools import singledispatch
from typing import _GenericAlias, get_args, get_origin  # type: ignore
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
    origin_str = pretty(origin)
    args = get_args(alias)
    args_str = ", ".join(pretty(a) for a in args)
    return f"{origin_str}[{args_str}]"


@pretty.register
def _(tpe: GenericAlias) -> str:  # pragma: py-gt-310
    origin = get_origin(tpe)
    args = get_args(tpe)
    origin_str = pretty(origin)
    args_str = ", ".join(pretty(a) for a in args)
    return f"{origin_str}[{args_str}]"


@pretty.register
def _(tpe: type) -> str:
    if isinstance(tpe, GenericAlias):  # pragma: py-gt-310
        origin = get_origin(tpe)
        args = get_args(tpe)
        origin_str = pretty(origin)
        args_str = ", ".join(pretty(a) for a in args)
        return f"{origin_str}[{args_str}]"
    return tpe.__name__
