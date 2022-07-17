from functools import singledispatch


@singledispatch
def pretty(obj):
    raise NotImplementedError


@pretty.register
def _(tpe: type) -> str:
    return "[".join([pretty(s) for s in tpe.__name__.split("[")])


@pretty.register
def _(type_name: str) -> str:
    return type_name.split(".")[-1]
