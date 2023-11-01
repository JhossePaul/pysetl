"""get_signature module."""
from inspect import signature


def get_signature(function) -> list[str]:
    """Get the expected parameters from a function."""
    return list((
        x
        for x, y
        in signature(function).parameters.items()
    ))
