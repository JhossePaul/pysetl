"""
get_signature module for PySetl.

Provides a utility function to extract parameter names from a function signature.
"""
from inspect import signature


def get_signature(function) -> list[str]:
    """
    Get the expected parameter names from a function's signature.

    Args:
        function: The function to inspect.

    Returns:
        list[str]: List of parameter names for the function.
    """
    return list((x for x, y in signature(function).parameters.items()))
