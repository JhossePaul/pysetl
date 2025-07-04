"""
BenchmarkModifier module for PySetl.

Provides a wrapper for benchmarking method calls on any object.
"""
import inspect
import types
import time
from typing import Generic, TypeVar, Callable
from typing_extensions import Self


T = TypeVar("T")


class BenchmarkModifier(Generic[T]):
    """
    Wrapper for benchmarking method calls on any object.

    Attributes:
        _obj (T): The wrapped object whose methods are benchmarked.
    """

    def __init__(self: Self, obj: T) -> None:
        """
        Initialize the BenchmarkModifier.

        Args:
            obj (T): The object whose methods will be benchmarked.
        """
        self._obj = obj
        for name, value in inspect.getmembers(obj, callable):
            if not hasattr(self, name) and isinstance(value, types.MethodType):
                setattr(self._obj, name, self.__benchmark(value))

        setattr(self._obj, "times", {})

    def __benchmark(self: Self, function: Callable) -> Callable:
        """
        Benchmark a function's execution time.

        Args:
            function (Callable): The function to benchmark.

        Returns:
            Callable: The wrapped function with timing logic.
        """

        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = function(*args, **kwargs)
            elapsed_time = time.time() - start_time
            # Ensure 'times' attribute exists and is a dict
            times = getattr(self._obj, "times", None)
            if times is None:
                times = {}
                setattr(self._obj, "times", times)
            times[function.__name__] = elapsed_time

            return result

        return wrapper

    def get(self) -> T:
        """
        Return the modified object with benchmarked methods.

        Returns:
            T: The wrapped object with timing information.
        """
        return self._obj
