"""
BenchmarkModifier module
"""
import inspect
import types
import time
from typing import Generic, TypeVar, Callable
from typing_extensions import Self


T = TypeVar("T")


class BenchmarkModifier(Generic[T]):
    """
    Wrapper for any function to benchmark its method calls
    """
    def __init__(self: Self, obj: T) -> None:
        self._obj = obj
        for name, value in inspect.getmembers(obj, callable):
            if not hasattr(self, name) and isinstance(value, types.MethodType):
                setattr(self._obj, name, self.__benchmark(value))

        object.__setattr__(self._obj, "times", {})

    def __benchmark(self: Self, function: Callable) -> Callable:
        """
        Benchmark a function execution time
        """
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = function(*args, **kwargs)
            elapsed_time = time.time() - start_time
            self._obj.times[function.__name__] = elapsed_time

            return result

        return wrapper

    def get(self) -> T:
        """
        Returns modified object
        """
        return self._obj
