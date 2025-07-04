"""
HasBenchmark mixin for PySetl.

Provides an abstract mixin to add benchmarking functionality to classes.
"""
from abc import abstractmethod, ABC
from pysetl.utils import BenchmarkResult


class HasBenchmark(ABC):
    """
    Abstract mixin to add benchmarking functionality to a class.

    This class cannot be instantiated directly.
    """

    benchmarked: bool = True

    @abstractmethod
    def get_benchmarks(self) -> list[BenchmarkResult]:
        """Return execution benchmarks."""

    def benchmark(self) -> None:
        """
        Abstract method to run a benchmark on the class or method.

        Raises:
            NotImplementedError: If not implemented in a subclass.
        """
