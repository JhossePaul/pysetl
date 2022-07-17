"""
HasBenchmarkMixin module
"""
from abc import abstractmethod
from pysetl.utils import BenchmarkResult


class HasBenchmarkMixin:
    """Indicates if the execution is benchmarked or not"""
    benchmarked: bool = True

    @abstractmethod
    def get_benchmarks(self) -> list[BenchmarkResult]:
        """Returns execution benchmarks"""
