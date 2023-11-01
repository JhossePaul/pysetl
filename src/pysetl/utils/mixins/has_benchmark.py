"""HasBenchmarkMixin module."""
from abc import abstractmethod, ABC
from pysetl.utils import BenchmarkResult


class HasBenchmark(ABC):
    """Indicate if the execution is benchmarked or not."""

    benchmarked: bool = True

    @abstractmethod
    def get_benchmarks(self) -> list[BenchmarkResult]:
        """Return execution benchmarks."""
