"""
BenchmarkResult module for PySetl.

Defines a dataclass for storing and formatting benchmark results for factory
execution.
"""
from dataclasses import dataclass


@dataclass
class BenchmarkResult:
    """
    Stores and formats benchmark results for factory execution.

    Attributes:
        cls (str): The class name of the factory.
        read (float): Time taken for the read phase (seconds).
        process (float): Time taken for the process phase (seconds).
        write (float): Time taken for the write phase (seconds).
        total (float): Total elapsed time (seconds).
    """

    cls: str
    read: float
    process: float
    write: float
    total: float

    def __str__(self) -> str:
        """
        Return a formatted string representation of the benchmark result.

        Returns:
            str: Human-readable summary of benchmark timings.
        """
        return "\n".join(
            [
                f"Benchmark class: {self.cls}",
                f"Total elapsed time: {self.total} s",
                f"read: {self.read} s",
                f"process: {self.process} s",
                f"write: {self.write} s",
                "=================",
            ]
        )
