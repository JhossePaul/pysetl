"""BanchmarkResult module."""
from dataclasses import dataclass


@dataclass
class BenchmarkResult:
    """A simple formmatter for factories execution benchmark."""
    cls: str
    read: float
    process: float
    write: float
    total: float

    def __str__(self) -> str:
        return "\n".join([
            f"Benchmark class: {self.cls}",
            f"Total elapsed time: {self.total} s",
            f"read: {self.read} s",
            f"process: {self.process} s",
            f"write: {self.write} s",
            "================="
        ])
