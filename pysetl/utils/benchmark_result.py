from dataclasses import dataclass


@dataclass
class BenchmarkResult:
    cls: str
    read: float
    process: float
    write: float
    get: float
    total: float

    def __str__(self) -> str:
        return f"""
         Benchmark class: {self.cls}
         Total elapsed time: {self.total} s
         read: {self.read} s\n
         process: {self.process} s
         write: {self.write} s
         =================
        """
