"""Stage module."""
from __future__ import annotations
from typing import Callable, Any, Optional
from functools import reduce
from typing_extensions import Self, ParamSpec
from pyspark.sql import SparkSession
from pysetl.utils.mixins import (
    HasLogger, IsIdentifiable, HasRegistry, HasBenchmark,
    IsWritable
)
from pysetl.utils import BenchmarkResult, BenchmarkModifier
from .deliverable import Deliverable
from .node import Node
from .factory import Factory


P = ParamSpec("P")


class Stage(
    HasRegistry[Factory],
    HasLogger,
    HasBenchmark,
    IsWritable,
    IsIdentifiable
):
    """
    A Stage is a collection of independent Factories.

    All the stages of a pipeline will be executed sequentially at runtime.
    Within a stage, all factories are executed in sequential order.
    """

    def __init__(self, benckmark: bool = False) -> None:
        HasRegistry.__init__(self)
        IsIdentifiable.__init__(self)
        self.benchmarked = benckmark
        self.__end: bool = True
        self.__stage_id: int = 0
        self.__deliverable: list[Deliverable] = []
        self.__benchmark_result: list[BenchmarkResult] = []

    @property
    def end(self: Self) -> bool:
        """Exposes private end property."""
        return self.__end

    @end.setter
    def end(self: Self, __value: bool) -> None:
        """Exposes private end property."""
        self.__end = __value

    @property
    def start(self: Self) -> bool:
        """Verifies if stage starts DAG."""
        return self.__stage_id == 0

    @property
    def stage_id(self: Self) -> int:
        """Exposes private stage_id property."""
        return self.__stage_id

    @stage_id.setter
    def stage_id(self: Self, __value: int) -> None:
        self.__stage_id = __value

    @property
    def factories(self: Self) -> list[Factory]:
        """Exposes registered factories in the stage."""
        return list(self.get_registry().values())

    @property
    def deliverable(self: Self) -> list[Deliverable]:
        """Returns the list of deliverables of the stage."""
        return self.__deliverable

    def instantiate_factory(
            self: Self,
            factory_type: type[Factory],
            *args,
            **kwargs) -> Factory:
        """Given a type[Factory] instantiates the class."""
        return factory_type(*args, **kwargs)

    def add_factory_from_type(
            self: Self,
            factory_type: type[Factory],
            *args,
            **kwargs) -> Self:
        """Add a factory to the stage given a type[Factory]."""
        factory = self.instantiate_factory(factory_type, *args, **kwargs)

        return self.add_factory(factory)

    def add_factory(self: Self, factory: Factory):
        """Add a factory to the stage given a Factory instance."""
        self.register(factory)

        return self

    def __str__(self: Self) -> str:
        """Customize str method."""
        self.log_info(
            f"Stage {self.stage_id} contains {len(self.get_registry())} items"
        )
        factories_string = "\n".join([str(f) for f in self.factories])

        return factories_string

    def __repr__(self: Self) -> str:
        """Customize repr method."""
        return str(self)

    def should_write(self: Self, factory: Factory) -> bool:
        """Verify if stage and factory should write."""
        return self.is_writable and factory.is_writable

    def execute_factory(self: Self, factory: Factory) -> Factory:
        """Execute a factory."""
        if self.should_write(factory):
            self.log_debug(f"Persist output of {factory.__class__.__name__}")

            return factory.read().process().write()
        else:
            return factory.read().process()

    def handle_benchmark(self, factory: Factory) -> BenchmarkResult:
        """Execute and benchmarks a given Factory."""
        factory_class_name = factory.__class__.__name__

        benckmarked_factory = BenchmarkModifier(factory).get()
        executed_factory = self.execute_factory(benckmarked_factory)

        benchmark_times: dict = executed_factory.times  # type: ignore
        elapsed_time = reduce(lambda x, y: x + y, benchmark_times.values())

        return BenchmarkResult(
            factory_class_name,
            benchmark_times.get("read", 0),
            benchmark_times.get("process", 0),
            benchmark_times.get("write", 0),
            elapsed_time
        )

    def with_spark_session(
        self: Self,
        fun: Callable[[SparkSession], Any]
    ) -> Any:
        """Run a functions if SparkSession exists otherwise returns None."""
        spark: Optional[SparkSession] = SparkSession.getActiveSession()

        if spark:
            fun(spark)
        else:
            return None

    def run_factory(self: Self, factory: Factory) -> Deliverable:
        """Run a factory and collects its deliverable."""
        self.with_spark_session(lambda _: _.sparkContext.setJobGroup(
            factory.__class__.__name__,
            "stage group"
        ))

        if self.benchmarked:
            benchmark_result = self.handle_benchmark(factory)

            self.__benchmark_result.append(benchmark_result)
        else:
            self.execute_factory(factory)

        # Find a way clearJobGroup since sparkContext method doesn't exists
        # self.with_spark_session(lambda _: _.sparkContext.clearJobGroup())
        return factory.deliverable

    def create_nodes(self: Self) -> list[Node]:
        """Create stage nodes with registered stage factories."""
        return [
            Node.from_factory(f, self.__stage_id, self.__end)
            for f
            in self.factories
        ]

    def get_benchmarks(self: Self) -> list[BenchmarkResult]:
        """Return collected benchmarks."""
        return self.__benchmark_result

    def run(self: Self) -> Self:
        """Run the Stage."""
        self.__deliverable = list(map(self.run_factory, self.factories))

        return self
