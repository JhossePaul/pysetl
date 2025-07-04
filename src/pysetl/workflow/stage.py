"""
Stage module for PySetl.

Defines the Stage abstraction, which is a collection of factories that execute sequentially
within a pipeline. Stages are the building blocks of pipelines, providing order and organization
to ETL workflows. All factories within a stage execute in sequential order, and all stages in
a pipeline execute sequentially at runtime.

Stages enable users to group related factories together, creating a natural project structure
and making large ETL processes more manageable and maintainable.
"""
from __future__ import annotations
from typing import Callable, Any, Optional
from functools import reduce
from typing_extensions import Self, ParamSpec
from pyspark.sql import SparkSession
from pysetl.utils.mixins import (
    HasLogger,
    IsIdentifiable,
    HasRegistry,
    HasBenchmark,
    IsWritable,
)
from pysetl.utils import BenchmarkResult, BenchmarkModifier
from .deliverable import Deliverable
from .node import Node
from .factory import Factory


P = ParamSpec("P")


class Stage(HasRegistry[Factory], HasLogger, HasBenchmark, IsWritable, IsIdentifiable):
    """
    A Stage is a collection of factories that execute sequentially within a pipeline.

    Stages are the building blocks of pipelines, providing order and organization to ETL
    workflows. All factories within a stage execute in sequential order, and all stages
    in a pipeline execute sequentially at runtime.

    Stages enable users to group related factories together, creating a natural project
    structure and making large ETL processes more manageable and maintainable.

    TODO: Implement parallel execution of factories within a stage (requires investigation
          of Python/PySpark threading capabilities)
    TODO: Implement clearJobGroup functionality (requires PySpark API investigation)
    """

    def __init__(self, benckmark: bool = False) -> None:
        """
        Initialize a new Stage.

        Args:
            benckmark: Whether to enable benchmarking for factories in this stage.
                      Defaults to False.
        """
        super().__init__()
        IsIdentifiable.__init__(self)
        self.benchmarked = benckmark
        self.__end: bool = True
        self.__stage_id: int = 0
        self.__deliverable: list[Deliverable] = []
        self.__benchmark_result: list[BenchmarkResult] = []

    @property
    def end(self: Self) -> bool:
        """
        Get whether this stage is the end stage of the pipeline.

        Returns:
            bool: True if this is the end stage, False otherwise.
        """
        return self.__end

    @end.setter
    def end(self: Self, __value: bool) -> None:
        """
        Set whether this stage is the end stage of the pipeline.

        Args:
            __value: True if this is the end stage, False otherwise.
        """
        self.__end = __value

    @property
    def start(self: Self) -> bool:
        """
        Check if this stage starts the DAG (stage_id == 0).

        Returns:
            bool: True if this stage starts the DAG, False otherwise.
        """
        return self.__stage_id == 0

    @property
    def stage_id(self: Self) -> int:
        """
        Get the stage ID within the pipeline.

        Returns:
            int: The stage ID (0-based index).
        """
        return self.__stage_id

    @stage_id.setter
    def stage_id(self: Self, __value: int) -> None:
        """
        Set the stage ID within the pipeline.

        Args:
            __value: The stage ID (0-based index).
        """
        self.__stage_id = __value

    @property
    def factories(self: Self) -> list[Factory]:
        """
        Get all registered factories in this stage.

        Returns:
            list[Factory]: List of all factories registered in this stage.
        """
        return list(self.get_registry().values())

    @property
    def deliverable(self: Self) -> list[Deliverable]:
        """
        Get the list of deliverables produced by this stage.

        Returns:
            list[Deliverable]: List of deliverables from all factories in this stage.
        """
        return self.__deliverable

    def instantiate_factory(
        self: Self, factory_type: type[Factory], *args: Any, **kwargs: Any
    ) -> Factory:
        """
        Instantiate a factory from its type.

        Args:
            factory_type: The factory class to instantiate.
            *args: Positional arguments to pass to the factory constructor.
            **kwargs: Keyword arguments to pass to the factory constructor.

        Returns:
            Factory: The instantiated factory instance.
        """
        return factory_type(*args, **kwargs)

    def add_factory_from_type(
        self: Self, factory_type: type[Factory], *args: Any, **kwargs: Any
    ) -> Self:
        """
        Add a factory to the stage by instantiating it from its type.

        Args:
            factory_type: The factory class to instantiate and add.
            *args: Positional arguments to pass to the factory constructor.
            **kwargs: Keyword arguments to pass to the factory constructor.

        Returns:
            Self: This stage instance for method chaining.
        """
        factory = self.instantiate_factory(factory_type, *args, **kwargs)

        return self.add_factory(factory)

    def add_factory(self: Self, factory: Factory) -> Self:
        """
        Add a factory instance to the stage.

        Args:
            factory: The factory instance to add to this stage.

        Returns:
            Self: This stage instance for method chaining.
        """
        self.register(factory)

        return self

    def __str__(self: Self) -> str:
        """
        Get a string representation of this stage.

        Returns:
            str: String representation showing stage ID and factory count.
        """
        self.log_info(
            f"Stage {self.stage_id} contains {len(self.get_registry())} items"
        )
        factories_string = "\n".join([str(f) for f in self.factories])

        return factories_string

    def __repr__(self: Self) -> str:
        """
        Get a string representation of this stage.

        Returns:
            str: String representation of this stage.
        """
        return str(self)

    def should_write(self: Self, factory: Factory) -> bool:
        """
        Check if both the stage and factory should write output.

        Args:
            factory: The factory to check.

        Returns:
            bool: True if both stage and factory are writable, False otherwise.
        """
        return self.is_writable and factory.is_writable

    def execute_factory(self: Self, factory: Factory) -> Factory:
        """
        Execute a factory's ETL process.

        If both stage and factory are writable, executes read().process().write().
        Otherwise, executes read().process() only.

        Args:
            factory: The factory to execute.

        Returns:
            Factory: The executed factory instance.
        """
        if self.should_write(factory):
            self.log_debug(f"Persist output of {factory.__class__.__name__}")

            return factory.read().process().write()

        return factory.read().process()

    def handle_benchmark(self, factory: Factory) -> BenchmarkResult:
        """
        Execute and benchmark a factory.

        Args:
            factory: The factory to execute and benchmark.

        Returns:
            BenchmarkResult: Benchmark results for the factory execution.
        """
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
            elapsed_time,
        )

    def with_spark_session(self: Self, fun: Callable[[SparkSession], Any]) -> Any:
        """
        Execute a function if a SparkSession exists, otherwise return None.

        Args:
            fun: Function to execute with the active SparkSession.

        Returns:
            Any: Result of the function execution, or None if no SparkSession exists.
        """
        spark: Optional[SparkSession] = SparkSession.getActiveSession()

        if spark:
            return fun(spark)

        return None

    def run_factory(self: Self, factory: Factory) -> Deliverable:
        """
        Run a factory and collect its deliverable.

        Sets up job grouping for Spark monitoring and optionally benchmarks the execution.

        Args:
            factory: The factory to run.

        Returns:
            Deliverable: The deliverable produced by the factory.

        TODO: Implement clearJobGroup functionality when PySpark API supports it.
        """
        self.with_spark_session(
            lambda _: _.sparkContext.setJobGroup(
                factory.__class__.__name__, "stage group"
            )
        )

        if self.benchmarked:
            benchmark_result = self.handle_benchmark(factory)

            self.__benchmark_result.append(benchmark_result)
        else:
            self.execute_factory(factory)

        # TODO: Find a way to clearJobGroup since sparkContext method doesn't exist
        # self.with_spark_session(lambda _: _.sparkContext.clearJobGroup())
        return factory.deliverable

    def create_nodes(self: Self) -> list[Node]:
        """
        Create nodes for all factories in this stage.

        Returns:
            list[Node]: List of nodes representing the factories in this stage.
        """
        return [
            Node.from_factory(f, self.__stage_id, self.__end) for f in self.factories
        ]

    def get_benchmarks(self: Self) -> list[BenchmarkResult]:
        """
        Get all benchmark results collected during stage execution.

        Returns:
            list[BenchmarkResult]: List of benchmark results for all factories in this stage.
        """
        return self.__benchmark_result

    def run(self: Self) -> Self:
        """
        Run all factories in this stage sequentially.

        Executes all registered factories in order and collects their deliverables.

        Returns:
            Self: This stage instance for method chaining.

        TODO: Implement parallel execution of factories within the stage.
        """
        self.__deliverable = list(map(self.run_factory, self.factories))

        return self
