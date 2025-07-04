"""
Pipeline module for PySetl.

Defines the Pipeline class, which is the main orchestrator and entry point for ETL workflows.
The Pipeline brings together all workflow components (stages, factories, inspector, dispatcher)
and provides a unified interface for building, validating, and executing complete data
transformation workflows.

The Pipeline is responsible for:
- Orchestrating the complete ETL workflow execution
- Managing stages and their sequential execution
- Validating dependencies before execution to prevent runtime errors
- Integrating the Inspector for DAG construction and validation
- Coordinating the Dispatcher for dependency injection
- Providing output access and benchmarking capabilities
- Ensuring the workflow is executable and all dependencies are satisfied

The Pipeline acts as the central coordinator that validates the entire workflow structure
before execution and then orchestrates the runtime execution of all stages and factories.
"""
from typing import Any
from typing_extensions import Self
from pysetl.utils import BenchmarkResult, pretty
from pysetl.utils.exceptions import InvalidDeliveryException, PipelineException
from pysetl.utils.mixins import (
    HasRegistry,
    HasLogger,
    IsIdentifiable,
    HasBenchmark,
    HasDiagram,
)
from .factory import Factory
from .stage import Stage
from .deliverable import Deliverable
from .dispatcher import Dispatcher
from .inspector import Inspector
from .expected_deliverable import ExpectedDeliverable
from .external import External


PotentialDeliverables = list[tuple[ExpectedDeliverable, ExpectedDeliverable]]


class Pipeline(HasRegistry[Stage], IsIdentifiable, HasBenchmark, HasDiagram, HasLogger):
    """
    Main orchestrator and entry point for ETL workflows in PySetl.

    The Pipeline is the central coordinator that brings together all workflow components
    and provides a unified interface for building, validating, and executing complete
    data transformation workflows. It orchestrates the entire ETL process from dependency
    validation through execution and output delivery.

    The Pipeline integrates several key components:
    - **Stages**: Sequential execution units containing factories
    - **Inspector**: DAG construction and validation system
    - **Dispatcher**: Runtime dependency injection system
    - **Factories**: Individual ETL logic units

    Before execution, the Pipeline performs comprehensive dependency validation to ensure
    all required inputs are available either from internal flows or external sources.
    During execution, it orchestrates stage execution, dependency injection, and result
    collection.

    The Pipeline provides multiple ways to add stages and factories, supports benchmarking,
    and offers various output access methods for retrieving results from the workflow.

    Attributes:
        benchmarked (bool): Whether benchmarking is enabled for the pipeline.
        inspector (Inspector): The DAG construction and validation system.
        dispatcher (Dispatcher): The runtime dependency injection system.
    """

    def __init__(self, benchmark: bool = False) -> None:
        """
        Initialize a new Pipeline with optional benchmarking.

        Args:
            benchmark (bool): Whether to enable benchmarking for factories in this pipeline.
                             Defaults to False.
        """
        super().__init__()
        IsIdentifiable.__init__(self)
        self.benchmarked = benchmark
        self.inspector = Inspector(self)
        self.dispatcher = Dispatcher()
        self.__stage_counter: int = 0
        self.__benchmark_result: list[BenchmarkResult] = []

    @property
    def stages(self: Self) -> list[Stage]:
        """
        Get all stages registered in the pipeline.

        Returns:
            list[Stage]: List of all stages in execution order.
        """
        return list(self.get_registry().values())

    def set_input_from_deliverable(self: Self, deliverable: Deliverable) -> Self:
        """
        Register an external deliverable in the dispatcher.

        This method allows external data to be injected into the pipeline,
        making it available for dependency injection to factories that need it.

        Args:
            deliverable (Deliverable): The external deliverable to register.

        Returns:
            Self: This pipeline instance for method chaining.
        """
        self.dispatcher.add_deliverable(deliverable)

        return self

    def add_stage(self: Self, stage: Stage) -> Self:
        """
        Register a stage in the pipeline.

        Adds a stage to the pipeline and assigns it the next available stage ID.
        The previous stage's end flag is reset to ensure proper stage ordering.

        Args:
            stage (Stage): The stage to add to the pipeline.

        Returns:
            Self: This pipeline instance for method chaining.
        """
        self.log_debug(f"Add stage {self.__stage_counter}")

        self.reset_end_stage()
        stage.stage_id = len(self.get_registry())
        self.register(stage)

        return self

    def reset_end_stage(self: Self) -> None:
        """
        Reset the end flag of the last registered stage.

        This ensures that when a new stage is added, the previous stage
        is no longer marked as the end stage.
        """
        last_item = self.last_registered_item

        if last_item:
            last_item.end = False

    def add_stage_from_factory(self: Self, factory: Factory) -> Self:
        """
        Create and add a new stage containing a single factory.

        This is a convenience method that creates a stage with one factory
        and adds it to the pipeline.

        Args:
            factory (Factory): The factory to add in a new stage.

        Returns:
            Self: This pipeline instance for method chaining.
        """
        stage = Stage().add_factory(factory)
        self.add_stage(stage)

        return self

    def add_stage_from_type(
        self: Self, factory_type: type[Factory], *args: Any, **kwargs: Any
    ) -> Self:
        """
        Instantiate a factory from its type and add it in a new stage.

        This is a convenience method that instantiates a factory with the given
        arguments and adds it to the pipeline in a new stage.

        Args:
            factory_type (type[Factory]): The factory class to instantiate.
            *args: Positional arguments to pass to the factory constructor.
            **kwargs: Keyword arguments to pass to the factory constructor.

        Returns:
            Self: This pipeline instance for method chaining.
        """
        factory = factory_type(*args, **kwargs)
        stage = Stage().add_factory(factory)
        self.add_stage(stage)

        return self

    def describe(self: Self) -> str:
        """
        Get a text representation of the pipeline structure.

        Creates a new inspector and returns a comprehensive summary of the
        pipeline's structure, including nodes and flows.

        Returns:
            str: Detailed text representation of the pipeline structure.
        """
        self.inspector = Inspector(self)

        return str(self.inspector)

    def to_diagram(self: Self) -> str:
        """
        Generate a Mermaid diagram representation of the pipeline.

        Creates a new inspector, performs inspection, and generates a
        visual diagram showing the DAG structure with nodes and flows.

        Returns:
            str: Mermaid diagram representation of the pipeline DAG.
        """
        self.inspector = Inspector(self)

        return self.inspector.inspect().graph.to_diagram()

    @property
    def diagram_id(self: Self) -> str:
        """
        Get the diagram ID for the pipeline.

        Raises:
            NotImplementedError: Pipeline doesn't have a diagram ID.
        """
        raise NotImplementedError("Pipeline doesn't have a diagram id")

    def get_pipeline_deliverables(self: Self) -> set[ExpectedDeliverable]:
        """
        Get deliverables that are directly set on the pipeline.

        Retrieves all deliverables registered in the dispatcher and converts
        them to ExpectedDeliverable format for dependency analysis.

        Returns:
            set[ExpectedDeliverable]: Set of pipeline-level deliverables.
        """
        consumer_deliverables = {
            ExpectedDeliverable(
                deliverable_type=pretty(deliverable.payload_type.tp),
                delivery_id=deliverable.delivery_id,
                producer=pretty(deliverable.producer),
                consumer=pretty(consumer),
            )
            for deliverable in self.dispatcher.get_registry().values()
            for consumer in deliverable.consumers
        }

        producer_deliverables = {
            ExpectedDeliverable(
                deliverable_type=pretty(deliverable.payload_type.tp),
                delivery_id=deliverable.delivery_id,
                producer=pretty(deliverable.producer),
                consumer=None,
            )
            for deliverable in self.dispatcher.get_registry().values()
            if len(deliverable.consumers) == 0
        }

        return consumer_deliverables.union(producer_deliverables)

    def get_stages_deliverables(self: Self) -> set[ExpectedDeliverable]:
        """
        Get deliverables that will be available from stages.

        Analyzes all factories in all stages (except the last) to determine
        what deliverables they will produce, converting them to ExpectedDeliverable
        format for dependency analysis.

        Returns:
            set[ExpectedDeliverable]: Set of deliverables that stages will produce.
        """
        consumer_deliverables = {
            ExpectedDeliverable(
                deliverable_type=pretty(factory.delivery_type()),
                delivery_id=factory.delivery_id,
                producer=pretty(type(factory)),
                consumer=pretty(consumer),
            )
            for stage in self.stages[:-1]
            for factory in stage.factories
            for consumer in factory.consumers
        }
        producer_deliverables = {
            ExpectedDeliverable(
                deliverable_type=pretty(factory.delivery_type().tp),
                delivery_id=factory.delivery_id,
                producer=pretty(type(factory)),
                consumer=None,
            )
            for stage in self.stages[:-1]
            for factory in stage.factories
        }

        return consumer_deliverables.union(producer_deliverables)

    def get_needed_deliverables(self: Self) -> set[ExpectedDeliverable]:
        """
        Get all deliverables needed by pipeline nodes.

        Analyzes all deliveries (dependencies) declared by all factories in all
        stages to determine what deliverables are required for execution.

        Returns:
            set[ExpectedDeliverable]: Set of all deliverables needed by the pipeline.
        """
        return {
            ExpectedDeliverable(
                deliverable_type=pretty(delivery.payload_type),
                delivery_id=delivery.delivery_id,
                producer=pretty(delivery.producer),
                consumer=pretty(delivery.consumer),
            )
            for stage in self.stages
            for node in stage.create_nodes()
            for delivery in node.input
        }

    def __compare_deliverables(
        self: Self,
        needed_deliverables: set[ExpectedDeliverable],
        available_deliverables: set[ExpectedDeliverable],
    ) -> PotentialDeliverables:
        """
        Find potential matches between needed and available deliverables.

        Compares needed deliverables against available ones to find potential
        matches based on type, delivery ID, producer, and consumer constraints.

        Args:
            needed_deliverables (set[ExpectedDeliverable]): Deliverables required by the pipeline.
            available_deliverables (set[ExpectedDeliverable]): Deliverables available to the pipeline.

        Returns:
            PotentialDeliverables: List of (needed, available) pairs that could match.
        """
        return [
            (needed, available)
            for available in available_deliverables
            for needed in needed_deliverables
            if available.deliverable_type == needed.deliverable_type
            and available.delivery_id == needed.delivery_id
            and (
                True
                if needed.producer == pretty(External)
                else needed.producer == available.producer
            )
            and (available.consumer == needed.consumer or not available.consumer)
        ]

    def __exists_solution(
        self: Self,
        needed: ExpectedDeliverable,
        potential_deliverables: PotentialDeliverables,
    ) -> None:
        """
        Validate that a solution exists for a needed deliverable.

        Checks if there are any potential matches for a specific needed deliverable,
        considering both explicit consumer matches and implicit matches (where
        consumer is None).

        Args:
            needed (ExpectedDeliverable): The deliverable that needs a solution.
            potential_deliverables (PotentialDeliverables): List of potential matches.

        Raises:
            InvalidDeliveryException: If no solution is found for the needed deliverable.
        """
        found_by_consumer = list(
            filter(
                lambda _: _[1].consumer == needed.consumer
                and _[0].deliverable_type == needed.deliverable_type,
                potential_deliverables,
            )
        )
        found_implicit = list(
            filter(
                lambda _: _[1].consumer is None
                and _[0].deliverable_type == needed.deliverable_type,
                potential_deliverables,
            )
        )

        if not (found_by_consumer or found_implicit):
            delivery_id_str = "''" if (needed.delivery_id == "") else needed.delivery_id
            raise InvalidDeliveryException(
                " ".join(
                    [
                        f"Deliverable of type {needed.deliverable_type}",
                        f"with deliveryId {delivery_id_str}",
                        f"produced by {pretty(needed.producer)}",
                        f"is expected in {pretty(needed.consumer)}.",
                    ]
                )
            )

    def run(self: Self) -> Self:
        """
        Execute the complete pipeline workflow.

        This is the main entry point for pipeline execution. The method performs
        comprehensive validation before execution and then orchestrates the
        execution of all stages.

        The execution process:
        1. Inspects the pipeline to construct and validate the DAG
        2. Analyzes all needed and available deliverables
        3. Validates that all dependencies can be satisfied
        4. Executes all stages sequentially
        5. Collects benchmark results if enabled

        Returns:
            Self: This pipeline instance for method chaining.

        Raises:
            InvalidDeliveryException: If any dependencies cannot be satisfied.
        """
        self.inspector = Inspector(self).inspect()

        needed_deliverables = self.get_needed_deliverables()
        pipeline_deliverables = self.get_pipeline_deliverables()
        stages_output = self.get_stages_deliverables()
        available_deliverables = pipeline_deliverables.union(stages_output)

        potential_deliverables = self.__compare_deliverables(
            needed_deliverables, available_deliverables
        )

        _ = [
            self.__exists_solution(needed, potential_deliverables)
            for needed in needed_deliverables
        ]

        self.__benchmark_result = [
            benchmark
            for stage in self.stages
            for benchmark in self.execute_stage(stage)
        ]

        return self

    def execute_stage(self: Self, stage: Stage) -> list[BenchmarkResult]:
        """
        Execute a single stage with dependency injection.

        Dispatches dependencies to all factories in the stage, runs the stage,
        and collects deliverables for use by downstream stages.

        Args:
            stage (Stage): The stage to execute.

        Returns:
            list[BenchmarkResult]: Benchmark results from the stage execution.
        """
        self.log_info(str(stage))

        if len(self.dispatcher.get_registry()) != 0:
            list(map(self.dispatcher.dispatch, stage.factories))

        stage.benchmarked = self.benchmarked

        stage.run()
        self.dispatcher.collect_stage_deliverables(stage)

        return stage.get_benchmarks()

    def get_last_output(self: Self) -> Any:
        """
        Get the output from the last factory in the pipeline.

        Returns:
            Any: The output from the last factory, or None if no stages exist.
        """
        if self.last_registered_item:
            return self.last_registered_item.factories[-1].get()

        return None

    def get_output(self: Self, factory_type: type[Factory]) -> Any:
        """
        Get the output from a specific factory type.

        Searches through all stages to find a factory of the specified type
        and returns its output.

        Args:
            factory_type (type[Factory]): The type of factory to get output from.

        Returns:
            Any: The output from the specified factory.

        Raises:
            PipelineException: If no factory of the specified type is found.
        """
        factory = [
            factory
            for stage in self.stages
            for factory in stage.factories
            if isinstance(factory, factory_type)
        ]

        if len(factory) == 0:
            raise PipelineException(f"There isn't any class {pretty(factory_type)}")

        return factory[0].get()

    def get_deliverable(self: Self, deliverable_type: type) -> list[Deliverable]:
        """
        Find all deliverables of a specific type.

        Args:
            deliverable_type (type): The type of deliverable to search for.

        Returns:
            list[Deliverable]: List of deliverables of the specified type.
        """
        return self.dispatcher.find_deliverable_by_type(deliverable_type)

    def get_benchmarks(self) -> list[BenchmarkResult]:
        """
        Get all benchmark results from the pipeline execution.

        Returns:
            list[BenchmarkResult]: List of benchmark results from all stages.
        """
        return self.__benchmark_result
