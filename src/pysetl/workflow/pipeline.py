"""Pipeline module."""
from typing import Any
from typing_extensions import Self
from pysetl.utils import BenchmarkResult, pretty
from pysetl.utils.exceptions import InvalidDeliveryException, PipelineException
from pysetl.utils.mixins import (
    HasRegistry, HasLogger, IsIdentifiable, HasBenchmark,
    HasDiagram
)
from .factory import Factory
from .stage import Stage
from .deliverable import Deliverable
from .dispatcher import Dispatcher
from .inspector import Inspector
from .expected_deliverable import ExpectedDeliverable
from .external import External


PotentialDeliverables = list[tuple[ExpectedDeliverable, ExpectedDeliverable]]


class Pipeline(
    HasRegistry[Stage], IsIdentifiable,
    HasBenchmark, HasDiagram, HasLogger
):
    """Pipeline is a complete data transformation workflow."""

    def __init__(self, benchmark: bool = False) -> None:
        super().__init__()
        IsIdentifiable.__init__(self)
        self.benchmarked = benchmark
        self.inspector = Inspector(self)
        self.dispatcher = Dispatcher()
        self.__stage_counter: int = 0
        self.__benchmark_result: list[BenchmarkResult] = []

    @property
    def stages(self: Self) -> list[Stage]:
        """Return current stages registered in popeline."""
        return list(self.get_registry().values())

    def set_input_from_deliverable(
            self: Self,
            deliverable: Deliverable) -> Self:
        """Register a deliverable Dispatcher."""
        self.dispatcher.add_deliverable(deliverable)

        return self

    def add_stage(self: Self, stage: Stage) -> Self:
        """Register a stage in pipeline."""
        self.log_debug(f"Add stage {self.__stage_counter}")

        self.reset_end_stage()
        stage.stage_id = len(self.get_registry())
        self.register(stage)

        return self

    def reset_end_stage(self: Self) -> None:
        """Reset end of pipeline."""
        last_item = self.last_registered_item

        if last_item:
            last_item.end = False

    def add_stage_from_factory(self: Self, factory: Factory) -> Self:
        """Register a new stage with a single factory."""
        stage = Stage().add_factory(factory)
        self.add_stage(stage)

        return self

    def add_stage_from_type(
            self: Self,
            factory_type: type[Factory],
            *args, **kwargs) -> Self:
        """Instantiate a factory and register a new stage with it."""
        factory = factory_type(*args, **kwargs)
        stage = Stage().add_factory(factory)
        self.add_stage(stage)

        return self

    def describe(self: Self) -> str:
        """Graph representation of the pipeline."""
        self.inspector = Inspector(self)

        return str(self.inspector)

    def to_diagram(self: Self) -> str:
        """Diagram represention of the pipeline."""
        self.inspector = Inspector(self)

        return self.inspector.inspect().graph.to_diagram()

    @property
    def diagram_id(self: Self) -> str:
        """Pipeline has no diagram id."""
        raise NotImplementedError("Pipeline doesn't have a diagram id")

    def get_pipeline_deliverables(self: Self) -> set[ExpectedDeliverable]:
        """Retrive deliverables set directly on pipeline."""
        consumer_deliverables = {
            ExpectedDeliverable(
                deliverable_type=pretty(deliverable.payload_type.tp),
                delivery_id=deliverable.delivery_id,
                producer=pretty(deliverable.producer),
                consumer=pretty(consumer)
            )
            for deliverable
            in self.dispatcher.get_registry().values()
            for consumer
            in deliverable.consumers
        }

        producer_deliverables = {
            ExpectedDeliverable(
                deliverable_type=pretty(deliverable.payload_type.tp),
                delivery_id=deliverable.delivery_id,
                producer=pretty(deliverable.producer),
                consumer=None
            )
            for deliverable
            in self.dispatcher.get_registry().values()
            if len(deliverable.consumers) == 0
        }

        return consumer_deliverables.union(producer_deliverables)

    def get_stages_deliverables(self: Self) -> set[ExpectedDeliverable]:
        """Retrive deliverables available from stages."""
        consumer_deliverables = {
            ExpectedDeliverable(
                deliverable_type=pretty(factory.delivery_type()),
                delivery_id=factory.delivery_id,
                producer=pretty(type(factory)),
                consumer=pretty(consumer)
            )
            for stage
            in self.stages[:-1]
            for factory
            in stage.factories
            for consumer
            in factory.consumers
        }
        producer_deliverables = {
            ExpectedDeliverable(
                deliverable_type=pretty(factory.delivery_type().tp),
                delivery_id=factory.delivery_id,
                producer=pretty(type(factory)),
                consumer=None
            )
            for stage
            in self.stages[:-1]
            for factory
            in stage.factories
        }

        return consumer_deliverables.union(producer_deliverables)

    def get_needed_deliverables(self: Self) -> set[ExpectedDeliverable]:
        """Retrive expected deliverables from each Pipeline node."""
        return {
            ExpectedDeliverable(
                deliverable_type=pretty(delivery.payload_type),
                delivery_id=delivery.delivery_id,
                producer=pretty(delivery.producer),
                consumer=pretty(delivery.consumer)
            )
            for stage
            in self.stages
            for node
            in stage.create_nodes()
            for delivery
            in node.input
        }

    def __compare_deliverables(
            self: Self,
            needed_deliverables: set[ExpectedDeliverable],
            available_deliverables: set[ExpectedDeliverable]
            ) -> PotentialDeliverables:
        """Find potential deliverables for each needed deliverable."""
        return [
            (needed, available)
            for available
            in available_deliverables
            for needed
            in needed_deliverables
            if available.deliverable_type == needed.deliverable_type and
            available.delivery_id == needed.delivery_id and
            (
                True
                if needed.producer == pretty(External)
                else needed.producer == available.producer
            ) and
            (
                available.consumer == needed.consumer or
                not available.consumer
            )
        ]

    def __exists_solution(
            self: Self,
            needed: ExpectedDeliverable,
            potential_deliverables: PotentialDeliverables) -> None:
        """Validate if dependency solution exists."""
        found_by_consumer = list(filter(
            lambda _: _[1].consumer == needed.consumer and
            _[0].deliverable_type == needed.deliverable_type,
            potential_deliverables
        ))
        found_implicit = list(filter(
            lambda _: _[1].consumer is None and
            _[0].deliverable_type == needed.deliverable_type,
            potential_deliverables
        ))

        if not (found_by_consumer or found_implicit):
            delivery_id_str = (
                "''"
                if (needed.delivery_id == "")
                else needed.delivery_id
            )
            raise InvalidDeliveryException(" ".join([
                f"Deliverable of type {needed.deliverable_type}",
                f"with deliveryId {delivery_id_str}",
                f"produced by {pretty(needed.producer)}",
                f"is expected in {pretty(needed.consumer)}."
            ]))

    def run(self: Self) -> Self:
        """Run a pipeline."""
        self.inspector = Inspector(self).inspect()

        needed_deliverables = self.get_needed_deliverables()
        pipeline_deliverables = self.get_pipeline_deliverables()
        stages_output = self.get_stages_deliverables()
        available_deliverables = pipeline_deliverables.union(stages_output)

        potential_deliverables = self.__compare_deliverables(
            needed_deliverables,
            available_deliverables
        )

        _ = [
            self.__exists_solution(needed, potential_deliverables)
            for needed
            in needed_deliverables
        ]

        self.__benchmark_result = [
            benchmark
            for stage
            in self.stages
            for benchmark
            in self.execute_stage(stage)
        ]

        return self

    def execute_stage(self: Self, stage: Stage) -> list[BenchmarkResult]:
        """Dispatch dependencies and run a stage."""
        self.log_info(str(stage))

        if len(self.dispatcher.get_registry()) != 0:
            list(map(self.dispatcher.dispatch, stage.factories))

        stage.benchmarked = self.benchmarked

        stage.run()
        self.dispatcher.collect_stage_deliverables(stage)

        return stage.get_benchmarks()

    def get_last_output(self: Self) -> Any:
        """Return last output from pipeline."""
        if self.last_registered_item:
            return self.last_registered_item.factories[-1].get()

        return None

    def get_output(self: Self, factory_type: type[Factory]) -> Any:
        """Return output from a given factory."""
        factory = [
            factory
            for stage
            in self.stages
            for factory
            in stage.factories
            if isinstance(factory, factory_type)
        ]

        if len(factory) == 0:
            raise PipelineException(
                f"There isn't any class {pretty(factory_type)}"
            )

        return factory[0].get()

    def get_deliverable(
            self: Self,
            deliverable_type: type) -> list[Deliverable]:
        """Find deliverables of a given type."""
        return self.dispatcher.find_deliverable_by_type(deliverable_type)

    def get_benchmarks(self) -> list[BenchmarkResult]:
        """Return collected benchmarks."""
        return self.__benchmark_result
