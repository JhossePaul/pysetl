"""Test pysetl.workflow.inpsector module."""
import pytest
from pysetl.workflow import Pipeline, Factory
from pysetl.workflow import Delivery, Deliverable
from pysetl.workflow.inspector import Inspector
from pysetl.utils.exceptions import PipelineException
from pysetl.workflow.external import External


class MultipleInputFactory(Factory[str]):
    """Class for testing purposes."""

    delivery_id = "MultipleInputFactory"
    str1_delivery = Delivery[str](delivery_id="1")
    str2_delivery = Delivery[str](delivery_id="2")
    str1 = ""
    str2 = ""
    output = ""

    def read(self):
        """Read deliveries and return itself."""
        self.str1 = self.str1_delivery.get()
        self.str2 = self.str2_delivery.get()

        return self

    def process(self):
        """Process and return itself."""
        self.output = "0" + self.str1 + self.str2

        return self

    def write(self):
        """Return itself."""
        return self

    def get(self):
        """Return output."""
        return self.output


class LastFactory(Factory[str]):
    """Class for testing purposes."""

    string_delivery = Delivery[str](delivery_id="MultipleInputFactory")
    string = ""

    def read(self):
        """Read delivery and return itself."""
        self.string = self.string_delivery.get()

        return self

    def process(self):
        """Return itself."""
        return self

    def write(self):
        """Return itself."""
        return self

    def get(self):
        """Return output."""
        return self.string


class WrongLastFactory(Factory[str]):
    """Class for testing purposes."""

    string_delivery = Delivery[str]()
    string = ""

    def read(self):
        """Read delivery and return itself."""
        self.string = self.string_delivery.get()

        return self

    def process(self):
        """Return itself."""
        return self

    def write(self):
        """Return itself."""
        return self

    def get(self):
        """Return output."""
        return self.string


def test_inpsector():
    """Test Inspector with multiple inputs with same type differnt ids."""
    str1_deliverable = Deliverable[str]("string_1", delivery_id="1")
    str2_deliverable = Deliverable[str]("string_2", delivery_id="2")
    factory = MultipleInputFactory()
    pipeline = (
        Pipeline()
        .set_input_from_deliverable(str1_deliverable)
        .set_input_from_deliverable(str2_deliverable)
        .add_stage_from_factory(factory)
        .add_stage_from_type(LastFactory)
    )

    inspector = Inspector(pipeline).inspect()

    assert len(inspector.graph.find_deliveries(factory)) == 2
    assert all([
        delivery.producer is External
        for delivery
        in inspector.graph.find_deliveries(factory)
    ])
    assert "Flow[External -> MultipleInputFactory]" in repr(inspector.graph)
    assert "Flow[External -> MultipleInputFactory]" in repr(inspector.graph)
    assert "Flow[MultipleInputFactory -> LastFactory]" in repr(inspector.graph)

    assert not inspector.find_node(WrongLastFactory())
    assert len(inspector.nodes) == 2
    assert len(inspector.flows) == 3
    assert (
        "LastFactory<|--strMultipleinputfactory:Input" in
        inspector.graph.to_diagram()
    )
    assert "========== Pipeline Summary ==========" in str(inspector)
    assert str(inspector) == repr(inspector)

    with pytest.raises(NotImplementedError) as error:
        _ = inspector.graph.diagram_id

    assert str(error.value) == "DAG doesn't have diagram id"


def test_inspector_exceptions():
    """Throws PipelineException when multiple factories found."""
    str1_deliverable = Deliverable[str]("string_1", delivery_id="1")
    str2_deliverable = Deliverable[str]("string_2", delivery_id="2")
    factory = MultipleInputFactory()
    pipeline = (
        Pipeline()
        .set_input_from_deliverable(str1_deliverable)
        .set_input_from_deliverable(str2_deliverable)
        .add_stage_from_factory(factory)
        .add_stage_from_factory(factory)
    )

    with pytest.raises(PipelineException) as error:
        Inspector(pipeline)

    assert str(error.value) == "Multiple factories found"
