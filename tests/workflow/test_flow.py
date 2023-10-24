"""Test pysetl.workflow.flow module."""
from dataclasses import dataclass
from pysetl.workflow import Factory, Delivery
from pysetl.workflow.flow import Flow
from pysetl.workflow.external import External
from pysetl.workflow.node import Node
from pysetl.workflow.node_output import NodeOutput


@dataclass
class Product:
    """Class for testing purposes."""

    id: str


@dataclass
class Product2:
    """Class for testing purposes."""

    id: str


class Producer(Factory[Product]):
    """Class for testing purposes."""

    id_delivery = Delivery[str](External)
    id = None
    output = None

    def read(self):
        """Get id from external source."""
        self.id = self.id_delivery.get()

        return self

    def process(self):
        """Get id from external source and create a Product."""
        self.output = Product(self.id) if self.id else None

        return self

    def write(self):
        """Return itself."""
        return self

    def get(self):
        """Return processed data."""
        return self.output


class Consumer(Factory[Product2]):
    """Class for testing purposes."""

    input = Delivery[Product](Producer)
    output = None

    def read(self):
        """Return itself."""
        return self

    def process(self):
        """Return itself."""
        return self

    def write(self):
        """Return itself."""
        return self

    def get(self):
        """Return itself."""
        return self.output


node_external = Node.external(NodeOutput(
    delivery_type=str,
    consumer=set(),
    delivery_id="",
    external=True
))
node_producer = Node.from_factory(Producer(), 0, False)
node_consumer = Node.from_factory(Consumer(), 1, False)


def test_flow():
    """Test flow from node to node."""
    flow_external = Flow(node_external, node_producer)
    flow_internal = Flow(node_producer, node_consumer)

    assert flow_internal.delivery_id == ""
    assert flow_internal.stage == 0
    assert flow_internal.payload is Product
    assert flow_internal.diagram_id == ""
    assert str(flow_internal) == "Flow[Producer -> Consumer]"
    assert "Stage       : 0" in repr(flow_internal)
    assert "Direction   : Producer ==> Consumer" in repr(flow_internal)
    assert "PayLoad     : Product" in repr(flow_internal)
    assert flow_internal.to_diagram() == "Consumer<|--Product:Input"
    assert flow_external.to_diagram() == "Producer<|--strExternal:Input"
