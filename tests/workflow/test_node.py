"""Test pysetl.workflow.node module."""
from abc import ABC
from collections.abc import Container
from uuid import uuid4
import pytest
from pyspark.sql.types import StringType, IntegerType
from typedspark import DataSet, Schema, Column, ArrayType
from pysetl.workflow import Factory, Delivery
from pysetl.workflow.node import Node
from pysetl.workflow.external import External
from pysetl.workflow.node_output import NodeOutput
from pysetl.utils.exceptions import InvalidDeliveryException


class Producer1(Factory[int], ABC):
    """Factory for testing purposes."""


class Producer2(Factory, ABC):
    """Factory for testing purposes."""


class Producer23(Factory, ABC):
    """Factory for testing purposes."""


class ProducerContainer1(Factory[Container], ABC):
    """Factory for testing purposes."""


class ProducerContainer2(Factory[Container], ABC):
    """Factory for testing purposes."""


class ProducerContainer23(Factory[Container], ABC):
    """Factory for testing purposes."""


class ComplexProduct(Schema):
    """Schema for testing purposes."""

    arg1: Column[StringType]
    arg2: Column[ArrayType[IntegerType]]


class ConcreteProducer(Factory[DataSet[ComplexProduct]]):
    """Concrete factory for testing purposes."""

    input1 = Delivery[str](External, "input1")
    input2 = Delivery[list[int]](External, "input2")

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
        """Nothing to return."""


def test_node():
    """Test Node."""
    factory = ConcreteProducer()
    node = Node.from_factory(factory, 0, True)
    diagram = node.to_diagram()
    [delivery_by_type] = node.find_input_by_type(str, "input1")
    [delivery_by_id] = node.find_input_by_delivery_id("input2")

    assert node.input == factory.expected_deliveries()
    assert node.diagram_id == "ConcreteProducer"
    assert node.producers == [External, External]
    assert delivery_by_type.delivery_id == "input1"
    assert delivery_by_type.producer is External
    assert delivery_by_type.payload_type is str
    assert delivery_by_id.producer is External
    assert delivery_by_id.delivery_id == "input2"
    assert delivery_by_id.payload_type == list[int]
    assert "Factory: ConcreteProducer" in str(node)
    assert "Stage: 0" in str(node)
    assert "Input: External (delivery id: input1)" in str(node)
    assert "Input: External (delivery id: input2)" in str(node)
    assert "Output: DataSet[ComplexProduct]" in str(node)
    assert str(node) == repr(node)
    assert "class ConcreteProducer" in diagram
    assert "<<Factory[DataSet[ComplexProduct]]>>" in diagram
    assert ">arg1: string" in diagram
    assert ">arg2: array<int>" in diagram
    assert "+str" in diagram
    assert "+list" in diagram
    assert "class DataSetComplexProductFinal" in diagram


def test_node_target_node():
    """Test target node method."""
    u1 = uuid4()
    u2 = uuid4()

    stage0 = Node(
        factory_class=Producer1,
        factory_uuid=u1,
        stage=0,
        expected_deliveries=[Delivery[str](External)],
        output=NodeOutput(delivery_type=int, consumer=set([Producer2]))
    )

    stage1 = Node(
        factory_class=Producer2,
        factory_uuid=u2,
        stage=1,
        expected_deliveries=[
            Delivery[int](Producer1),
            Delivery[int](ProducerContainer2)
        ],
        output=NodeOutput(delivery_type=list[str], consumer=set([Producer23]))
    )

    stage1wrong = Node(
        factory_class=Producer23,
        factory_uuid=u2,
        stage=1,
        expected_deliveries=[
            Delivery[int](Producer1),
            Delivery[int](ProducerContainer2)
        ],
        output=NodeOutput(delivery_type=list[str], consumer=set([Producer23]))
    )

    assert stage0.target_node(stage1)
    assert not stage0.target_node(stage1wrong)


def test_target_node_different_stage():
    """Test target_node method when nodes on different stages."""
    u1 = uuid4()
    u2 = uuid4()

    stage0 = Node(
        factory_class=Producer1,
        factory_uuid=u1,
        stage=0,
        expected_deliveries=[Delivery[str](External)],
        output=NodeOutput(delivery_type=int, consumer=set())
    )

    stage1 = Node(
        factory_class=Producer2,
        factory_uuid=u2,
        stage=1,
        expected_deliveries=[
            Delivery[int](Producer1),
            Delivery[int](ProducerContainer2)
        ],
        output=NodeOutput(delivery_type=list[str], consumer=set([Producer23]))
    )

    stage1in0 = Node(
        factory_class=Producer2,
        factory_uuid=u2,
        stage=0,
        expected_deliveries=[
            Delivery[int](Producer1),
            Delivery[int](ProducerContainer2)
        ],
        output=NodeOutput(delivery_type=list[str], consumer=set([Producer23]))
    )

    assert stage0.target_node(stage1)
    assert not stage0.target_node(stage1in0)


def test_target_node_uuid():
    """Test target_node method with uuid."""
    u1 = uuid4()
    u2 = uuid4()

    node1 = Node(
        factory_class=Producer1,
        factory_uuid=u1,
        stage=0,
        expected_deliveries=[Delivery[str](External)],
        output=NodeOutput(delivery_type=int, consumer=set())
    )

    node2 = Node(
        factory_class=Producer2,
        factory_uuid=u2,
        stage=1,
        expected_deliveries=[
            Delivery[int](Producer1),
            Delivery[int](ProducerContainer2)
        ],
        output=NodeOutput(delivery_type=list[str], consumer=set([Producer23]))
    )

    node3 = Node(
        factory_class=Producer2,
        factory_uuid=u1,
        stage=1,
        expected_deliveries=[
            Delivery[int](Producer1),
            Delivery[int](ProducerContainer2)
        ],
        output=NodeOutput(delivery_type=list[str], consumer=set([Producer23]))
    )

    assert node1.target_node(node2)
    assert not node1.target_node(node3)
    assert not node1.target_node(node1)


def test_target_node_io():
    """Test target_node method with uuid."""
    u1 = uuid4()
    u2 = uuid4()

    node1 = Node(
        factory_class=Producer1,
        factory_uuid=u1,
        stage=0,
        expected_deliveries=[Delivery[str](External)],
        output=NodeOutput(delivery_type=int, consumer=set())
    )

    node1_with_consumer = Node(
        factory_class=Producer1,
        factory_uuid=u1,
        stage=0,
        expected_deliveries=[Delivery[str](External)],
        output=NodeOutput(delivery_type=int, consumer=set([Producer2]))
    )

    node1_wrong_consumer = Node(
        factory_class=Producer1,
        factory_uuid=u1,
        stage=0,
        expected_deliveries=[Delivery[str](External)],
        output=NodeOutput(
            delivery_type=int,
            consumer=set([ProducerContainer2])
        )
    )

    node2 = Node(
        factory_class=Producer2,
        factory_uuid=u2,
        stage=1,
        expected_deliveries=[
            Delivery[int](External),
            Delivery[int](External)
        ],
        output=NodeOutput(delivery_type=list[str], consumer=set([Producer23]))
    )

    node21 = Node(
        factory_class=Producer2,
        factory_uuid=u2,
        stage=1,
        expected_deliveries=[
            Delivery[int](External),
            Delivery[int](ProducerContainer2)
        ],
        output=NodeOutput(delivery_type=list[str], consumer=set([Producer23]))
    )

    node21_wrong_type = Node(
        factory_class=Producer2,
        factory_uuid=u2,
        stage=1,
        expected_deliveries=[
            Delivery[float](External),
            Delivery[int](ProducerContainer2)
        ],
        output=NodeOutput(delivery_type=list[str], consumer=set([Producer23]))
    )

    node21_wrong_producer = Node(
        factory_class=Producer2,
        factory_uuid=u2,
        stage=1,
        expected_deliveries=[
            Delivery[int](ProducerContainer1),
            Delivery[int](ProducerContainer2)
        ],
        output=NodeOutput(delivery_type=list[str], consumer=set([Producer23]))
    )

    node22 = Node(
        factory_class=Producer2,
        factory_uuid=u2,
        stage=1,
        expected_deliveries=[
            Delivery[int](Producer1),
            Delivery[int](ProducerContainer2)
        ],
        output=NodeOutput(delivery_type=list[str], consumer=set([Producer23]))
    )

    assert node1.target_node(node21)
    assert not node1.target_node(node21_wrong_type)
    assert node1.producers == [External]

    assert node1_with_consumer.target_node(node21)
    assert not node1_with_consumer.target_node(node21_wrong_type)
    assert not node1_with_consumer.target_node(node21_wrong_producer)

    assert node1.target_node(node22)
    assert node1_with_consumer.target_node(node22)
    assert not node1_wrong_consumer.target_node(node22)
    assert node1_wrong_consumer.producers == [External]

    with pytest.raises(InvalidDeliveryException) as error1:
        node1.target_node(node2)

    with pytest.raises(InvalidDeliveryException) as error2:
        node1_with_consumer.target_node(node2)

    assert error1
    assert error2


def test_target_node_ereasure():
    """Test Node class ereasure."""
    u1 = uuid4()
    u2 = uuid4()
    u3 = uuid4()

    node1 = Node(
        factory_class=ProducerContainer1,
        factory_uuid=u1,
        stage=0,
        expected_deliveries=[Delivery[str](External)],
        output=NodeOutput(delivery_type=int, consumer=set())
    )

    node11 = Node(
        factory_class=ProducerContainer1,
        factory_uuid=u1,
        stage=0,
        expected_deliveries=[Delivery[str](External)],
        output=NodeOutput(
            delivery_type=int,
            consumer=set([ProducerContainer2])
        )
    )

    node2 = Node(
        factory_class=ProducerContainer2,
        factory_uuid=u2,
        stage=1,
        expected_deliveries=[
            Delivery[int](ProducerContainer1),
            Delivery[int](ProducerContainer2)
        ],
        output=NodeOutput(delivery_type=list[str], consumer=set([Producer23]))
    )

    node3 = Node(
        factory_class=ProducerContainer23,
        factory_uuid=u3,
        stage=1,
        expected_deliveries=[
            Delivery[int](ProducerContainer1),
            Delivery[int](ProducerContainer2)
        ],
        output=NodeOutput(delivery_type=list[str], consumer=set([Producer23]))
    )

    node3_string = Node(
        factory_class=ProducerContainer23,
        factory_uuid=u3,
        stage=1,
        expected_deliveries=[
            Delivery[float](ProducerContainer1)
        ],
        output=NodeOutput(delivery_type=list[str], consumer=set([Producer23]))
    )

    assert node1.target_node(node2)
    assert node1.target_node(node3)
    assert node11.target_node(node2)
    assert not node11.target_node(node3)
    assert not node11.target_node(node3_string)


def test_target_node_delivery_id():
    """Test Node class ereasure."""
    u1 = uuid4()
    u2 = uuid4()

    node1 = Node(
        factory_class=Producer1,
        factory_uuid=u1,
        stage=0,
        expected_deliveries=[Delivery[str](External, "id_1")],
        output=NodeOutput(
            delivery_type=Producer1,
            consumer=set(),
            delivery_id="id_2"
        )
    )

    node1_bis = Node(
        factory_class=Producer1,
        factory_uuid=u1,
        stage=0,
        expected_deliveries=[Delivery[str](External, "id_1")],
        output=NodeOutput(
            delivery_type=Producer1,
            consumer=set(),
            delivery_id="id_wrong"
        )
    )

    node1_ter = Node(
        factory_class=Producer1,
        factory_uuid=u1,
        stage=0,
        expected_deliveries=[Delivery[str](External, "id_1")],
        output=NodeOutput(
            delivery_type=Producer1,
            consumer=set(),
            delivery_id="nothing"
        )
    )

    node2 = Node(
        factory_class=ProducerContainer1,
        factory_uuid=u2,
        stage=1,
        expected_deliveries=[Delivery[Producer1](External, "id_2")],
        output=NodeOutput(
            delivery_type=ProducerContainer1,
            consumer=set(),
            delivery_id="id_3"
        )
    )

    node2_bis = Node(
        factory_class=ProducerContainer1,
        factory_uuid=u2,
        stage=1,
        expected_deliveries=[Delivery[Producer1](External, "id_wrong")],
        output=NodeOutput(
            delivery_type=ProducerContainer1,
            consumer=set(),
            delivery_id="id_3"
        )
    )

    node2_ter = Node(
        factory_class=ProducerContainer1,
        factory_uuid=u2,
        stage=1,
        expected_deliveries=[
            Delivery[Producer1](External, "id_wrong"),
            Delivery[Producer1](External, "id_2")
        ],
        output=NodeOutput(
            delivery_type=ProducerContainer1,
            consumer=set(),
            delivery_id="id_3"
        )
    )

    assert node1.target_node(node2)
    assert node1_bis.target_node(node2_bis)
    assert not node1.target_node(node2_bis)
    assert not node1_bis.target_node(node2)
    assert node1.target_node(node2_ter)
    assert node1_bis.target_node(node2_ter)
    assert not node1_ter.target_node(node2_ter)


def test_node_external():
    """Test external node creation."""
    external = Node.external(NodeOutput(delivery_type=int, consumer=set()))

    assert external.factory_class is External
    assert external.stage == -1
    assert not external.expected_deliveries
