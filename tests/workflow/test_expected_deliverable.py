"""Test pysetl.workflow.expected_deliverable module."""
from abc import ABC
from pysetl.workflow.expected_deliverable import ExpectedDeliverable
from pysetl.workflow import Factory


class ProducerFactory(Factory, ABC):
    """Producer abstract class for testing."""


class ConsumerFactory(Factory, ABC):
    """Consumer abstract class for testing."""


def test_expected_deliverable():
    """Test ExpectedDeliverable."""
    expected_deliverable = ExpectedDeliverable(
        deliverable_type="CitizenModel",
        delivery_id="A citizen",
        producer=ProducerFactory,
        consumer=ConsumerFactory
    )

    assert expected_deliverable
