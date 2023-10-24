"""Test pysetl.workflow.external module."""
from pysetl.workflow.external import External


def test_external():
    """Test External singleton."""
    assert External().delivery_type() is External
