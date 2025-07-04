"""
ExpectedDeliverable module for PySetl.

Defines the ExpectedDeliverable class, which provides a string-based
representation of deliverable metadata for dependency analysis and validation.
ExpectedDeliverable avoids type conflicts by using string representations
instead of actual types, enabling reliable set-based operations for comparing
available vs needed deliverables in the pipeline.

This class is used by the Pipeline for dependency validation, ensuring that all
required inputs are available before execution begins.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from pysetl.utils import pretty


@dataclass
class ExpectedDeliverable:
    """
    String-based representation of deliverable metadata for dependency analysis.

    ExpectedDeliverable provides a string-based representation of deliverable metadata
    to avoid type conflicts during dependency analysis. It uses string representations
    of types, producers, and consumers, enabling reliable set-based operations for
    comparing available vs needed deliverables in the pipeline.

    This class is used by the Pipeline to validate that all required dependencies
    are available before execution begins, preventing runtime dependency errors.

    Attributes:
        deliverable_type (str): String representation of the deliverable type.
        delivery_id (str): Optional identifier for the deliverable.
        producer (type): The producer class (converted to string for comparison).
        consumer (Optional[type[Factory]]): The consumer factory class (converted to string for comparison).
    """

    deliverable_type: str
    delivery_id: str
    producer: str
    consumer: Optional[str]

    def __hash__(self) -> int:
        """
        Generate a hash for set-based operations.

        Creates a hash based on all attributes to enable reliable set membership
        testing and deduplication.

        Returns:
            int: Hash value for this expected deliverable.
        """
        return hash(
            self.deliverable_type
            + self.delivery_id
            + pretty(self.producer)
            + pretty(self.consumer)
        )
