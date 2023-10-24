"""ExpectedDelivery Module."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from pysetl.utils import pretty
from .factory import Factory


@dataclass
class ExpectedDeliverable:
    """
    Expected Deliverable metadata.

    A Deliverable metadata conatainer to trace available deliverables in a
    Pipeline
    """

    deliverable_type: str
    delivery_id: str
    producer: type
    consumer: Optional[type[Factory]]

    def __hash__(self) -> int:
        return hash(
            self.deliverable_type +
            self.delivery_id +
            pretty(self.producer) +
            pretty(self.consumer)
        )
