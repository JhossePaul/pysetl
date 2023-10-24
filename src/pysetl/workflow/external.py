"""External module."""
from abc import ABCMeta
from typing import final
from pysetl.utils.mixins import IsIdentifiable


@final
class External(IsIdentifiable, metaclass=ABCMeta):
    """External is the default deliverable producer."""

    @classmethod
    def delivery_type(cls) -> type:
        """Return delivery type of this class."""
        return External
