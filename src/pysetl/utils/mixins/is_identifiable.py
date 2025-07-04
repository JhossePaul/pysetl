"""
IsIdentifiable mixin for PySetl.

Provides an abstract mixin to add identifiability to classes.
"""
from uuid import UUID, uuid4
from pysetl.utils import pretty


class IsIdentifiable:
    """
    Abstract mixin to add identifiability to a class.

    This class cannot be instantiated directly.
    """

    def __init__(self) -> None:
        self.uuid: UUID = uuid4()

    @classmethod
    def name(cls) -> str:
        """Return the name of class in a pretty format."""
        return pretty(cls)
