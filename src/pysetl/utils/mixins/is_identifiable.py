"""IsIdentifiable module."""
from uuid import UUID, uuid4
from pysetl.utils import pretty


class IsIdentifiable:
    """Mixin class to provide an UUID to child object."""

    def __init__(self) -> None:
        self.uuid: UUID = uuid4()

    @classmethod
    def name(cls) -> str:
        """Return the name of class in a pretty format."""
        return pretty(cls)
