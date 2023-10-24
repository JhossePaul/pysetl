from uuid import UUID, uuid4
from pysetl.utils import pretty


class IsIdentifiable:
    def __init__(self) -> None:
        self.uuid: UUID = uuid4()

    @classmethod
    def name(cls) -> str:
        return pretty(cls)
