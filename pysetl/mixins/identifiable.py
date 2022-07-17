from uuid import UUID, uuid4


class Identifiable:
    def getUUID(self) -> UUID:
        return uuid4()

    def get_type(self) -> type:
        return type(self)
