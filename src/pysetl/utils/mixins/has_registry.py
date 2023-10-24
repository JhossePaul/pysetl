"""HasRegistryMixin module."""
from typing import TypeVar, Generic, Optional
from uuid import UUID
from typing_extensions import Self
from pysetl.utils.exceptions import AlreadyExistsException
from pysetl.utils.mixins.is_identifiable import IsIdentifiable


T = TypeVar("T", bound=IsIdentifiable)


class HasRegistry(Generic[T]):
    def __init__(self):
        self._registry: dict[UUID, T] = {}

    def is_registered(self, item: T) -> bool:
        return item.uuid in self._registry.keys()

    def clear_registry(self):
        self._registry = dict()

        return self

    def register_items(self, items: list[T]):
        for item in items:
            self.register(item)

        return self

    def register(self, item: T):
        if self.is_registered(item):
            raise AlreadyExistsException(
                f"The item {item.uuid} of type {item.name()} already exists"
            )
        else:
            self._registry[item.uuid] = item

        return self

    def get_registry(self) -> dict[UUID, T]:
        return self._registry

    def get_item(self, uuid: UUID) -> Optional[T]:
        return self._registry.get(uuid, None)

    @property
    def last_registered_item(self: Self) -> Optional[T]:
        if len(self._registry) == 0:
            return None
        else:
            return list(self._registry.items())[-1][1]

    @property
    def size(self: Self) -> int:
        return len(self._registry)
