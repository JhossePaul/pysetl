"""
HasRegistryMixin module
"""

from typing import TypeVar, Generic, Optional, Any
from pysetl.exceptions.already_exists_exception import AlreadyExistsException
from pysetl.mixins.identifiable import Identifiable
from uuid import UUID

T = TypeVar("T", Any, Identifiable)


class HasRegistryMixin(Generic[T]):
    __registry: dict[UUID, T] = dict()

    def is_registered(self, item: T) -> bool:
        return item.getUUID() in self.__registry.keys()

    def clear_registry(self):
        self.__registry = dict()

        return self

    def register_items(self, items: list[T]):
        for item in items:
            self.register(item)

        return self

    def register(self, item: T):
        if self.is_registered(item):
            raise AlreadyExistsException(f"""
                The item {item.getUUID()}
                of type {item.get_type()}
                already exists
            """)
        else:
            self.__registry[item.getUUID()] = item

        return self

    def get_registry(self) -> dict[UUID, T]:
        return self.__registry

    def get_item(self, uuid: UUID) -> Optional[T]:
        return self.__registry.get(uuid)
