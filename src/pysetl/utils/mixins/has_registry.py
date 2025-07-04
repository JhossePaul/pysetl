"""
HasRegistry mixin for PySetl.

Provides a mixin to add registry functionality to classes, allowing registration and retrieval by UUID.
"""
from typing import TypeVar, Generic, Optional
from uuid import UUID
from typing_extensions import Self
from pysetl.utils.exceptions import AlreadyExistsException
from pysetl.utils.mixins.is_identifiable import IsIdentifiable


T = TypeVar("T", bound=IsIdentifiable)


class HasRegistry(Generic[T]):
    """
    Mixin to add registry functionality to a class.

    Provides methods to register, retrieve, and manage objects by UUID.
    """

    def __init__(self) -> None:
        self._registry: dict[UUID, T] = {}

    def is_registered(self, item: T) -> bool:
        """Check if item is registered."""
        return item.uuid in self._registry

    def clear_registry(self):
        """Restore registry."""
        self._registry = {}

        return self

    def register_items(self, items: list[T]):
        """Register multiple items."""
        for item in items:
            self.register(item)

        return self

    def register(self, item: T):
        """
        Register an object in the registry.

        Args:
            item: The object to register.
        """
        if self.is_registered(item):
            raise AlreadyExistsException(
                f"The item {item.uuid} of type {item.name()} already exists"
            )

        self._registry[item.uuid] = item

        return self

    def get_registry(self) -> dict[UUID, T]:
        """
        Get the registry dictionary.

        Returns:
            dict: The registry mapping UUIDs to objects.
        """
        return self._registry

    def get_item(self, uuid: UUID) -> Optional[T]:
        """Get an item by its UUID."""
        return self._registry.get(uuid, None)

    @property
    def last_registered_item(self: Self) -> Optional[T]:
        """Return last registered item."""
        if len(self._registry) == 0:
            return None

        return list(self._registry.items())[-1][1]

    @property
    def size(self: Self) -> int:
        """Return dize of the registry."""
        return len(self._registry)

    def unregister(self, uuid: UUID):
        """
        Unregister an object from the registry by UUID.

        Args:
            uuid: The UUID of the object to unregister.
        """
        self._registry.pop(uuid, None)
