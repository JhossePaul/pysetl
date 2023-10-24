"""Abstract Builder module."""
from abc import ABCMeta, abstractmethod
from typing import Generic, TypeVar, Optional
from typing_extensions import Self


T_co = TypeVar("T_co", covariant=True)


class Builder(Generic[T_co], metaclass=ABCMeta):
    """Abstract base Builder class."""

    @abstractmethod
    def build(self: Self) -> Self:
        """Build an object."""

    @abstractmethod
    def get(self: Self) -> Optional[T_co]:
        """Return built object."""

    def getOrCreate(self: Self) -> T_co:  # pylint: disable=C0103
        """Build the object and returns its value."""
        return self.build().get()
