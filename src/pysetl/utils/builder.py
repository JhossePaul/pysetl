"""
Abstract Builder module for PySetl.

Defines an abstract base class for builder patterns, supporting build and get
operations.
"""
from abc import ABCMeta, abstractmethod
from typing import Generic, TypeVar
from typing_extensions import Self


T_co = TypeVar("T_co", covariant=True)


class Builder(Generic[T_co], metaclass=ABCMeta):
    """
    Abstract base class for builder patterns.

    All builders must implement build and get methods.
    This class cannot be instantiated directly.
    """

    @abstractmethod
    def build(self: Self) -> Self:
        """
        Abstract method to build an object.

        Returns:
            Self: The builder instance (for chaining).

        Raises:
            NotImplementedError: If not implemented in a subclass.
        """

    @abstractmethod
    def get(self: Self) -> T_co:
        """
        Abstract method to return the built object.

        Returns:
            T_co: The built object.

        Raises:
            NotImplementedError: If not implemented in a subclass.
        """

    def getOrCreate(self: Self) -> T_co:  # pylint: disable=C0103
        """
        Build the object and return its value.

        Returns:
            T_co: The built object.
        """
        return self.build().get()
