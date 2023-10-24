"""
IsConfigurable Module
"""
from abc import ABCMeta, abstractmethod
from typing import Any
from typing_extensions import Self


class IsConfigurable(metaclass=ABCMeta):
    """
    IsConfigurable mixin
    """
    @abstractmethod
    def get(self: Self, key: str) -> Any:
        """
        Returns the value of the key in the mapping
        """

    @abstractmethod
    def set(self: Self, key: str, value: Any) -> Self:
        """
        Sets a value to a key in the mapping
        """
