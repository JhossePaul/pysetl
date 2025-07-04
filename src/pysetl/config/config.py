"""
Base Config module for PySetl.

Defines the base configuration model and builder for ETL settings, including
validation and utility methods.
"""
from __future__ import annotations
from abc import abstractmethod
from functools import reduce
from typing import Any, TypeVar, Generic
from pydantic import BaseModel
from typing_extensions import Self
from enum import Enum


class BaseConfigModel(BaseModel):
    """
    Base configuration model for validation.

    Attributes:
        storage (Enum): The storage backend type.
    """

    storage: Enum


class ConfigBuilder:
    """
    Builder for Config classes.

    Provides methods to construct Config objects from dictionaries or objects.
    """

    def __init__(self, cls: "type[Config]"):
        """
        Initialize the ConfigBuilder.

        Args:
            cls: The Config class to build.
        """
        self.cls = cls

    def from_dict(self, __config: dict) -> "Config":
        """
        Construct a Config object from a dictionary.

        Args:
            __config: Dictionary of configuration parameters.

        Returns:
            Config: An instance of the Config class.
        """
        return reduce(lambda x, y: x.set(y[0], y[1]), __config.items(), self.cls())

    def from_object(self, __config: object) -> "Config":
        """
        Construct a Config object from an object's attributes.

        Args:
            __config: An object with configuration attributes.

        Returns:
            Config: An instance of the Config class.
        """
        return self.from_dict(
            {x: y for (x, y) in vars(__config).items() if not x.startswith("__")}
        )


T = TypeVar("T", bound=BaseConfigModel)


class Config(Generic[T]):
    """
    Base configuration class for PySetl.

    Provides a flexible interface for managing ETL configuration parameters.
    """

    @classmethod
    def builder(cls) -> ConfigBuilder:
        """
        Create a builder for this Config class.

        Returns:
            ConfigBuilder: A builder instance for this Config class.
        """
        return ConfigBuilder(cls)

    def __init__(self, **kwargs: Any) -> None:
        """
        Initialize the Config object.

        Args:
            **kwargs: Arbitrary keyword arguments for configuration parameters.
        """
        self.params: dict = kwargs

    def __str__(self) -> str:
        """
        Return a string representation of the configuration.

        Returns:
            str: Human-readable string of configuration parameters.
        """
        settings_string: str = "\n".join(
            [f"  {k}: {v}" for k, v in self.params.items()]
        )

        return f"PySetl {self.__class__.__name__}:\n{settings_string}"

    def __repr__(self) -> str:
        """
        Return the string representation for debugging.

        Returns:
            str: Human-readable string of configuration parameters.
        """
        return str(self)

    def __or__(self, other: Any) -> "Config":
        """
        Merge two Config objects using the | operator.

        Args:
            other: Another Config object.

        Returns:
            Config: A new Config object with merged parameters.
        """
        if not isinstance(other, Config):
            return NotImplemented

        return self.builder().from_dict(self.params | other.params)

    def __contains__(self, __value: str) -> bool:
        """
        Check if a value is present in the configuration parameters.

        Args:
            __value: The value to check for.

        Returns:
            bool: True if present, False otherwise.
        """
        return __value in self.params

    def set(self: Self, key: str, value: Any) -> Self:
        """
        Set a value for a key in the configuration.

        Args:
            key: The configuration key.
            value: The value to set.

        Returns:
            Self (for chaining).
        """
        self.params[key] = value

        return self

    def get(self: Self, key: str) -> Any:
        """
        Get the value for a key in the configuration.

        Args:
            key: The configuration key.

        Returns:
            The value associated with the key.
        """
        return self.params[key]

    def has(self: Self, key: str) -> bool:
        """
        Check if the configuration contains a specific key.

        Args:
            key: The configuration key.

        Returns:
            bool: True if the key exists, False otherwise.
        """
        return key in self.params

    @property
    def config_dict(self: Self) -> dict:
        """
        Return a validated dictionary with the configuration.

        Returns:
            dict: The validated configuration dictionary.
        """
        return vars(self.config)

    @property
    @abstractmethod
    def config(self: Self) -> T:
        """
        Expose the validated config object.

        Returns:
            T: The validated config model.
        """

    @property
    @abstractmethod
    def reader_config(self) -> dict:
        """
        Returns the reader configuration dictionary.

        Returns:
            dict: Reader configuration.
        """

    @property
    @abstractmethod
    def writer_config(self) -> dict:
        """
        Returns the writer configuration dictionary.

        Returns:
            dict: Writer configuration.
        """
