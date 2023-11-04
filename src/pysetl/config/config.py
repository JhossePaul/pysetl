"""Base Config module."""
from __future__ import annotations
from abc import abstractmethod
from functools import reduce
from typing import Any, TypeVar, Generic
from pydantic import BaseModel
from typing_extensions import Self
from pysetl.enums import (
    FileStorage,
)


class BaseConfigModel(BaseModel):
    """BaseConfig validation."""

    storage: FileStorage


class ConfigBuilder:
    """Builder methods for Config class."""

    def __init__(self, cls):
        self.cls = cls

    def from_dict(self, __config: dict) -> Config:
        """Config constructor from a dict."""
        return reduce(
            lambda x, y: x.set(y[0], y[1]),
            __config.items(),
            self.cls()
        )

    def from_object(self, __config: object) -> Config:
        """Config constructor from an object."""
        return self.from_dict({
            x: y
            for (x, y)
            in vars(__config).items()
            if not x.startswith("__")}
        )


T = TypeVar("T", bound=BaseConfigModel)


class Config(Generic[T]):
    """Base Config class."""

    @classmethod
    def builder(cls) -> ConfigBuilder:
        """Create builder."""
        return ConfigBuilder(cls)

    def __init__(self, **kwargs) -> None:
        self.params: dict = kwargs

    def __str__(self) -> str:
        settings_string: str = "\n".join([
            f"  {k}: {v}"
            for k, v
            in self.params.items()
        ])

        return f"PySetl {self.__class__.__name__}:\n{settings_string}"

    def __repr__(self) -> str:
        return str(self)

    def __or__(self, other: Config):
        if not isinstance(other, Config):
            return NotImplemented

        return self.builder().from_dict(self.params | other.params)

    def __contains__(self, __value) -> bool:
        return __value in self.params

    def set(self: Self, key: str, value: Any) -> Self:
        """Set a value to a key in the settings."""
        self.params[key] = value

        return self

    def get(self: Self, key: str) -> Any:
        """Return the value of the key in the settings."""
        return self.params[key]

    def has(self: Self, key: str) -> bool:
        """Check if settings contains a specific value."""
        return key in self.params

    @property
    def config_dict(self: Self) -> dict:
        """Return a validated dictionary with the configuration."""
        return vars(self.config)

    @property
    @abstractmethod
    def config(self: Self) -> T:
        """Expose validated config."""

    @property
    @abstractmethod
    def reader_config(self) -> dict:
        """Returns reader configuration."""

    @property
    @abstractmethod
    def writer_config(self) -> dict:
        """Returns writer configuration."""
