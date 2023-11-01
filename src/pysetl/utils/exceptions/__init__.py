"""PySetl expected exceptions."""


class BasePySetlException(Exception):
    """Throw a PySetl Exception."""


class AlreadyExistsException(BasePySetlException):
    """Throw an exception for an already registered object."""


class PySparkException(BasePySetlException):
    """Throw an exception if issue is related with PySpark."""


class InvalidConnectorException(BasePySetlException):
    """Throw an exception if connector does not satisfies requirements."""


class InvalidConfigException(BasePySetlException):
    """Throw an exception if configuration has an error."""


class InvalidDeliveryException(BasePySetlException):
    """Throw an exception if delivery is duplicated or not found."""


class PipelineException(BasePySetlException):
    """Throw an exception if Pipeline fails."""


class BuilderException(BaseException):
    """Throw if builder fails."""


__all__ = [
    "AlreadyExistsException",
    "PySparkException",
    "InvalidConnectorException",
    "InvalidConfigException",
    "PipelineException"
]
