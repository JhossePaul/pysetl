"""PySetl expected exceptions."""


class BasePySetlException(Exception):
    """
    Base exception for all PySetl-specific errors.

    All custom exceptions in PySetl should inherit from this class.
    """


class AlreadyExistsException(BasePySetlException):
    """
    Raised when an object is already registered or exists in the registry.
    """


class PySparkException(BasePySetlException):
    """
    Raised for errors related to PySpark operations or integration.
    """


class InvalidConnectorException(BasePySetlException):
    """
    Raised when a connector does not satisfy the required interface or conditions.
    """


class InvalidConfigException(BasePySetlException):
    """
    Raised when a configuration is invalid or contains errors.
    """


class InvalidDeliveryException(BasePySetlException):
    """
    Raised when a delivery is duplicated or not found in the workflow.
    """


class PipelineException(BasePySetlException):
    """
    Raised when a pipeline operation fails.
    """


class BuilderException(BaseException):
    """
    Raised when a builder operation fails.
    """


__all__ = [
    "AlreadyExistsException",
    "PySparkException",
    "InvalidConnectorException",
    "InvalidConfigException",
    "PipelineException",
]
