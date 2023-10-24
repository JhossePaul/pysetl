class BasePySetlException(Exception):
    pass


class AlreadyExistsException(BasePySetlException):
    pass


class PySparkException(BasePySetlException):
    pass


class BadConfiguration(BasePySetlException):
    pass


class S3ConnectionException(BasePySetlException):
    pass


class InvalidConnectorException(BasePySetlException):
    pass


class InvalidConfigException(BasePySetlException):
    pass


class InvalidDeliveryException(BasePySetlException):
    pass


class NoDeliverableException(BasePySetlException):
    pass


class PipelineException(BasePySetlException):
    pass


__all__ = [
    "AlreadyExistsException",
    "PySparkException",
    "BadConfiguration",
    "S3ConnectionException",
    "InvalidConnectorException",
    "InvalidConfigException",
    "NoDeliverableException",
    "PipelineException"
]
