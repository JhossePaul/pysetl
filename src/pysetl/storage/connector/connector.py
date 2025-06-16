"""
Defines the abstract Connector class, integrating Spark and logging, for the
pysetl.connector package.
"""
from abc import ABCMeta
from pysetl.utils.mixins import HasSparkSession, HasLogger
from pysetl.config.config import BaseConfigModel
from .base_connector import BaseConnector


class Connector(BaseConnector, HasSparkSession, HasLogger, metaclass=ABCMeta):
    """
    Abstract Connector.

    A connector should implement read/write operations with a SparkSession
    object. These implementations should be architecture specific
    """

    config: BaseConfigModel
