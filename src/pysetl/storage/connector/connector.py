"""
Abstract Connector class for PySetl.

Defines the abstract Connector class, integrating SparkSession and logging capabilities.
All connectors should implement read/write operations with a SparkSession object.
"""
from abc import ABCMeta
from pysetl.utils.mixins import HasSparkSession, HasLogger
from pysetl.config.config import BaseConfigModel
from .base_connector import BaseConnector


class Connector(BaseConnector, HasSparkSession, HasLogger, metaclass=ABCMeta):
    """
    Abstract base class for Spark-based data connectors with logging.

    Inherits from BaseConnector, HasSparkSession, and HasLogger.
    All connectors should implement read/write operations with a SparkSession object.
    This class cannot be instantiated directly.

    Attributes:
        config (BaseConfigModel): The configuration model for the connector.
    """

    config: BaseConfigModel
