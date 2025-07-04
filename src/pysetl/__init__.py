"""
This module provides the main entry point for building and managing ETL
pipelines using PySpark.

It exposes the PySetl class, which acts as a workflow orchestrator, and the
PySetlBuilder for convenient construction.
"""
from __future__ import annotations
from uuid import UUID
from typing import Optional, TypeVar
from typing_extensions import Self
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pysetl.config.config import Config
from pysetl.utils.mixins import HasRegistry
from pysetl.utils import Builder
from pysetl.storage import SparkRepositoryBuilder
from pysetl.storage.repository import SparkRepository
from pysetl.utils.exceptions import InvalidConfigException, BuilderException
from pysetl.workflow import Deliverable, Pipeline, Factory


__version__ = "0.1.3b1"


C = TypeVar("C", bound=Config)
ConfigDict = dict[str, C]


class PySetl(HasRegistry[Pipeline]):
    """
    Main orchestrator for PySetl ETL pipelines.

    This class manages SparkSession, configuration, and external deliverables, and provides
    methods to register and retrieve Spark repositories and pipelines.
    """

    def __init__(
        self: Self,
        spark: SparkSession,
        config_dict: ConfigDict,
        benchmark: bool = False,
    ) -> None:
        """
        Initialize a PySetl orchestrator.

        Args:
            spark: The active SparkSession.
            config_dict: Dictionary mapping config keys to Config objects.
            benchmark: Whether to enable benchmarking for pipelines.
        """
        super().__init__()
        self.spark: SparkSession = spark
        self.external_inputs: dict[str, Deliverable] = {}
        self.config_dict = config_dict
        self.benchmark = benchmark

    def has_external_input(self: Self, external_id: str) -> bool:
        """
        Check if a deliverable is set in the pipeline dispatcher.

        Args:
            external_id: The identifier for the external deliverable.

        Returns:
            True if the deliverable is registered, False otherwise.
        """
        return external_id in self.external_inputs

    def list_inputs(self: Self) -> dict[str, Deliverable]:
        """
        List all deliverables registered in the pipeline dispatcher.

        Returns:
            Dictionary mapping external IDs to Deliverable objects.
        """
        return self.external_inputs

    def get_sparkrepository(
        self: Self, repository_type: type, config_key: str
    ) -> Optional[SparkRepository]:
        """
        Get a SparkRepository registered in the pipeline dispatcher.

        Args:
            repository_type: The class/type of the repository.
            config_key: The key for the configuration.

        Returns:
            The SparkRepository instance if registered, else None.
        """
        self.set_spark_repository_from_config(repository_type, config_key)

        repository_delivery = self.external_inputs.get(config_key)

        return repository_delivery.payload if repository_delivery else None

    def reset_spark_repository_from_config(
        self: Self,
        repository_type: type,
        config_key: str,
        consumers: Optional[set[type[Factory]]] = None,
        delivery_id: str = "",
        cache: bool = False,
    ) -> Self:
        """
        Create a SparkRepository and register it in the dispatcher.

        Args:
            repository_type: The class/type of the repository.
            config_key: The key for the configuration.
            consumers: Optional set of Factory types that consume this repository.
            delivery_id: Optional delivery identifier.
            cache: Whether to enable caching for the repository.

        Returns:
            Self (for chaining).
        """
        _consumers = consumers if consumers is not None else set()
        config = self.config_dict.get(config_key)

        if not config:
            raise InvalidConfigException(f"No config found with key: {config_key}")

        repository = SparkRepositoryBuilder[repository_type](  # type: ignore
            config, cache=cache
        ).getOrCreate()

        repository_deliverable = (
            Deliverable[SparkRepository[repository_type]](  # type: ignore
                repository
            )
            .set_consumers(_consumers)
            .set_delivery_id(delivery_id)
        )

        self.external_inputs[config_key] = repository_deliverable

        return self

    def set_spark_repository_from_config(
        self: Self,
        repository_type: type,
        config_key: str,
        consumers: Optional[set[type[Factory]]] = None,
        delivery_id: str = "",
        cache: bool = False,
    ) -> Self:
        """
        Register a SparkRepository if not already registered, otherwise reset it.

        Args:
            repository_type: The class/type of the repository.
            config_key: The key for the configuration.
            consumers: Optional set of Factory types that consume this repository.
            delivery_id: Optional delivery identifier.
            cache: Whether to enable caching for the repository.

        Returns:
            Self (for chaining).
        """
        if not self.has_external_input(config_key):
            self.reset_spark_repository_from_config(
                repository_type, config_key, consumers, delivery_id, cache
            )

        return self

    def reset_spark_repository(
        self: Self,
        repository: SparkRepository,
        repository_id: str,
        consumers: Optional[set[type[Factory]]] = None,
        delivery_id: str = "",
    ) -> Self:
        """
        Register a SparkRepository in the pipeline dispatcher.

        Args:
            repository: The SparkRepository instance.
            repository_id: The identifier for the repository.
            consumers: Optional set of Factory types that consume this repository.
            delivery_id: Optional delivery identifier.

        Returns:
            Self (for chaining).
        """
        _consumers = consumers if consumers else set()
        __type = repository.type_annotation
        repository_deliverable = Deliverable[__type](  # type: ignore
            repository, consumers=_consumers, delivery_id=delivery_id
        )
        self.external_inputs[repository_id] = repository_deliverable

        return self

    def set_spark_repository(
        self: Self,
        repository: SparkRepository,
        repository_id: str,
        consumers: Optional[set[type[Factory]]] = None,
        delivery_id: str = "",
    ) -> Self:
        """
        Register a SparkRepository if not already registered, otherwise reset it.

        Args:
            repository: The SparkRepository instance.
            repository_id: The identifier for the repository.
            consumers: Optional set of Factory types that consume this repository.
            delivery_id: Optional delivery identifier.

        Returns:
            Self (for chaining).
        """
        if not self.has_external_input(repository_id):
            self.reset_spark_repository(
                repository, repository_id, consumers, delivery_id
            )

        return self

    def new_pipeline(self: Self) -> Pipeline:
        """
        Create a new Pipeline and register it in the PySetl object.

        Returns:
            The created Pipeline instance.
        """
        pipeline = Pipeline(benchmark=self.benchmark)

        self.register(pipeline)

        list(
            map(
                lambda t: pipeline.set_input_from_deliverable(t[1]),
                self.external_inputs.items(),
            )
        )

        return pipeline

    def get_pipeline(self: Self, uuid: UUID) -> Optional[Pipeline]:
        """
        Get a Pipeline from the registry by its UUID.

        Args:
            uuid: The UUID of the pipeline.

        Returns:
            The Pipeline instance if found, else None.
        """
        return self.get_registry().get(uuid)

    def stop(self: Self) -> None:
        """
        Stop the SparkSession managed by this PySetl instance.
        """
        self.spark.stop()

    @classmethod
    def builder(cls) -> "PySetlBuilder":
        """
        Return a builder for PySetl.

        Returns:
            PySetlBuilder: A builder instance for PySetl.
        """
        return PySetlBuilder()


class PySetlBuilder(Builder[PySetl]):
    """
    Builder for PySetl objects.

    Allows stepwise configuration and construction of a PySetl instance.
    """

    def __init__(
        self: Self,
        benchmark: bool = False,
        config: Optional[ConfigDict] = None,
        shuffle_partitions: Optional[int] = None,
        spark_master: str = "local",
        spark_session: Optional[SparkSession] = None,
        spark_config: Optional[SparkConf] = None,
    ) -> None:
        """
        Initialize a PySetlBuilder.

        Args:
            benchmark: Whether to enable benchmarking for pipelines.
            config: Optional configuration dictionary.
            shuffle_partitions: Optional number of shuffle partitions for Spark.
            spark_master: Spark master URL (default: 'local').
            spark_session: Optional pre-existing SparkSession.
            spark_config: Optional SparkConf object.
        """
        self.benchmark = benchmark
        self.config = config
        self.spark_config = spark_config
        self.shuffle_partitions = shuffle_partitions
        self.spark_master = spark_master
        self.spark_session = spark_session
        self.__pysetl: Optional[PySetl] = None

    def set_config(self: Self, config: ConfigDict) -> Self:
        """
        Set the configuration dictionary for PySetl.

        Args:
            config: Dictionary mapping config keys to Config objects.

        Returns:
            Self (for chaining).
        """
        self.config = config

        return self

    def set_spark_config(self: Self, spark_config: SparkConf) -> Self:
        """
        Set the SparkConf configuration for the SparkSession.

        Args:
            spark_config: SparkConf object.

        Returns:
            Self (for chaining).
        """
        self.spark_config = spark_config

        return self

    def set_shuffle_partitions(self: Self, shuffle: int) -> Self:
        """
        Set the number of shuffle partitions for the SparkSession.

        Args:
            shuffle: Number of shuffle partitions.

        Returns:
            Self (for chaining).
        """
        self.shuffle_partitions = shuffle

        return self

    def set_spark_master(self: Self, master: str) -> Self:
        """
        Set the Spark master URL.

        Args:
            master: Spark master URL (e.g., 'local', 'yarn').

        Returns:
            Self (for chaining).
        """
        self.spark_master = master

        return self

    def set_spark_session(self: Self, session: SparkSession) -> Self:
        """
        Set the SparkSession directly.

        Args:
            session: An existing SparkSession instance.

        Returns:
            Self (for chaining).
        """
        self.spark_session = session

        return self

    def build_spark_session(self: Self) -> SparkSession:
        """
        Build and configure a SparkSession.

        Returns:
            The constructed SparkSession instance.
        """
        shuffle_partitions = (
            [(str("spark.sql.shuffle.partitions"), str(self.shuffle_partitions))]
            if self.shuffle_partitions
            else []
        )
        config_from_dict: Optional[Config] = (
            self.config.get("spark") if self.config else None
        )
        spark_app_name = (
            config_from_dict.params.get("app_name", "default_app_name")
            if config_from_dict
            else "default"
        )
        spark_config_dict = (
            list(config_from_dict.params.items()) if config_from_dict else []
        )
        spark_config_from_builder = (
            self.spark_config.getAll() if self.spark_config else []
        )
        final_config: SparkConf = (
            SparkConf()
            .setAll(spark_config_from_builder)
            .setAll(spark_config_dict)
            .setAll(shuffle_partitions)
        )

        session_builder: SparkSession.Builder = (
            SparkSession.Builder()
            .master(self.spark_master)
            .appName(spark_app_name)
            .config(conf=final_config)
        )

        return session_builder.getOrCreate()

    def build(self: Self) -> Self:
        """Build PySetl."""
        spark = self.spark_session if self.spark_session else self.build_spark_session()

        config = self.config if self.config else {}

        self.__pysetl = PySetl(spark, config, self.benchmark)

        return self

    def get(self: Self) -> PySetl:
        """Return PySetl."""
        if not self.__pysetl:
            raise BuilderException("No PySetl object built")

        return self.__pysetl
