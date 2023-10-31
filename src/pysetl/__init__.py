"""PySetl: Python Spark ETL Framework."""
from __future__ import annotations
from abc import ABCMeta
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
from pysetl.utils.exceptions import InvalidConfigException
from pysetl.workflow import Deliverable, Pipeline, Factory


__version__ = "0.1.3-b"


C = TypeVar("C", bound=Config)
ConfigDict = dict[str, C]


class PySetl(HasRegistry[Pipeline], metaclass=ABCMeta):
    """PySetl is a Spark ETL framework for Python."""

    def __init__(
            self: Self,
            spark: SparkSession,
            config_dict: dict[str, C],
            benchmark: bool = False) -> None:
        """Pysetl Constructor."""
        super().__init__()
        self.spark: SparkSession = spark
        self.external_inputs: dict[str, Deliverable] = {}
        self.config_dict = config_dict
        self.benchmark = benchmark

    def has_external_input(self: Self, external_id: str) -> bool:
        """Check if a deliverable is set in Pipeline dispatcher."""
        return external_id in self.external_inputs

    def list_inputs(self: Self) -> dict[str, Deliverable]:
        """List Deliverable registered in Pipeline Dispatcher."""
        return self.external_inputs

    def get_sparkrepository(
            self: Self,
            repository_type: type,
            config_key: str) -> Optional[SparkRepository]:
        """Get SparkRepository registered in Pipeline Dispatcher."""
        self.set_spark_repository_from_config(repository_type, config_key)

        repository_delivery = self.external_inputs.get(config_key)

        return repository_delivery.payload if repository_delivery else None

    def reset_spark_repository_from_config(
            self: Self,
            repository_type: type,
            config_key: str,
            consumers: Optional[set[type[Factory]]] = None,
            delivery_id: str = "",
            cache: bool = False) -> Self:
        """Create a SparkRepository and register it in Dispatcher."""
        _consumers = consumers if consumers is not None else set()
        config = self.config_dict.get(config_key)

        if not config:
            raise InvalidConfigException(
                f"No config found with key: {config_key}"
            )

        repository = SparkRepositoryBuilder[repository_type](
            config,
            cache=cache
        ).getOrCreate()
        repository_deliverable = (
            Deliverable[SparkRepository[repository_type]](repository)
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
            cache: bool = False) -> Self:
        """Check if SparkRepository already registered otherwise reset it."""
        if not self.has_external_input(config_key):
            self.reset_spark_repository_from_config(
                repository_type,
                config_key,
                consumers,
                delivery_id,
                cache
            )

        return self

    def reset_spark_repository(
            self: Self,
            repository: SparkRepository,
            repository_id: str,
            consumers: Optional[set[type[Factory]]] = None,
            delivery_id: str = "") -> Self:
        """Register SparkRepository in Pipeline Dispatcher."""
        _consumers = consumers if consumers else set()
        repository_deliverable = Deliverable[repository.type_annotation](
            repository,
            consumers=_consumers,
            delivery_id=delivery_id
        )
        self.external_inputs[repository_id] = repository_deliverable

        return self

    def set_spark_repository(
            self: Self,
            repository: SparkRepository,
            repository_id: str,
            consumers: Optional[set[type[Factory]]] = None,
            delivery_id: str = "") -> Self:
        """Check if SparkRepositoty registered otherwise, register it."""
        if not self.has_external_input(repository_id):
            self.reset_spark_repository(
                repository,
                repository_id,
                consumers,
                delivery_id
            )

        return self

    def new_pipeline(self: Self) -> Pipeline:
        """Create Pipeline and register it in PySel object."""
        pipeline = Pipeline(benchmark=self.benchmark)

        self.register(pipeline)

        list(map(
            lambda t: pipeline.set_input_from_deliverable(t[1]),
            self.external_inputs.items()
        ))

        return pipeline

    def get_pipeline(self: Self, uuid: UUID) -> Optional[Pipeline]:
        """Get Pipeline from registry."""
        return self.get_registry().get(uuid)

    def stop(self: Self) -> None:
        """Stop SparkSession."""
        self.spark.stop()

    @classmethod
    def builder(cls) -> PySetlBuilder:
        """Return a builder for PySetl."""
        return PySetlBuilder()


class PySetlBuilder(Builder[PySetl]):
    """Build a PySetl object."""

    def __init__(
            self: Self,
            benchmark: bool = False,
            config: Optional[ConfigDict] = None,
            shuffle_partitions: Optional[int] = None,
            spark_master: str = "local",
            spark_session: Optional[SparkSession] = None,
            spark_config: Optional[SparkConf] = None) -> None:
        self.benchmark = benchmark
        self.config = config
        self.spark_config = spark_config
        self.shuffle_partitions = shuffle_partitions
        self.spark_master = spark_master
        self.spark_session = spark_session
        self.__pysetl = None

    def set_config(self: Self, config: ConfigDict) -> Self:
        """Set PySetl SparkRepository configurations."""
        self.config = config

        return self

    def set_spark_config(self: Self, spark_config: SparkConf) -> Self:
        """Set SparkSession configuration."""
        self.spark_config = spark_config

        return self

    def set_shuffle_partitions(self: Self, shuffle: int) -> Self:
        """Set SparkSession number of shuffle partitions."""
        self.shuffle_partitions = shuffle

        return self

    def set_spark_master(self: Self, master: str) -> Self:
        """Set SparkSession master."""
        self.spark_master = master

        return self

    def set_spark_session(self: Self, session: SparkSession):
        """Set SparkSession directly."""
        self.spark_session = session

        return self

    def build_spark_session(self: Self) -> SparkSession:
        """Build SparkSession."""
        shuffle_partitions = (
            [(
                str("spark.sql.shuffle.partitions"),
                str(self.shuffle_partitions)
            )]
            if self.shuffle_partitions
            else []
        )
        config_from_dict: Optional[Config] = (
            self.config.get("spark")
            if self.config
            else None
        )
        spark_app_name = config_from_dict._config.get(
            "app_name",
            "default_app_name"
        ) if config_from_dict else "default"
        spark_config_dict = list(
            config_from_dict._config.items()
        ) if config_from_dict else []
        spark_config_from_builder = (
            self.spark_config.getAll()
            if self.spark_config
            else []
        )
        final_config: SparkConf = (
            SparkConf()
            .setAll(spark_config_from_builder)
            .setAll(spark_config_dict)
            .setAll(shuffle_partitions)
        )

        session_builder: SparkSession.Builder = (
            SparkSession.builder
            .master(self.spark_master)
            .appName(spark_app_name)
            .config(conf=final_config)
        )

        return session_builder.getOrCreate()

    def build(self: Self) -> Self:
        """Build PySetl."""
        spark = self.spark_session \
            if self.spark_session \
            else self.build_spark_session()

        config = self.config if self.config else {}

        self.__pysetl = PySetl(spark, config, self.benchmark)

        return self

    def get(self: Self) -> Optional[PySetl]:
        """Return PySetl."""
        return self.__pysetl
