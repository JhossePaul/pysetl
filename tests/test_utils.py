"""Unit tests for pysetl.utils module."""
import inspect
import logging
import time
from typing import Generic, TypeVar
import pytest
from pyspark.sql import SparkSession
from pysetl.utils import BenchmarkModifier, BenchmarkResult, pretty
from pysetl.utils.exceptions import PySparkException
from pysetl.utils.get_signature import get_signature
from pysetl.utils.mixins import (
    HasDiagram, HasLogger, HasRegistry, IsIdentifiable,
    HasSparkSession
)


class DummyFactory:
    """Simple object to be benchmarked."""

    def read(self):
        """Wait."""
        time.sleep(0.2)
        return self

    def process(self):
        """Wait."""
        time.sleep(0.3)
        return self

    def write(self):
        """Wait and return string."""
        time.sleep(0.1)
        return self

    def get(self):
        """Return string."""
        return "benchmarked"


def test_benchmark_modifier_get():
    """Test BenchmarkModifier."""
    benchmarked = BenchmarkModifier[DummyFactory](DummyFactory()).get()
    methods = dict(inspect.getmembers(benchmarked, callable))

    assert benchmarked.read().process().write().get() == "benchmarked"

    times = {
        k: round(v, 1)
        for k, v
        in benchmarked.times.items()  # type: ignore
    }
    results = BenchmarkResult(**times | {
        "cls": "DummyFactory",
        "total": sum(times.values())
    })

    assert "read" in methods
    assert "process" in methods
    assert "write" in methods
    assert "get" in methods
    assert isinstance(times, dict)
    assert str(results) == "\n".join([
        "Benchmark class: DummyFactory",
        "Total elapsed time: 0.6 s",
        "read: 0.2 s",
        "process: 0.3 s",
        "write: 0.1 s",
        "================="
    ])


def test_get_signature():
    """Test get_signature utility."""
    def _sum(x, y):
        """For testing purposes."""
        return x + y

    assert get_signature(_sum) == ["x", "y"]


def test_pretty():
    """Test pretty utility."""
    T = TypeVar("T")

    class GenericTest(Generic[T]):
        """Simple object to be formatted."""

    assert pretty(type("")) == "str"
    assert pretty(None) == ""
    assert pretty(type(GenericTest[int])) == "_GenericAlias"
    assert pretty(GenericTest[int]) == "GenericTest[int]"
    assert pretty(list[int]) == "list[int]"
    assert pretty("module.type") == "type"

    with pytest.raises(NotImplementedError) as error:
        pretty(1)

    assert error


def test_has_diagram():
    """Test HasDiagram mixin."""
    class WithDiagram(HasDiagram):
        """Class for test purposes."""
        def to_diagram(self) -> str:
            return "A mermaid diagram"

        @property
        def diagram_id(self) -> str:
            return "id"

    format_diagram_str = (
        WithDiagram()
        .format_diagram_id(
            name="hola",
            diagram_id="adios",
            suffix="12g"
        )
    )

    assert WithDiagram().get_signature() == ["args", "kwargs"]
    assert format_diagram_str == "holaAdios12g"


def test_has_logger():
    """Test logging functionality."""
    assert HasLogger().log_info("hola") == logging.info("hola")
    assert HasLogger().log_debug("hola") == logging.debug("hola")
    assert HasLogger().log_debug("hola") == logging.debug("hola")
    assert HasLogger().log_warning("hola") == logging.warning("hola")
    assert HasLogger().log_error("hola") == logging.error("hola")


def test_has_registry():
    """Test Registry."""
    class Id(IsIdentifiable):
        """Identifiable object."""

    obj1 = Id()
    obj2 = Id()
    obj3 = Id()
    registry = (
        HasRegistry[Id]()
        .register_items([obj2, obj3])
        .register(obj1)
    )

    assert registry.last_registered_item == obj1
    assert registry.get_item(obj2.uuid) == obj2
    assert len(registry.get_registry()) == 3
    assert registry.clear_registry().size == 0
    assert not registry.last_registered_item


def test_has_spark_session_exceptions():
    """Throw PySparkException if no spark session found."""
    SparkSession.builder.getOrCreate().stop()

    with pytest.raises(PySparkException) as error:
        _ = HasSparkSession().spark

    assert str(error.value) == "No active Spark session"
