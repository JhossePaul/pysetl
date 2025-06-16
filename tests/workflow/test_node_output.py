"""Test pysetl.workflow.node_output module."""
from dataclasses import dataclass, is_dataclass
from typing import TypeVar, Generic
from pyspark.sql.types import StringType, IntegerType
from pydantic import BaseModel
from typedspark import Schema, DataSet, Column
import pytest

from pysetl.workflow.node_output import NodeOutput
from pysetl.utils.exceptions import BasePySetlException


T = TypeVar("T")


@dataclass
class Container(Generic[T]):
    """Class for testing purposes."""

    content: T


class CitizenSchema(Schema):
    """Class for testing purposes."""

    name: Column[StringType]
    age: Column[IntegerType]


@dataclass
class CitizenDataclass:
    """Class for testing purposes."""

    name: str
    age: int


class CitizenPydantic(BaseModel):
    """Class for testing purposes."""

    name: str
    age: int


def test_node_output():
    """Test node output."""
    output_base = NodeOutput(
        delivery_type=int,
        consumer=set(),
        delivery_id="base",
        external=True
    )
    output_dataclass = NodeOutput(
        delivery_type=CitizenDataclass,
        consumer=set(),
        delivery_id="schema",
        final_output=True
    )
    output_dataset = NodeOutput(
        delivery_type=DataSet[CitizenSchema],
        consumer=set(),
        delivery_id="dataset"
    )
    output_pydantic = NodeOutput(
        delivery_type=CitizenPydantic,
        consumer=set(),
        delivery_id="pydantic"
    )
    output_generic = NodeOutput(
        delivery_type=Container[str],
        consumer=set(),
        delivery_id="generic"
    )
    output_generic.to_diagram()

    assert output_base.delivery_type is int
    assert repr(output_base.wrapped_delivery_type) == "<class 'int'>"
    assert output_base.delivery_id == "base"
    assert not output_base.final_output
    assert output_base.external
    assert output_base.diagram_id == "intBaseExternal"
    assert not output_base.get_fields()
    assert (
        output_base.to_diagram() ==
        "class intBaseExternal {\n    <<int>>\n\n}"
    )
    assert output_dataclass.final_output
    assert is_dataclass(output_dataclass.delivery_type)
    assert output_dataclass.get_fields() == ["    >name: str", "    >age: int"]
    assert issubclass(output_pydantic.delivery_type, BaseModel)
    assert output_pydantic.get_fields() == ["    >name: str", "    >age: int"]
    assert output_dataset.wrapped_delivery_type == DataSet[CitizenSchema]
    assert output_dataset.get_fields() == [
        "    >name: string",
        "    >age: int"
    ]
    assert ">content: str" in output_generic.to_diagram()

def test_node_output_exceptions():
    """Test NodeOutput exceptions."""
    output_generic = NodeOutput(
        delivery_type=DataSet,
        consumer=set(),
        delivery_id="error"
    )

    with pytest.raises(BasePySetlException) as error:
        output_generic.get_fields()

    assert "DataSet has no schema." == str(error.value)
