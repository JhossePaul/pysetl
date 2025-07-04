"""
External module for PySetl.

Defines the External class, which acts as a marker for external input sources in
the pipeline.  External is used as the default producer for Deliveries that are
not produced by a Factory.  It enables the pipeline to represent and resolve
dependencies on data that comes from outside the pipeline, such as user-provided
data, files, or other external systems.

External nodes are injected into the DAG as special nodes, allowing the workflow
to treat external inputs in a uniform way alongside factory-produced data.
"""
from abc import ABCMeta
from typing import final
from pysetl.utils.mixins import IsIdentifiable


@final
class External(IsIdentifiable, metaclass=ABCMeta):
    """
    Marker class for external input sources in the pipeline.

    External is used as the default producer for Deliveries that are not
    produced by a Factory.  It allows the pipeline to represent dependencies on
    data that originates outside the workflow, such as user input, files, or
    external systems. External nodes are injected into the DAG as special nodes,
    enabling uniform dependency resolution and visualization.

    Methods:
        delivery_type: Returns the type of this class (used for dependency resolution).
    """

    @classmethod
    def delivery_type(cls) -> type:
        """
        Return the delivery type of this class (for dependency resolution).

        Returns:
            type: The External class itself.
        """
        return External
