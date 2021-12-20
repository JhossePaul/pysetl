"""
Collection of class mixins to add functionalities to base package classes
"""
from pysetl.mixins.capabilities.can_create import CanCreateMixin
from pysetl.mixins.capabilities.can_delete import CanDeleteMixin
from pysetl.mixins.capabilities.can_drop import CanDropMixin
from pysetl.mixins.capabilities.can_partition import CanPartitionMixin
from pysetl.mixins.capabilities.can_update import CanUpdateMixin
from pysetl.mixins.capabilities.can_vaccum import CanVaccumMixin
from pysetl.mixins.capabilities.can_wait import CanWaitMixin
from pysetl.mixins.features.has_benchmark import HasBenchmarkMixin

__all__ = [
    "CanCreateMixin",
    "CanDeleteMixin",
    "CanDropMixin",
    "CanPartitionMixin",
    "CanUpdateMixin",
    "CanVaccumMixin",
    "CanWaitMixin",
    "HasBenchmarkMixin"
]
