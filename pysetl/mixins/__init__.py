"""
Collection of class mixins to add functionalities to base package classes
"""
from pysetl.mixins.can_create import CanCreateMixin
from pysetl.mixins.can_delete import CanDeleteMixin
from pysetl.mixins.can_drop import CanDropMixin

__all__ = ["CanCreateMixin", "CanDeleteMixin", "CanDropMixin"]
