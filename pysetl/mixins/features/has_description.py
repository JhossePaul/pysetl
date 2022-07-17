"""
HasDescriptionMixin module
"""
from utils.printers import pretty


class HasDescriptionMixin:
    """Returns pretty class name"""
    def pretty(self): pretty(self.describe())

    """Returns instance class name"""
    def describe(self):
        return self.__class__.__name__
