"""
An example of factories.

This modules shows how to process data with factories and how to
take advantage of dependency injection.
"""
from .citizens_factory import CitizensFactory
from .cities_factory import CitiesFactory
from .citizens_country_factory import CitizenCountryFactory


__all__ = [
    "CitizensFactory",
    "CitiesFactory",
    "CitizenCountryFactory"
]
