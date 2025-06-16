"""
Factory takes multiple deliveries and processes data.
"""
from typedspark import DataSet
from typing_extensions import Self
from etl.factories import CitiesFactory, CitizensFactory
from etl.schemas import Citizen, City, CitizenCountry
from pysetl.workflow import Factory, Delivery


class CitizenCountryFactory(Factory[DataSet[CitizenCountry]]):
    """
    CitizenCountryFactory produces a DataSet[CitizenCountry].

    It expects two ambiguous deliveries. We provide the producer to resolve the
    dependencies.
    """
    citizens: DataSet[Citizen]
    cities: DataSet[City]
    output: DataSet[CitizenCountry]

    citizens_delivery = Delivery[DataSet[Citizen]](producer=CitizensFactory)
    cities_delivery = Delivery[DataSet[City]](producer=CitiesFactory)

    def read(self) -> Self:
        self.citizens = self.citizens_delivery.get()
        self.cities = self.cities_delivery.get()

        return self

    def process(self) -> Self:
        self.output = DataSet[CitizenCountry](self.citizens.join(self.cities, "city"))

        return self

    def write(self) -> Self:
        return self

    def get(self) -> DataSet[CitizenCountry]:
        self.output.show()

        return self.output
