from typedspark import DataSet
from typing_extensions import Self
from pysetl.workflow import Factory, Delivery
from etl.schemas import Citizen, City, CitizenCountry
from .cities_factory import CitiesFactory


class CitizenCountryFactory(Factory[DataSet[CitizenCountry]]):
    citizens_delivery = Delivery[DataSet[Citizen]]()
    citizens: DataSet[Citizen]
    cities_delivery = Delivery[DataSet[City]]()
    cities: DataSet[City]
    output: DataSet[CitizenCountry]

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
