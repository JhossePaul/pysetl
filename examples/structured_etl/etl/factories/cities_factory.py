from typedspark import DataSet
from typing_extensions import Self
from pysetl.workflow import Factory, Delivery
from etl.schemas import City


class CitiesFactory(Factory[DataSet[City]]):
    cities_delivery = Delivery[DataSet[City]](delivery_id="right")

    def read(self) -> Self:
        return self

    def process(self) -> Self:
        return self

    def write(self) -> Self:
        return self

    def get(self) -> DataSet[City]:
        cities = self.cities_delivery.get()
        cities.show()

        return cities
