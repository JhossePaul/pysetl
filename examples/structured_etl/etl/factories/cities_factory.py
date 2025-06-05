"""
Factory shows how to inject ambiguous dependencies.
"""
from typedspark import DataSet
from typing_extensions import Self
from etl.schemas import City

from pysetl.workflow import Factory, Delivery


class CitiesFactory(Factory[DataSet[City]]):
    """
    CitiesFactory returns a Dataset[City] 

    It expects data to be injected automatically by a Delivery. Since there are
    multiple deliveries with of this type we provide the delivery ID.
    """
    cities: DataSet[City]
    cities_delivery = Delivery[DataSet[City]](delivery_id="right")

    def read(self) -> Self:
        self.cities = self.cities_delivery.get()

        return self

    def process(self) -> Self:
        return self

    def write(self) -> Self:
        return self

    def get(self) -> DataSet[City]:
        self.cities.show()

        return self.cities
