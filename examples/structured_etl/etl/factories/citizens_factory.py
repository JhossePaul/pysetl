"""
Factory shows how to inject dependencies.
"""
from typedspark import DataSet
from typing_extensions import Self
from etl.schemas import Citizen
from pysetl.workflow import Factory, Delivery


class CitizensFactory(Factory[DataSet[Citizen]]):
    """
    CitiesFactory returns a Dataset[Citizen] 

    It expects data to be injected automatically by a Delivery.
    """
    citizens: DataSet[Citizen]
    citizens_delivery = Delivery[DataSet[Citizen]]()

    def read(self) -> Self:
        self.citizens = self.citizens_delivery.get()
        return self

    def process(self) -> Self:
        return self

    def write(self) -> Self:
        return self

    def get(self) -> DataSet[Citizen]:
        self.citizens.show()

        return self.citizens
