from typedspark import DataSet
from typing_extensions import Self
from pysetl.workflow import Factory, Delivery
from etl.schemas import Citizen


class CitizensFactory(Factory[DataSet[Citizen]]):
    citizens_delivery = Delivery[DataSet[Citizen]]()

    def read(self) -> Self:
        return self

    def process(self) -> Self:
        return self

    def write(self) -> Self:
        return self

    def get(self) -> DataSet[Citizen]:
        citizens = self.citizens_delivery.get()
        citizens.show()

        return  citizens
