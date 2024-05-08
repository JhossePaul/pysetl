PySetl Context: Putting all together
============================================

:class:`pysetl.PySetl` is a context to execute a Pipeline. It consists of a set
of Config, a set of SparkRepository and a SparkSession. The PySetl context links
a configuration to its SparkRepository. It comes with a builder that will create
a SparkSession if needed but you can pass a custom SparkSession. Finally,
it can create a Pipeline and register the linked SparkRepository as deliverables.

Here is an example including all the lingo learned in the previous sections. But
of course, you would prefer to see it in action in a full `structured example`_:

.. code-block:: python

    from typing_extensions import Self
    from typedspark import DataSet, Column, Schema, create_partially_filled_dataset
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StringType, IntegerType
    from pysetl import PySetl
    from pysetl.enums import SaveMode
    from pysetl.storage.repository import SparkRepository
    from pysetl.config import CsvConfig, ParquetConfig
    from pysetl.workflow import Factory, Delivery, Stage, Deliverable


    spark = SparkSession.builder.getOrCreate()


    class Citizen(Schema):
        name: Column[StringType]
        age: Column[IntegerType]
        city: Column[StringType]


    class City(Schema):
        city: Column[StringType]
        country: Column[StringType]


    class CitizenCountry(Citizen):
        country: Column[StringType]


    citizens = create_partially_filled_dataset(
        spark,
        Citizen,
        [
            {Citizen.name: "Gerardo", Citizen.age: 28, Citizen.city: "Los Angeles"},
            {Citizen.name: "Paul", Citizen.age: 31, Citizen.city: "Puebla"},
        ]
    )

    cities = create_partially_filled_dataset(
        spark,
        City,
        [
            {City.city: "Puebla", City.country: "Mexico"},
            {City.city: "CDMX", City.country: "Mexico"},
            {City.city: "Los Angeles", City.country: "USA"}
        ]
    )

    fake_cities = create_partially_filled_dataset(
        spark,
        City,
        [
            {City.city: "Seoul", City.country: "Korea"},
            {City.city: "Berlin", City.country: "Germany"},
            {City.city: "Los Angeles", City.country: "USA"}
        ]
    )


    class CitizensFactory(Factory[DataSet[Citizen]]):
        citizens: DataSet[Citizen]
        repo_delivery = Delivery[SparkRepository[Citizen]]()

        def read(self) -> Self:
            repository = self.repo_delivery.get()
            self.citizens = repository.load()
            return self

        def process(self) -> Self:
            return self

        def write(self) -> Self:
            return self

        def get(self) -> DataSet[Citizen]:
            return self.citizens


    class CitiesFactory(Factory[DataSet[City]]):
        repo_delivery = Delivery[SparkRepository[City]]()
        cities: DataSet[City]

        def read(self) -> Self:
            repository = self.repo_delivery.get()
            self.cities = repository.load()

            return self

        def process(self) -> Self:
            return self

        def write(self) -> Self:
            return self

        def get(self) -> DataSet[City]:
            return self.cities


    class CitizenCountryFactory(Factory[DataSet[CitizenCountry]]):
        output: DataSet[CitizenCountry]
        citizens_delivery = Delivery[DataSet[Citizen]]()
        cities_delivery = Delivery[DataSet[City]](producer=CitiesFactory)

        def read(self) -> Self:
            self.citizens = self.citizens_delivery.get()
            self.states = self.cities_delivery.get()

            return self

        def process(self) -> Self:
            self.output = DataSet[CitizenCountry](self.citizens.join(self.states, "city"))

            return self

        def write(self) -> Self:
            return self

        def get(self) -> DataSet[CitizenCountry]:
            self.output.show()

            return self.output


    config = {
        "citizens": CsvConfig(
            path="/content/citizens.csv",
            inferSchema="true",
            savemode=SaveMode.OVERWRITE
        ),
        "cities": ParquetConfig(
            path="/content/city.parquet",
            savemode=SaveMode.ERRORIFEXISTS
        )
    }

    pysetl = (
        PySetl.builder()
        .set_config(config)
        .getOrCreate()
        .set_spark_repository_from_config(Citizen, "citizens")
        .set_spark_repository_from_config(City, "cities")
    )

    repo_cities = pysetl.get_sparkrepository(City, "cities")
    repo_citizens = pysetl.get_sparkrepository(Citizen, "citizens")

    deliverable = Deliverable[DataSet[City]](fake_cities)

    stage = (
        Stage()
        .add_factory_from_type(CitizensFactory)
        .add_factory_from_type(CitiesFactory)
    )

    pipeline = (
        pysetl
        .new_pipeline()
        .set_input_from_deliverable(deliverable)
        .add_stage(stage)
        .add_stage_from_type(CitizenCountryFactory)
        .run()
    )

    pipeline.inspector

    print(pipeline.to_diagram())

.. _structured example: https://github.com/JhossePaul/pysetl/tree/main/examples
