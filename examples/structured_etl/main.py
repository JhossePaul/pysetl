from pyspark.sql import SparkSession
from typedspark import DataSet, create_partially_filled_dataset
from pysetl import PySetl
from pysetl.workflow import Deliverable, Stage
from etl.schemas import Citizen, City
from etl.settings import SETTINGS
from etl.factories import CitizensFactory, CitiesFactory, CitizenCountryFactory


spark = SparkSession.builder.getOrCreate()


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

fake_deliverable = Deliverable[DataSet[City]](fake_cities, delivery_id="fake")
cities_deliverable = Deliverable[DataSet[City]](cities, delivery_id="right")
citizens_deliverable = Deliverable[DataSet[Citizen]](citizens)

stage = (
    Stage()
    .add_factory_from_type(CitizensFactory)
    .add_factory_from_type(CitiesFactory)
)

pipeline = (
    PySetl.builder()
    .set_config(SETTINGS)
    .getOrCreate()
    .new_pipeline()
    .set_input_from_deliverable(fake_deliverable)
    .set_input_from_deliverable(cities_deliverable)
    .set_input_from_deliverable(citizens_deliverable)
    .add_stage(stage)
    .add_stage_from_type(CitizenCountryFactory)
)

if __name__ == "__main__":
    print(pipeline.run().inspector)
