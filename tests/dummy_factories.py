"""Dummy factories for testing purposes."""
import time

from pyspark.sql import functions as F

from typedspark import DataSet, create_partially_filled_dataset

from pysetl.storage.repository import SparkRepository
from pysetl.workflow import Factory, Delivery
from pysetl.utils.mixins import HasSparkSession

from tests.dummy_schemas import DSSchema, DS3Schema, DUMMY_DATA


class FactoryToBenchmark:
    """Simple object to be benchmarked."""

    def read(self):
        """Wait."""
        time.sleep(0.2)
        return self

    def process(self):
        """Wait."""
        time.sleep(0.3)
        return self

    def write(self):
        """Wait and return string."""
        time.sleep(0.1)
        return self

    def get(self):
        """Return string."""
        return "benchmarked"


class RepositoryLoad(Factory[DataSet[DSSchema]]):
    """Class for testing purposes."""

    repo_delivery = Delivery[SparkRepository[DSSchema]](delivery_id="id")
    repo: SparkRepository[DSSchema]

    def read(self):
        """Test Pysetl."""
        self.repo = self.repo_delivery.get()

        return self

    def process(self):
        """Test Pysetl."""
        self.repo.load().show()

        return self

    def write(self):
        """Test Pysetl."""
        return self

    def get(self):
        """Test Pysetl."""
        return self.repo.load()


class ProducerFactory(Factory[DataSet[DSSchema]], HasSparkSession):
    """Class for testing purposes."""

    repo_delivery = Delivery[SparkRepository[DSSchema]]()
    repo: SparkRepository[DSSchema]
    output: DataSet[DSSchema]

    def read(self):
        """Test Pysetl."""
        self.repo = self.repo_delivery.get()
        self.output = create_partially_filled_dataset(
            self.spark,
            DSSchema,
            DUMMY_DATA["DSSchema"]
        )

        return self

    def process(self):
        """Test Pysetl."""
        return self

    def write(self):
        """Test Pysetl."""
        self.repo.save(self.output)

        return self

    def get(self):
        """Test Pysetl."""
        return self.output


class ConsumerFactory(Factory[DataSet[DS3Schema]]):
    """Class for testing purposes."""

    input_delivery = Delivery[DataSet[DSSchema]]()
    input: DataSet[DSSchema]

    repo_delivery = Delivery[SparkRepository[DS3Schema]]()
    repo: SparkRepository[DS3Schema]

    output: DataSet[DS3Schema]

    def read(self):
        """Test Pysetl."""
        self.repo = self.repo_delivery.get()
        self.input = self.input_delivery.get()
        return self

    def process(self):
        """Test Pysetl."""
        self.output = DataSet[DS3Schema](
            self.input
            .withColumn("value2", F.lit("haha"))
        )

        return self

    def write(self):
        """Test Pysetl."""
        self.repo.save(self.output)

        return self

    def get(self):
        """Test Pysetl."""
        return self.output
