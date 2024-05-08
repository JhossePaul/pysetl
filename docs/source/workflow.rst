Workflow
=============================================

PySetl offers a set of tools to structure your project, manage data pipelines
and solve dependencies dynamically.

Pipeline Management
---------------------------------------------

Factory
+++++++++++++++++++++++++++++++++++++++++++++

At the core of a PySetl project is a Factory. It gives structure and readability
to our code and isolates business logic into single-responsibility concrete
implementations.

:class:`pysetl.workflow.Factory` is a generic abstract class that resembles the
ETL process. It should define a data transformation process to produce an object
of type T. 

.. code-block:: python

    class Factory(Generic[T]):
        @abstractmethod
        def read(self) -> Self: pass

        @abstractmethod
        def processs(self) -> Self: pass

        @abstractmethod
        def write(self) -> Self: pass

        @abstractmethod
        def get(self) -> T: pass

Implementation of concrete factories is left to the development team. Therefore,
a Factory should extended and next, you can invoke the newly defined methods:

.. code-block:: python

    from pysetl.workflow import Factory
    from typedspark import DataSet, Schema, Column, create_partially_filled_dataset
    from pyspark.sql.types import IntegerType, StringType

    class Citizen(Schema):
        name: Column[StringType]
        age: Column[IntegerType]
        city: Column[StringType]

    class CitizensFactory(Factory[DataSet[Citizen]]):
        def read(self) -> Self:
            self.citizens = create_partially_filled_dataset(
                spark,
                Citizen,
                [
                    {Citizen.name: "citizen1", Citizen.age: 28, Citizen.city: "Los Angeles"},
                    {Citizen.name: "citizen2", Citizen.age: 31, Citizen.city: "Mexico City"}
                ]
            )

            return self

        def process(self) -> Self:
            return self

        def write(self) -> Self:
            return self

        def get(self) -> DataSet[Citizen]:
            return self.citizens

    citizens: DataSet[Citizen] = ConcreteFactory().read().process().write().get()

.. note::
    Although we strongly suggest taking advantage of the whole framework, Factory
    by itself is a good enough practice in every project that even if you don't
    want to base your project on PySetl, please split your business logic into
    factories following the Single Responsibility Principle for the sake of
    every data engineer peace of mind in the world.

Stage
+++++++++++++++++++++++++++++++++++++++++++++

:class:`pysetl.wokflow.Stage` is a collection of one or more Factory objects.

.. code-block:: python
    
    from pysetl.workflow import Stage

    stage = (
        Stage()
        .add_factory_from_type(CitizensFactory)
        .add_factory(...)
    )

    stage.run()

Pipeline
+++++++++++++++++++++++++++++++++++++++++++++

:class:`pysetl.workflow.Pipeline` is a collection of Stage objects run
sequentially. Besides organizing the execution of our project, Pipeline
solves data dependency injection across the data flow.

Pipelines come with a fancy `mermaidjs`_ diagram representation.

For advanced understanding see :mod:`pysetl.workflow.dispatcher` and 
:class:`pysetl.workflow.inspector`.

.. code-block:: python

    from pysetl.workflow import Pipeline

    pipeline = (
        Pipeline()
        .add_stage(stage)
        .add_stage_from_type(AnotherFactory)
        .run()
    )

Dependency Injection
---------------------------------------------

Across a Pipeline, you can handle data dependencies with
:class:`pysetl.workflow.Deliverable` and :class:`pysetl.workflow.Delivery`.

By default, a Factory will produce a ``Deliverable[T]`` and the Pipeline will
register each available deliverable produced by the factories.  You can take
advantage of this deliverable pool with a ``Delivery[T]`` declaration inside
your factory and the Pipeline dispatcher will try to solve the dependency by
searching for a ``Deliverable`` of the same type. If ambiguity occurs you can
pass a deliverable_id or explicitly state the expected producer class. Finally,
you can register external deliverables into the Pipeline.

.. code-block:: python

    from pysetl.workflow import Delivery, Deliverable


    class City(Schema):
        city: Column[StringType]
        country: Column[StringType]


    class CitizenCountry(Citizen):
        country: Column[StringType]


    class CitiesFactory(Factory[DataSet[City]]):
        ...


    class CitizenCountryFactory(Factory[DataSet[CitizenCountry]]):
        output: DataSet[CitizenCountry]
        citizens_delivery = Delivery[DataSet[Citizen]]()
        states_delivery = Delivery[DataSet[City]](producer=CitiesFactory)

        def read(self) -> Self:
            self.citizens = self.citizens_delivery.get()
            self.states = self.states_delivery.get()

            return self

        def process(self) -> Self:
            self.output = DataSet[CitizenCountry](self.citizens.join(self.states, "city"))

            return self

        def write(self) -> Self:
            return self

        def get(self) -> DataSet[CitizenCountry]:
            return self.output

    deliverable = Deliverable[DataSet[City]](fake_cities)

    pipeline = (
        Pipeline()
        .set_input_from_deliverable(deliverable)
        .add_stage(stage)
        .add_stage_from_type(CitizenCountryFactory)
        .run()
    )


.. _mermaidjs: https://mermaid.js.org/