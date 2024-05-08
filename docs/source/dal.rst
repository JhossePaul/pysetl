Data Access Layers
=============================================

PySetl provides two Data Access Layers:

- :mod:`pysetl.storage.connector`: low-level non-typed DAL working with
  :class:`pyspark.sql.DataFrame`.
- :mod:`pysetl.storage.repository`: high-level DAL to work with
  :class:`typedspark.DataSet`.

Connector
--------------------------------------------

Extending Connector
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

:class:`pysetl.storage.connector.BaseConnector` is an abstract base class with
the following structure:

.. code-block:: python

    class BaseConnector:
        @abstractmethod
        def read(self) -> DataFrame: pass

        @abstractmethod
        def write(self, dataframe: DataFrame) -> None: pass

The class :class:`pysetl.storage.connector.Connector` assumes a standard
:class:`pyspark.sql.SparkSession` to read and write data but this might not be
necessarily the case. If you want to implement your custom SparkSession
implementation just inherit from BaseConnector.

Implementations
++++++++++++++++++++++++++++++++++++++++++++

|connector_diagram|

FileConnector
********************************************

:class:`pysetl.storage.connector.FileConnector` has a
:class:`pyarrow.fs.FileSystem` to access basic functionalities to manipulate
files (drop, list, create directories). Hence, it gives support for:

- AWS S3 File System
- GCP Storage File System
- Hadoop File System
- Local File System

PyArrow tries to instantiate the corresponding file system with an educated
guess from the URI provided. See PyArrow `documentation`_ on file systems for
more details.

Repository
--------------------------------------------

Extending Repository
++++++++++++++++++++++++++++++++++++++++++++

:class:`pysetl.storage.repository.BaseRepository` is a type-safe layer over a
BaseConnector. Instead of returning a generic DataFrame, it will return a 
:class:`typedspark.DataSet`. This will allow you column-wise type validation. 
PySetl strongly supports :mod:`typedspark` philosophy and encourages its use over
Connectors to reduce runtime errors and development time. 

.. code-block:: python

    class Repository(Generic[T], HasSparkSession, HasLogger):
        @abstractmethod
        def load(self) -> DataSet[T]: pass

        @abstractmethod
        def save(self, dataset: DataSet[T]) -> None: pass

Repository inherits from :class:`typing.Generic` and expects a mandatory
type parameter. It should fail if not given.

SparkRepository implementation expects a standard Connector and a standard
SparkSession too. You can instantiate it by passing a Connector and the type
parameter, or you can use the convenient
:class:`pysetl.storage.repository.SparkRepositoryBuilder` and simply pass a 
configuration.

.. _documentation: https://arrow.apache.org/docs/python/filesystems.html
.. |connector_diagram| image:: https://mermaid.ink/svg/pako:eNqNkMEKwjAMhl-l5KwvUG8qHkRB9NpLWDNXWNvZpoKMvbtRx8CdllPy58sP-XuooiXQcE_YNep03ZigpLaYaRdDoIpjUmspNY0jMlsfXEtz5E8bXfJzAXXB9CjEC8hjjmGSTIAVeEoenZWf-s-VAW7IkwEtraUaS8sGTBgExcLx9goVaE6FVlA6i0x7h5KGB11jm0Ul68T7_MvpG9fwBtBPZ0M
    :alt: Connector Hierarchy
