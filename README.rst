PySetl - A PySpark ETL Framework
============================================
|PyPI Badge|
|Build Status|
|Code Coverage|
|Documentation Status|

Overview
--------------------------------------------
PySetl is a framework focused to improve readability and structure of PySpark
ETL projects. Also, it is designed to take advantage of Python's typing syntax
to reduce runtime errors through linting tools and verifying types at runtime.
Thus, effectively enhacing stability for large ETL pipelines.

In order to accomplish this task we provide some tools:

- :code:`pysetl.config`: Type-safe configuration.
- :code:`pysetl.storage`: Agnostic and extensible data sources connections.
- :code:`pysetl.workflow`: Pipeline management and dependency injection.

PySetl is designed with Python typing syntax at its core. Hence, we strongly
suggest `typedspark`_ and `pydantic`_ for development.

Why use PySetl?
--------------------------------------------
- Model complex data pipelines.
- Reduce risks at production with type-safe development.
- Improve large project structure and readability.

Installation
--------------------------------------------
PySetl is available in PyPI:

.. code-block:: bash

    pip install pysetl

PySetl doesn't list `pyspark` as dependency since most environments have their own
Spark environment. Nevertheless, you can install pyspark running:

.. code-block:: bash

    pip install "pysetl[pyspark]"

Acknowledgments
--------------------------------------------

PySetl is a port from `SETL`_.  We want
to fully recognise this package is heavily inspired by the work of the SETL
team. We just adapted things to work in Python. 

.. _typedspark: https://typedspark.readthedocs.io/en/latest/
.. _pydantic: https://docs.pydantic.dev/latest/
.. _SETL: https://setl-framework.github.io/setl/ 

.. |PyPI Badge| image:: https://img.shields.io/pypi/v/pysetl
    :target: https://pypi.org/project/pysetl

.. |Build Status| image:: https://github.com/JhossePaul/pysetl/actions/workflows/build.yml/badge.svg
    :target: https://github.com/JhossePaul/pysetl/actions/workflows/build.yml

.. |Code Coverage| image:: https://codecov.io/gh/JhossePaul/pysetl/branch/main/graph/badge.svg
    :target: https://codecov.io/gh/JhossePaul/pysetl

.. |Documentation Status| image:: https://readthedocs.org/projects/pysetl/badge/?version=latest
    :target: https://pysetl.readthedocs.io/en/latest/?badge=latest
