PySetl - Python Spark ETL Framework
============================================
[![PyPI Version][pypi-image]][pypi-url]
[![Build Status][build-image]][build-url]
[![Code Coverage][coverage-image]][coverage-url]


Overview
--------------------------------------------
PySetl is a framework focused to improve readability and structure of PySpark
ETL projects. Also, it is designed to take advantage of Python's typing syntax
to reduce runtime errors through linting tools and verifying types at runtime.
Thus, effectively enhacing stability for large ETL pipelines.

In order to accomplish this task we provide some tools:

- pysetl.config: Type-validated configurations.
- pysetl.storage: Agnostic and extensible data sources connections.
- pysetl.workflow: Pipeline management and dependency injection.


Installation
--------------------------------------------
PySetl is available in PyPI:

```
$ pip install pysetl
```

PySetl doesn't list `pyspark` as dependency since most environments have their own
Spark environment. Nevertheless, you can install pyspark running:

```
$ pip install pysetl[pyspark]
```

Why use PySetl?
--------------------------------------------
- Model complex data pipelines.
- Reduce risks at production with type-safe development.
- Improve large project structure and readability.

Acknowledgments
--------------------------------------------

PySetl is a port from [SETL](https://setl-framework.github.io/setl/).  We want
to fully recognise this package is heavily inspired by the work of the SETL
team. We just adapted things to work in Python. 

PySetl is designed with Python typing syntax at its core. Hence, we strongly
suggest [typedspark](https://typedspark.readthedocs.io/en/latest/) and
[pydantic](https://docs.pydantic.dev/latest/) for development.

[pypi-image]: https://img.shields.io/pypi/v/pysetl
[pypi-url]: https://pypi.org/project/pysetl
[build-image]: https://github.com/JhossePaul/pysetl/actions/workflows/build.yml/badge.svg
[build-url]: https://github.com/JhossePaul/pysetl/actions/workflows/build.yml
[coverage-image]: https://codecov.io/gh/JhossePaul/pysetl/branch/main/graph/badge.svg
[coverage-url]: https://codecov.io/gh/JhossePaul/pysetl