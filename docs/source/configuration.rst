Configuration
=============================================

:mod:`pysetl.config` is a type-safe way to configure our project.

This configuration will be passed to a
:class:`pysetl.storage.connector.Connector` and thus is relevant to avoid any
unexpected behaviour restricting the available values passed to our arguments.
PySetl provides some default enums in :mod:`pysetl.enums` to achieve this.

|config_diagram|

Currently, PySetl only provides configuration for file system storage
:class:`pysetl.config.FileConfig`.

Extending Config
---------------------------------------------

:class:`pysetl.config.Config` is an abstract base class that contains and
validates a :class:`dict`. PySetl implements validation with `pydantic`_ but you are
free to choose how to validate your arguments. Keep in mind that the validation
output will be passed to a :class:`pysetl.storage.connector.Connector` and for internal
PySetl connector it is expected a :class:`pydantic.BaseModel`.

:class:`pysetl.config.Config` comes with a handful of useful methods by default
and a :class:`pysetl.config.ConfigBuilder`. You can pass any number of arguments
regardless of the type but only those validated should by exposed in the
:meth:`pysetl.config.Config.config`. If you are using pydantic to validate, this
behaviour will depend on the model `configuration`_. In order to extend this
class you have to implement three methods

- :meth:`pysetl.config.Config.config`: the whole validated configuration.
- :meth:`pysetl.config.Config.reader_config`: a subset of configurations to
  pass to :meth:`pyspark.sql.DataFrameReader.options`.
- :meth:`pysetl.config.Config.writer_config`: a subset of configurations to
  pass to :meth:`pyspark.sql.DataFrameWriter.options`.


.. code-block:: python

    from pydantic import BaseModel

    class ConfigModel(BaseModel):
        arg1: int
        arg2: str
        arg3: float
    
    class ConcreteConfig(Config):
        @property
        def config(self):
            return ConfigModel(**self.params)
        
        @property
        def reader_config(self):
            return self.config

        @property
        def writer_config(self):
            return self.config

    # This should be fine since we are giving the right params
    concrete_config = ConcreteConfig(arg1=1, arg2="x", arg3=1.0, unexpected=10)
    concrete_config.config

    # On the other hand, this will fail
    bad_config = ConcreteConfig(arg1="1", arg2=1, arg3=1)
    bad_config.config


.. _pydantic: https://docs.pydantic.dev/latest/
.. _configuration: https://docs.pydantic.dev/latest/concepts/config/
.. |config_diagram| image:: https://mermaid.ink/svg/pako:eNp9j0ELwjAMhf9KyXn-gXqceBAF0WsvYc22wtrONh3I2H83OtHb3il57wvkzdBES6ChSzj26nzbm6BEdQyt69ROpI5uoHX_hn9jBeo8beZXTI9CvMmccgw_ACrwlDw6K5_N7wMD3JMnA1pGSy2WgQ2YsAiKheP9GRrQnApVUEaLTAeH0smDbnHI4pJ1HNNlbfspvbwAQBdRjA
    :alt: Configuration Hierarchy