# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import os
import sys
sys.path.insert(0, os.path.abspath('../../src/'))


project = 'PySetl'
copyright = '2023, Paul Marquez'
author = 'Paul Marquez'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx_rtd_theme",
    "sphinx.ext.napoleon",
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx"
]

templates_path = ['_templates']
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']

# -- Extensions configuration ------------------------------------------------
intersphinx_mapping = {
    "pydantic": ("https://docs.pydantic.dev/latest/", None),
    'python': ('https://docs.python.org/3', None),
    "pyspark": ("https://spark.apache.org/docs/latest/api/python/", None),
    "typedspark": ("https://typedspark.readthedocs.io/en/latest/", None),
    "pyarrow": ("https://arrow.apache.org/docs/", None)
}
