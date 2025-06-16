# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import os
import sys
from datetime import date
sys.path.insert(0, os.path.abspath('../../src/'))


current_year = date.today().year
project = 'PySetl'
copyright = f'{current_year}, Paul Marquez'
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
html_logo = "_static/logo_pysetl.png"
html_theme_options = {
    'logo_only': True,
    'display_version': False,
}
html_css_files = [
    'css/style.css',
]

# -- Extensions configuration ------------------------------------------------
intersphinx_mapping = {
    "pydantic": ("https://docs.pydantic.dev/latest/", None),
    'python': ('https://docs.python.org/3', None),
    "pyspark": ("https://spark.apache.org/docs/latest/api/python/", None),
    "typedspark": ("https://typedspark.readthedocs.io/en/latest/", None),
    "pyarrow": ("https://arrow.apache.org/docs/", None)
}
