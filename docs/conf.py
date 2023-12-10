# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys

sys.path.insert(0, os.path.abspath('../arango_rdf'))

project = 'ArangoRDF'
copyright = '2023, Anthony Mahanna'
author = 'Anthony Mahanna'
release = '0.1.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx_rtd_theme",
    "sphinx.ext.autodoc",
    "sphinx.ext.doctest",
    "sphinx.ext.viewcode",
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'alabaster'
html_static_path = ['_static']

autodoc_member_order = "bysource"


doctest_global_setup = """
from arango import ArangoClient
from arango_rdf import ArangoRDF
from rdflib import Graph

# Initialize the ArangoDB client db.
db = ArangoClient().db('_system', username='root', password='passwd')

# Initialize the ArangoRDF client.
adbrdf = ArangoRDF(db)

# Create a new RDF graph.
rdf_graph = Graph()
rdf_graph.parse("https://raw.githubusercontent.com/stardog-union/stardog-tutorials/master/music/beatles.ttl", format="ttl")

# RDF to ArangoDB
adb_graph_rpt = adbrdf.rdf_to_arangodb_by_rpt("BeatlesRPT", rdf_graph, overwrite_graph=True)
adb_graph_pgt = adbrdf.rdf_to_arangodb_by_pgt("BeatlesPGT", rdf_graph, overwrite_graph=True)

# ArangoDB to RDF
rdf_graph_2, adb_mapping = adbrdf.arangodb_graph_to_rdf("Beatles", Graph())
"""