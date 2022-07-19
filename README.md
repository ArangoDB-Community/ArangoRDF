# DEVELOPMENT VERSION - WIP - EXPECT BREAKING CHANGES
___

# Arango-RDF

[![PyPI version badge](https://img.shields.io/pypi/v/arango-rdf?color=3775A9&style=for-the-badge&logo=pypi&logoColor=FFD43B)](https://pypi.org/project/arango-rdf/)
[![Python versions badge](https://img.shields.io/pypi/pyversions/arango-rdf?color=3776AB&style=for-the-badge&logo=python&logoColor=FFD43B)](https://pypi.org/project/arango-rdf/)

[![License](https://img.shields.io/github/license/ArangoDB-Community/ArangoRDF?color=9E2165&style=for-the-badge)](https://github.com/ArangoDB-Community/ArangoRDF/blob/main/LICENSE)
[![Code style: black](https://img.shields.io/static/v1?style=for-the-badge&label=code%20style&message=black&color=black)](https://github.com/psf/black)
[![Downloads](https://img.shields.io/badge/dynamic/json?style=for-the-badge&color=282661&label=Downloads&query=total_downloads&url=https://api.pepy.tech/api/projects/arango-rdf)](https://pepy.tech/project/arango-rdf)

<a href="https://www.arangodb.com/" rel="arangodb.com"><img src="https://raw.githubusercontent.com/ArangoDB-Community/ArangoRDF/main/examples/assets/adb_logo.png" width=10%/>
<a href="https://www.w3.org/RDF/" rel="w3.org/RDF"><img src="https://raw.githubusercontent.com/ArangoDB-Community/ArangoRDF/main/examples/assets/rdf_logo.png" width=7% /></a>

Import/Export RDF graphs with ArangoDB

## About RDF

RDF is a standard model for data interchange on the Web. RDF has features that facilitate data merging even if the underlying schemas differ, and it specifically supports the evolution of schemas over time without requiring all the data consumers to be changed.

RDF extends the linking structure of the Web to use URIs to name the relationship between things as well as the two ends of the link (this is usually referred to as a "triple"). Using this simple model, it allows structured and semi-structured data to be mixed, exposed, and shared across different applications.

This linking structure forms a directed, labeled graph, where the edges represent the named link between two resources, represented by the graph nodes. This graph view is the easiest possible mental model for RDF and is often used in easy-to-understand visual explanations.

Resources to get started:
* [RDF Primer](https://www.w3.org/TR/rdf11-concepts/)
* [RDFLib (Python)](https://pypi.org/project/rdflib/)
* [One Example for Modeling RDF as ArangoDB Graphs](https://www.arangodb.com/docs/stable/data-modeling-graphs-from-rdf.html)
## Installation

#### Latest Release
```
pip install arango-rdf
```
#### Current State
```
pip install git+https://github.com/ArangoDB-Community/ArangoRDF
```

##  Quickstart
Run the full version with Google Colab: <a href="https://colab.research.google.com/github/ArangoDB-Community/ArangoRDF/blob/main/examples/ArangoRDF.ipynb" target="_parent"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/></a>

```py
from arango import ArangoClient
from arango_rdf import ArangoRDF

db = ArangoClient(hosts="http://localhost:8529").db(
    "rdf", username="root", password="openSesame"
)

# Clean up existing data and collections
if db.has_graph("default_graph"):
    db.delete_graph("default_graph", drop_collections=True, ignore_missing=True)

# Initializes default_graph and sets RDF graph identifier (ArangoDB sub_graph)
# Optional: sub_graph (stores graph name as the 'graph' attribute on all edges in Statement collection)
# Optional: default_graph (name of ArangoDB Named Graph, defaults to 'default_graph',
#           is root graph that contains all collections/relations)
adb_rdf = ArangoRDF(db, sub_graph="http://data.sfgov.org/ontology") 
config = {"normalize_literals": False}  # default: False

# RDF Import
adb_rdf.init_rdf_collections(bnode="Blank")

# Start with importing the ontology
adb_rdf.import_rdf("./examples/data/airport-ontology.owl", format="xml", config=config, save_config=True)

# Next, let's import the actual graph data
adb_rdf.import_rdf(f"./examples/data/sfo-aircraft-partial.ttl", format="ttl", config=config, save_config=True)


# RDF Export
# WARNING:
# Exports ALL collections of the database,
# currently does not account for default_graph or sub_graph
# Results may vary, minifying may occur
adb_rdf.export_rdf(f"./examples/data/rdfExport.xml", format="xml")

# Drop graph and ALL documents and collections to test import from exported data
if db.has_graph("default_graph"):
    db.delete_graph("default_graph", drop_collections=True, ignore_missing=True)

# Re-initialize our RDF Graph
# Initializes default_graph and sets RDF graph identifier (ArangoDB sub_graph)
adb_rdf = ArangoRDF(db, sub_graph="http://data.sfgov.org/ontology")

adb_rdf.init_rdf_collections(bnode="Blank")

config = adb_rdf.get_config_by_latest() # gets the last config saved
# config = adb_rdf.get_config_by_key_value('graph', 'music')
# config = adb_rdf.get_config_by_key_value('AnyKeySuppliedInConfig', 'SomeValue')

# Re-import Exported data
adb_rdf.import_rdf(f"./examples/data/rdfExport.xml", format="xml", config=config)

```

##  Development & Testing

1. `git clone https://github.com/ArangoDB-Community/ArangoRDF`
2. `cd arango-rdf`
3. (create virtual environment of choice)
4. `pip install -e .[dev]`
5. (create an ArangoDB instance with method of choice)
6. `pytest --url <> --dbName <> --username <> --password <>`

**Note**: A `pytest` parameter can be omitted if the endpoint is using its default value:
```python
def pytest_addoption(parser):
    parser.addoption("--url", action="store", default="http://localhost:8529")
    parser.addoption("--dbName", action="store", default="_system")
    parser.addoption("--username", action="store", default="root")
    parser.addoption("--password", action="store", default="")
```

