# DEVELOPMENT VERSION - WIP - EXPECT BREAKING CHANGES
___

# Arango-RDF

[![build](https://github.com/ArangoDB-Community/ArangoRDF/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/ArangoDB-Community/ArangoRDF/actions/workflows/build.yml)
[![CodeQL](https://github.com/ArangoDB-Community/ArangoRDF/actions/workflows/analyze.yml/badge.svg?branch=main)](https://github.com/ArangoDB-Community/ArangoRDF/actions/workflows/analyze.yml)
[![Coverage Status](https://coveralls.io/repos/github/ArangoDB-Community/ArangoRDF/badge.svg?branch=main)](https://coveralls.io/github/ArangoDB-Community/ArangoRDF?branch=main)
[![Last commit](https://img.shields.io/github/last-commit/ArangoDB-Community/ArangoRDF)](https://github.com/ArangoDB-Community/ArangoRDF/commits/main)

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
from arango_rdf import ArangoRDF

from arango import ArangoClient
from rdflib import Graph

db = ArangoClient(hosts="http://localhost:8529").db("_system_", username="root", password="")

adbrdf = ArangoRDF(db)

g = Graph()
g.parse("https://raw.githubusercontent.com/stardog-union/stardog-tutorials/master/music/beatles.ttl", format="ttl")

db.delete_graph("BeatlesPGT", ignore_missing=True, drop_collections=True)
db.delete_graph("BeatlesRPT", ignore_missing=True, drop_collections=True)

# RDF to ArangoDB: RDF-Topology Presevering Transformation (RPT)

adbrdf.rdf_to_arangodb_by_rpt("BeatlesRPT", g)

# RDF to ArangoDB: Property Graph Transformation (PGT)

adbrdf.rdf_to_arangodb_by_pgt("BeatlesPGT", g)

# ArangoDB to RDF: By Graph Name

adbrdf.arangodb_graph_to_rdf("BeatlesRPT", Graph())
adbrdf.arangodb_graph_to_rdf("BeatlesPGT", Graph())

# ArangoDB to RDF: By Collection Names

adbrdf.arangodb_collections_to_rdf(
    "BeatlesRPT",
    Graph(),
    v_cols={"BeatlesRPT_URIRef", "BeatlesRPT_BNode", "BeatlesRPT_Literal"},
    e_cols={"BeatlesRPT_Statement"},
)

adbrdf.arangodb_collections_to_rdf(
    "BeatlesPGT",
    Graph(),
    v_cols={"Band", "SoloArtist", "Album", "Song"},
    e_cols={"member", "artist", "track", "writer"},
)

# ArangoDB to RDF: By Metagraph

metagraph = {
    "vertexCollections": {
        "SoloArtist": {},  # TODO - Figure out use case
        "Band": {},
    },
    "edgeCollections": {"member": {}},
}

adbrdf.arangodb_to_rdf("BeatlesPGT", Graph(), metagraph)
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

