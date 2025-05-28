# ArangoRDF

[![build](https://github.com/ArangoDB-Community/ArangoRDF/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/ArangoDB-Community/ArangoRDF/actions/workflows/build.yml)
[![CodeQL](https://github.com/ArangoDB-Community/ArangoRDF/actions/workflows/analyze.yml/badge.svg?branch=main)](https://github.com/ArangoDB-Community/ArangoRDF/actions/workflows/analyze.yml)
[![Coverage Status](https://coveralls.io/repos/github/ArangoDB-Community/ArangoRDF/badge.svg?branch=main)](https://coveralls.io/github/ArangoDB-Community/ArangoRDF?branch=main)
[![Last commit](https://img.shields.io/github/last-commit/ArangoDB-Community/ArangoRDF)](https://github.com/ArangoDB-Community/ArangoRDF/commits/main)

[![PyPI version badge](https://img.shields.io/pypi/v/arango-rdf?color=3775A9&style=for-the-badge&logo=pypi&logoColor=FFD43B)](https://pypi.org/project/arango-rdf/)
[![Python versions badge](https://img.shields.io/pypi/pyversions/arango-rdf?color=3776AB&style=for-the-badge&logo=python&logoColor=FFD43B)](https://pypi.org/project/arango-rdf/)

[![License](https://img.shields.io/github/license/ArangoDB-Community/ArangoRDF?color=9E2165&style=for-the-badge)](https://github.com/ArangoDB-Community/ArangoRDF/blob/main/LICENSE)
[![Code style: black](https://img.shields.io/static/v1?style=for-the-badge&label=code%20style&message=black&color=black)](https://github.com/psf/black)
[![Downloads](https://img.shields.io/pepy/dt/arango-rdf?style=for-the-badge&color=282661)](https://pepy.tech/project/arango-rdf)

<a href="https://www.arangodb.com/" rel="arangodb.com"><img src="https://raw.githubusercontent.com/ArangoDB-Community/ArangoRDF/main/examples/assets/adb_logo.png" width=10%/>
<a href="https://www.w3.org/RDF/" rel="w3.org/RDF"><img src="https://raw.githubusercontent.com/ArangoDB-Community/ArangoRDF/main/examples/assets/rdf_logo.png" width=7% /></a>

Convert RDF Graphs to ArangoDB, and vice-versa.

## About RDF

RDF is a standard model for data interchange on the Web. RDF has features that facilitate data merging even if the underlying schemas differ, and it specifically supports the evolution of schemas over time without requiring all the data consumers to be changed.

RDF extends the linking structure of the Web to use URIs to name the relationship between things as well as the two ends of the link (this is usually referred to as a "triple"). Using this simple model, it allows structured and semi-structured data to be mixed, exposed, and shared across different applications.

This linking structure forms a directed, labeled graph, where the edges represent the named link between two resources, represented by the graph nodes. This graph view is the easiest possible mental model for RDF and is often used in easy-to-understand visual explanations.

Resources to get started:
* [RDF Primer](https://www.w3.org/TR/rdf11-concepts/)
* [RDFLib (Python)](https://pypi.org/project/rdflib/)
* [RDF Base Terminology](https://docs.arangodb.com/stable/data-science/adapters/arangodb-rdf-adapter/#terminology)

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
<a href="https://colab.research.google.com/github/ArangoDB-Community/ArangoRDF/blob/main/examples/ArangoRDF.ipynb" target="_parent"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/></a>

```py
from rdflib import Graph
from arango import ArangoClient
from arango_rdf import ArangoRDF

db = ArangoClient().db()

adbrdf = ArangoRDF(db)

def beatles():
    g = Graph()
    g.parse("https://raw.githubusercontent.com/ArangoDB-Community/ArangoRDF/main/tests/data/rdf/beatles.ttl", format="ttl")
    return g
```

### RDF to ArangoDB

**Note**: RDF-to-ArangoDB functionality has been implemented using concepts described in the paper
*[Transforming RDF-star to Property Graphs: A Preliminary Analysis of Transformation Approaches](https://arxiv.org/abs/2210.05781)*. So we offer two transformation approaches:

1. [RDF-Topology Preserving Transformation (RPT)](https://arangordf.readthedocs.io/en/latest/rdf_to_arangodb_rpt.html)
2. [Property Graph Transformation (PGT)](https://arangordf.readthedocs.io/en/latest/rdf_to_arangodb_pgt.html)

```py
# 1. RDF-Topology Preserving Transformation (RPT)
adbrdf.rdf_to_arangodb_by_rpt(name="BeatlesRPT", rdf_graph=beatles(), overwrite_graph=True)

# 2. Property Graph Transformation (PGT) 
adbrdf.rdf_to_arangodb_by_pgt(name="BeatlesPGT", rdf_graph=beatles(), overwrite_graph=True)
```

RPT preserves the RDF Graph structure by transforming each RDF Statement into an ArangoDB Edge.

PGT on the other hand ensures that Datatype Property Statements are mapped as ArangoDB Document Properties.

```ttl
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:book ex:publish_date "1963-03-22"^^xsd:date .
ex:book ex:pages "100"^^xsd:integer .
ex:book ex:cover 20 .
ex:book ex:index 55 .
```

| RPT | PGT |
|:-------------------------:|:-------------------------:|
| ![image](https://user-images.githubusercontent.com/43019056/232347662-ab48ebfb-e215-4aff-af28-a5915414a8fd.png) | ![image](https://user-images.githubusercontent.com/43019056/232347681-c899ef09-53c7-44de-861e-6a98d448b473.png) |


### ArangoDB to RDF

```py
# Assumption: "BeatlesPGT" loaded in ArangoDB ^

# 1. Graph to RDF
rdf_graph = adbrdf.arangodb_graph_to_rdf("BeatlesPGT", rdf_graph=Graph())

# 2. Collections to RDF
rdf_graph_2 = adbrdf.arangodb_collections_to_rdf(
    "BeatlesPGT",
    rdf_graph=Graph(),
    v_cols={"Album", "Band"},
    e_cols={"artist"},
)

# 3. Metagraph to RDF
rdf_graph_3 = adbrdf.arangodb_to_rdf(
    name=name,
    rdf_graph=Graph(),
    metagraph={
        "vertexCollections": {
            "Album": {"name", "date"},
            "Band": {"name"}
        },
        "edgeCollections": {
            "artist": {}
        },
    },
)
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
