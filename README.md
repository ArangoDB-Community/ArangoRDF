# DEVELOPMENT VERSION - WIP - EXPECT BREAKING CHANGES
___

# Arango-RDF

[![PyPI version badge](https://img.shields.io/pypi/v/arango-rdf?color=3775A9&style=for-the-badge&logo=pypi&logoColor=FFD43B)](https://pypi.org/project/arango-rdf/)
[![Python versions badge](https://img.shields.io/pypi/pyversions/arango-rdf?color=3776AB&style=for-the-badge&logo=python&logoColor=FFD43B)](https://pypi.org/project/arango-rdf/)

[![License](https://img.shields.io/github/license/ArangoDB-Community/ArangoRDF?color=9E2165&style=for-the-badge)](https://github.com/ArangoDB-Community/ArangoRDF/blob/main/LICENSE)
[![Code style: black](https://img.shields.io/static/v1?style=for-the-badge&label=code%20style&message=black&color=black)](https://github.com/psf/black)
[![Downloads](https://img.shields.io/badge/dynamic/json?style=for-the-badge&color=282661&label=Downloads&query=total_downloads&url=https://api.pepy.tech/api/projects/arango-rdf)](https://pepy.tech/project/arango-rdf)

<a href="https://www.arangodb.com/" rel="arangodb.com"><img src="./examples/assets/adb_logo.png" width=10%/>
<a href="https://www.w3.org/RDF/" rel="w3.org/RDF"><img src="./examples/assets/rdf_logo.png" width=7% /></a>

Import/Export RDF graphs with ArangoDB

## About RDF

RDF is a standard model for data interchange on the Web. RDF has features that facilitate data merging even if the underlying schemas differ, and it specifically supports the evolution of schemas over time without requiring all the data consumers to be changed.

RDF extends the linking structure of the Web to use URIs to name the relationship between things as well as the two ends of the link (this is usually referred to as a "triple"). Using this simple model, it allows structured and semi-structured data to be mixed, exposed, and shared across different applications.

This linking structure forms a directed, labeled graph, where the edges represent the named link between two resources, represented by the graph nodes. This graph view is the easiest possible mental model for RDF and is often used in easy-to-understand visual explanations.

Resources to get started:
* [RDF Data Model Example](https://docs.stardog.com/tutorials/rdf-graph-data-model)
* [RDFLib (Python)](https://pypi.org/project/rdflib/)

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

```py
from arango import ArangoClient
from arango_rdf import ArangoRDF

db = ArangoClient(hosts="http://localhost:8529").db("_system", username="root", password="")

adb_rdf = ArangoRDF(db, sub_graph="music")

config = {'normalize_literals': True} # {'normalize_literals': False}

# RDF Import
adb_rdf.init_rdf_collections(bnode="Blank")
adb_rdf.import_rdf("./examples/data/music_schema.ttl", format="ttl", config)
adb_rdf.import_rdf("./examples/data/beatles.ttl", format="ttl", config)

# RDF Export
adb_rdf.export(f"./examples/data/rdfExport.ttl", format="ttl")

# Re-import RDF Export
adb_rdf.import_rdf(f"./examples/data/rdfExport.ttl", format="ttl")

# Ontology Import
adb_rdf_2 = ArangoRDF(db, graph="ontology_iao")
adb_rdf_2.init_ontology_collections()
adb_rdf_2.import_ontology("./examples/data/iao.owl")
```

##  Development & Testing

1. `git clone https://github.com/ArangoDB-Community/ArangoRDF`
2. `cd arango-rdf`
3. (create virtual environment of choice)
4. `pip install -e .[dev]`
5. (create an ArangoDB instance with method of choice)
6. `python tests/test.py` (assumes `username=root`, `password=openSesame`)
