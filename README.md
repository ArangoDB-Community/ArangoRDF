# ArangoRDF

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

Convert RDF Graphs to ArangoDB, and vice-versa.

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
from rdflib import Graph
from arango import ArangoClient
from arango_rdf import ArangoRDF

db = ArangoClient(hosts="http://localhost:8529").db("_system_", username="root", password="")

adbrdf = ArangoRDF(db)

g = Graph()
g.parse("https://raw.githubusercontent.com/stardog-union/stardog-tutorials/master/music/beatles.ttl")

# RDF to ArangoDB
###################################################################################

# 1.1: RDF-Topology Preserving Transformation (RPT)
adbrdf.rdf_to_arangodb_by_rpt("Beatles", g, overwrite_graph=True)

# 1.2: Property Graph Transformation (PGT) 
adbrdf.rdf_to_arangodb_by_pgt("Beatles", g, overwrite_graph=True)

g = adbrdf.load_meta_ontology(g)

# 1.3: RPT w/ Graph Contextualization
adbrdf.rdf_to_arangodb_by_rpt("Beatles", g, contextualize_graph=True, overwrite_graph=True)

# 1.4: PGT w/ Graph Contextualization
adbrdf.rdf_to_arangodb_by_pgt("Beatles", g, contextualize_graph=True, overwrite_graph=True)

# 1.5: PGT w/ ArangoDB Document-to-Collection Mapping Exposed
adb_mapping = adbrdf.build_adb_mapping_for_pgt(g)
print(adb_mapping.serialize())
adbrdf.rdf_to_arangodb_by_pgt("Beatles", g, adb_mapping, contextualize_graph=True, overwrite_graph=True)

# ArangoDB to RDF
###################################################################################

# Start from scratch!
g = Graph()
g.parse("https://raw.githubusercontent.com/stardog-union/stardog-tutorials/master/music/beatles.ttl")
adbrdf.rdf_to_arangodb_by_pgt("Beatles", g, overwrite_graph=True)

# 2.1: Via Graph Name
g2, adb_mapping_2 = adbrdf.arangodb_graph_to_rdf("Beatles", Graph())

# 2.2: Via Collection Names
g3, adb_mapping_3 = adbrdf.arangodb_collections_to_rdf(
    "Beatles",
    Graph(),
    v_cols={"Album", "Band", "Class", "Property", "SoloArtist", "Song"},
    e_cols={"artist", "member", "track", "type", "writer"},
)

print(len(g2), len(adb_mapping_2))
print(len(g3), len(adb_mapping_3))

print('--------------------')
print(g2.serialize())
print('--------------------')
print(adb_mapping_2.serialize())
print('--------------------')
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

## Additional Info: RDF to ArangoDB

RDF-to-ArangoDB functionality has been implemented using concepts described in the paper *[Transforming RDF-star to Property Graphs: A Preliminary Analysis of Transformation Approaches](https://arxiv.org/abs/2210.05781)*.

In other words, `ArangoRDF` offers 2 RDF-to-ArangoDB transformation methods:
1. RDF-topology Preserving Transformation (RPT): `ArangoRDF.rdf_to_arangodb_by_rpt()`
2. Property Graph Transformation (PGT): `ArangoRDF.rdf_to_arangodb_by_pgt()`

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

--------------------
### RPT


The `ArangoRDF.rdf_to_arangodb_by_rpt` method will store the RDF Resources of your RDF Graph under the following ArangoDB Collections:
    
    - {graph_name}_URIRef: The Document collection for `rdflib.term.URIRef` resources.
    - {graph_name}_BNode: The Document collection for`rdflib.term.BNode` resources.
    - {graph_name}_Literal: The Document collection for `rdflib.term.Literal` resources.
    - {graph_name}_Statement: The Edge collection for all triples/quads.

--------------------
### PGT

In contrast to RPT, the `ArangoRDF.rdf_to_arangodb_by_pgt` method will rely on the nature of the RDF Resource/Statement to determine which ArangoDB Collection it belongs to. This is referred as the **ArangoDB Collection Mapping Process**. This process relies on 2 fundamental URIs:

1) `<http://www.arangodb.com/collection>` (adb:collection)
    - Any RDF Statement of the form `<http://example.com/Bob> <adb:collection> "Person"` will map the Subject to the ArangoDB "Person" document collection.
    
2) `<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>` (rdf:type)
    - This strategy is divided into 3 cases:

        1. If an RDF Resource only has one `rdf:type` statement,
            then the local name of the RDF Object is used as the ArangoDB
            Document Collection name. For example,
            `<http://example.com/Bob> <rdf:type> <http://example.com/Person>`
            would create an JSON Document for `<http://example.com/Bob>`,
            and place it under the `Person` Document Collection.
            NOTE: The RDF Object will also have its own JSON Document
            created, and will be placed under the "Class"
            Document Collection.

        2. If an RDF Resource has multiple `rdf:type` statements,
            with some (or all) of the RDF Objects of those statements
            belonging in an `rdfs:subClassOf` Taxonomy, then the
            local name of the "most specific" Class within the Taxonomy is
            used (i.e the Class with the biggest depth). If there is a
            tie between 2+ Classes, then the URIs are alphabetically
            sorted & the first one is picked.

        3. If an RDF Resource has multiple `rdf:type` statements, with none
            of the RDF Objects of those statements belonging in an
            `rdfs:subClassOf` Taxonomy, then the URIs are
            alphabetically sorted & the first one is picked. The local
            name of the selected URI will be designated as the Document
            collection for that Resource.
--------------------
