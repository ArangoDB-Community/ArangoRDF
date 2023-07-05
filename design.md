# ArangoRDF: Data interoperability for RDF and Property Graphs

https://github.com/ArangoDB-Community/ArangoRDF

ArangoRDF is python package for converting ArangoDB Graphs into RDF (Resource Description Framework), and vice-versa. 

Technologies similar to ArangoRDF include Neo4J’s NeoSemantics package (https://neo4j.com/labs/neosemantics/), and an open source Java package called RDF2PG (https://github.com/renzoar/rdf2pg).

It can be considered as the 5th installment in the ArangoDB "Adapter Suite", sharing functional similarities with ArangoDB's NetworkX, cuGraph, DGL, and PyG adapters.

This Design Doc reflects the implementation overhaul of ArangoRDF conducted during January - June 2023 (https://github.com/ArangoDB-Community/ArangoRDF/pull/15).

## People
* Owner TBD
* Creator <anthony.mahanna@arangodb.com>
* Creator <arthur@arangodb.com>
* Creator <christopher@arangodb.com>
* Reviewer TBD


## Timeline
- January 2023 - June 2023

## Customer/ Stakeholder
- Rakuten
- University of MaryLand Medical System
- NZ Plant & Food Research
- Booz Allen Hamilton
- SAIC
- etc.

## Research Potential

The overhaul was implemented as part of an Undergrad Thesis project at the University of Ottawa (known as CSI4900 - Honours Project). Its development was presented at the end of April 2023 to supervising professors at uOttawa, and was awarded the Cognos Prize for being recognized as the top Honours Project within the Computer Science department for that year (https://www.site.uottawa.ca/~flocchin/CSI4900/cognos_en.html). 

This has opened the following research opportunities for ArangoRDF, which have been proposed by the supervising professors: 
1. A demonstration paper to formally describe ArangoRDF’s, system functionality, user types, use cases, and architecture 
2. A follow-up short paper or applied paper on ArangoRDF's novel algorithms (if any)
3. A presentation of ArangoRDF at Semantic Web and/or Information Management conferences such as SEMAPRO or ACM CIKM.

# Goal

Address the limited scope of the previous state of ArangoRDF, and to improve upon it by:

1. Introducing RDF to Property Graph transformation algorithms presented in the "Transforming RDF-star to Property Graphs: A Preliminary Analysis of Transformation Approaches" Research Paper (https://arxiv.org/abs/2210.05781), co-authored by Arthur Keen (ArangoDB). These transformation algorithms are known as RPT & PGT.

2. Refactoring the ArangoDB to RDF transformation algorithm

3. Refactoring the Test Suite & achieve 100% code coverage 

4. Refactoring the ArangoRDF UX to unify it with its neighboring adapters

5. Refactoring the Jupyter Notebook to reflect the ArangoRDF changes

6. Refactoring the API Docstrings

7. Making general code quality changes

## Motivation

1. Its main prupose is to bridge the gap between the Semantic Community and the Property Graph community. ArangoRDF allows ArangoDB users to run inference & processing on their ArangoDB graphs by converting them into RDF, and allows RDF users to leverage AQL to run scalable graph algorithms & analytics.

2. Given ArangoDB’s prioritization of data interoperability, ArangoDB can now claim it "speaks" RDF, among other data modeling "languages". In other words, it is possible to virtualize ArangoDB as a triple store.

3. From a performance point of view, using the PGT algorithm (RDF to ArangoDB) will cut an RDF Graph's number of edges down by 3-to-5x (on average), resulting in a lighter graph to query in ArangoDB. 

4. Lastly, it was paramout for ArangoRDF to work towards achieving lossless round-tripping (i.e RDF to ArangoDB to RDF, or ArangoDB to RDF to ArangoDB).

# Scope

- RDF to ArangoDB
- ArangoDB to RDF
- Documentation
- Test Suite
- UX/API

Most of ArangoRDF's existing codebase has been discarded in favor of rebuilding.

# Risk
> How good is the testcoverage for existing code which needs to be touched for this feature?

Code coverage was increased from 136 of 155 relevent lines (87%) to 792 of 804 (99%).

## Technical Assumptions
Supported Python versions are 3.7 to 3.11, although 3.7 has reached EOL as of June 27 2023.

## Technical Restrictions
ArangoRDF is only available as a Python package.

## Technical Challenges
- Memory optimization; how do transformations look like for large graphs?
- Data streaming; one HTTP call per triple, or bulk processing? and at what cost?
- Support for all kinds of RDF creations; RDF is the wild west in the world of graphs. Is ArangoRDF dynamic enough?
- Future RDF Schema changes: How do we maintain ArangoRDF as the world of RDF evolves?
- Graph contextualization: To what level should ArangoRDF provide domain/range introspection & inference?
- TODO: What else?

## Dependencies

Only a few dependency changes were made.

Added dependencies:

- rich (https://pypi.org/project/rich/)
- cityhash (https://pypi.org/project/cityhash/)

Removed dependences:
- tqdm (replaced by rich)

# Limitations
> What is the tested scale?

TBD

# New APIs and breaking changes
> Do you have to introduce new APIs (user-facing or internally)?
ArangoRDF's API has changed to support the new transformation techniques offered.

Yes, see below.

Previous:
```py
from arango_rdf import ArangoRDF

from arango import ArangoClient

db = ArangoClient(hosts="http://localhost:8529").db("rdf", username="root", password="openSesame")

adb_rdf = ArangoRDF(db, sub_graph="http://data.sfgov.org/ontology")

adb_rdf.init_rdf_collections(bnode="Blank")

# ADB to RDF
adb_graph = adb_rdf.import_rdf("./examples/data/airport-ontology.owl", format="xml")
adb_graph = adb_rdf.import_rdf("./examples/data/sfo-aircraft-partial.ttl", format="ttl")

# RDF to ArangoDB
rdf_graph = adb_rdf.export_rdf(f"./examples/data/rdfExport.xml", format="xml")
```

Current:
```py
from arango_rdf import ArangoRDF

from arango import ArangoClient
from rdflib import Graph

db = ArangoClient(hosts="http://localhost:8529").db("_system_", username="root", password="")

adbrdf = ArangoRDF(db)

g = Graph()
g.parse("https://raw.githubusercontent.com/stardog-union/stardog-tutorials/master/music/beatles.ttl", format="ttl")
# g.parse(...)

# ADB to RDF (RPT & PGT)
adbrdf.rdf_to_arangodb_by_rpt("BeatlesRPT", g)
adbrdf.rdf_to_arangodb_by_pgt("BeatlesPGT", g)

# RDF to ADB (By Graph)
g1 = Graph(), g2
adbrdf.arangodb_graph_to_rdf("BeatlesRPT", Graph())
adbrdf.arangodb_graph_to_rdf("BeatlesPGT", Graph()) 

# RDF to ADB (By Collection Names)
adbrdf.arangodb_collections_to_rdf(
   "BeatlesPGT",
   Graph(),
   v_cols={"Album", "Band", "Class", "Property", "SoloArtist", "Song"},
   e_cols={"artist", "member", "track", "type", "writer"},
)

# RDF to ADB (By Metagraph)
# ...
```

> Do you break a api in use (also consider added json fields which were ignored before)

Yes, ArangoRDF's Overhaul is breaking. A new major (?) release would be required (current ArangoRDF version is 0.0.3)


# Implementation

This section presents high-level Objective/Solution content on the primary features introduced in ArangoRDF post-overhaul.

## <div align="center"> RDF-topology Preserving Transformation (RPT) </div>

**Objective**: Convert an RDF Graph into a Property Graph while maintaining the topological structure of the original RDF Graph.

**Solution:**

RPT preserves the RDF graph structure by transforming each RDF statement into an edge in the ArangoDB Graph. ArangoDB vertices are created out of the subject & object of each triple, resulting in a graph that is isomorphic to the original. 

This type of transformation is ideal for converting RDF Ontologies, or for when the RDF data is highly complex & heterogeneous.

The ArangoRDF RPT method will store the RDF Resources of the RDF Graph under the following ArangoDB Collections:

1. `{graph_name}_URIRef`: The Document collection for RDF URIRef resources.
2. `{graph_name}_BNode`: The Document collection for RDF BNode resources.
3. `{graph_name}_Literal`: The Document collection for RDF Literal resources.
4. `{graph_name}_Statement`: The Edge collection for all triples/quads.

Where `{graph_name}` is the ArangoDB Graph name appointed by the user.

It is possible to run ArangoRDF’s RPT implementation on the paper’s test cases by accessing the Simple RPT & PGT Examples section of the ArangoRDF Jupyter Notebook on Google Colab: https://colab.research.google.com/github/ArangoDB-Community/ArangoRDF/blob/arangordf-overhaul/examples/ArangoRDF.ipynb#scrollTo=cy_BWXK2AX5n. 

Time Complexity: `O(N)`, where `N` is the number of triples in the RDF Graph.

## <div align="center"> Property Graph Transformation (PGT) </div>

**Objective**: Convert an RDF Graph into a Property Graph while reducing the number of nodes & edges required to represent the contents of the original RDF Graph.

**Solution**:

PGT reduces the number of nodes & edges required to represent the original RDF Graph by converting any DataType Property Statements (i.e RDF statements whose object is a Literal) into Node Properties (instead of creating Edges for them).

This type of transformation is ideal for converting instance data, and also ideal when combined with tabular data. 

In contrast to the ArangoRDF RPT method, the ArangoRDF PGT method will rely on the nature of the RDF Resource/Statement to determine which ArangoDB Collection it belongs to. This is referred to as the **ArangoDB Document-to-Collection Mapping Process**, and is explained in section `4` (below).

It is possible to run ArangoRDF’s PGT implementation on the paper’s test cases by accessing the Simple RPT & PGT Examples section of the ArangoRDF Jupyter Notebook on  Google Colab: https://colab.research.google.com/github/ArangoDB-Community/ArangoRDF/blob/arangordf-overhaul/examples/ArangoRDF.ipynb#scrollTo=cy_BWXK2AX5n. 

Time Complexity: `O(N)`, where `N` is the number of triples in the RDF Graph.

## <div align="center"> Property Graph to RDF </div>

**Objective**: Convert a Property Graph into an RDF Graph, whether the original Property Graph is "natively" a Property Graph, or whether it originated from an RDF context (i.e RDF→PG→RDF).

**Solution**:

ArangoRDF’s Property Graph to RDF implementation has to keep in mind the origin of the ArangoDB Graph. There are 3 possible "types" of ArangoDB Graphs that can impact the way the transformation is handled:
1. The ArangoDB Graph is native to ArangoDB. As in, it is not previously of RDF origin
2. The ArangoDB Graph originates from RDF, and was converted via the RPT algorithm
3. The ArangoDB graph originates from RDF, and was converted via the PGT algorithm

In `Case 1`, there are no existing RDF Namespaces or RDF URIs to work with, as the graph data is not from RDF. This **implies that all soon-to-be RDF Resources will have the same namespace**, which is set to the user’s database endpoint, appended with the name of their ArangoDB Graph. Furthermore, due to RDFLib’s absence of support for RDF-star graphs, any ArangoDB Edge that contains Edge Properties will be transferred into a separate RDF Resource (of type RDF:Statement). This is a temporary solution which will be replaced as soon as RDF-star support is introduced into the RDFLib library.

In `Case 2`, the generated RDF Graph remains Isomorphic to the ArangoDB Graph, as the original data is already structured in "triple" format. Should an ArangoDB Edge have a "_sub_graph_uri" edge property associated to it, then an RDF Quad is appended to the RDF Graph, instead of an RDF Triple.

In `Case 3`, the possibility of having JSON Properties associated with the ArangoDB Vertices arises, so it is important to iterate through the key-value pairs of each ArangoDB Vertex & Edge and create RDF Triples/Quads out of the data. 

It is possible to run examples of ArangoRDF’s Property Graph to RDF implementation by accessing the ArangoDB to RDF section of the ArangoRDF Jupyter Notebook on Google Colab: https://colab.research.google.com/github/ArangoDB-Community/ArangoRDF/blob/arangordf-overhaul/examples/ArangoRDF.ipynb#scrollTo=UCQ9ppnUQa7e

Time Complexity: `O(V + E)`, where `V` is the number of vertices & `E` is the number of edges.

## <div align="center"> ArangoDB Document-To-Collection mapping process (PGT) </div>

**Objective**: The PGT algorithm dynamically creates ArangoDB Collections based on the nature of the RDF Resources found within the user’s RDF Graph. An algorithm must be derived in order to systematically identify the ideal ArangoDB Collection of every RDF Resource within the RDF Graph. 

**Solution**:

The ideal ArangoDB Collection `IC` (Ideal Class) for a given RDF Resource `R` is identified by examining the explicit & inferred `RDF:type` relationships of the resource `R`. These relationships are "collected" as part of a pre-processing stage in the PGT algorithm, such that there is a "type mapping" for every RDF Resource within the RDF Graph. In other words, `R` will map to a set of RDF Classes `CS` (Class Set), whereby the ideal class `IC` is chosen.

Once the ideal class `IC` has been selected, **the local name (i.e the label) of `IC`’s URI is used as the ArangoDB Document Collection name**.

The "ideal" RDF Class **`IC` is defined as an RDF Class that best represents the typed nature of `R`**, and whose local name can be used as the ArangoDB Document Collection name to store `R`.

The current "ideal class" identification process goes as follows:

1. If an RDF Resource only has one `rdf:type` statement (either by explicit definition or by domain/range inference), then **the local name of the single RDF Class is used as the ArangoDB Document Collection name**. For example, 
`<http://example.com/Bob> <rdf:type> <http://example.com/Person>`
would place the ArangoDB Document for `<http://example.com/Bob>` under the ArangoDB `Person` Document Collection.

2. If an RDF Resource has multiple `rdf:type` statements (either by explicit definition or by domain/range inference), with some (or all) of the RDF Classes of those statements belonging in an `rdfs:subClassOf` Taxonomy, then **the local name of the "most specific" Class within the Taxonomy is used (i.e the Class with the largest depth)**. If there is a  tie between 2+ Classes, then the URIs are alphabetically sorted & the first one is picked. 

3. If an RDF Resource has multiple `rdf:type` statements, with none of the RDF Classes of those statements belonging in an `rdfs:subClassOf` Taxonomy, then **the URIs are alphabetically sorted & the first one is picked**. The local name of the selected URI will be designated as the Document Collection for `R`.

4. If an RDF Resource is in an RDF Datatype Property Statement with the predicate `http://www.arangodb.com/collection`, then **the steps above are disregarded, and the RDF Resource is placed in the ArangoDB Collection represented by the RDF Statement's object**. For example, the statement `<http://example.com/Bob> <adb:collection> "Human"` would result in the ArangoDB Document `Bob` be placed in the `Human` cocument collection (regardless of any `rdf:type` relationships that `http://example.com/Bob` may already have).


The current identification process was inspired by how the **Common Lisp Object System** handles multiple inheritance. Given that it is still a work in progress, **users are able to override this process by defining their own identification algorithm**.

To demo the ArangoDB Collection Mapping process, let us consider the following RDF Graph:
```
@prefix ex: <http://example.com/> .
@prefix adb: <http://www.arangodb.com/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

ex:B rdfs:subClassOf ex:A .
ex:C rdfs:subClassOf ex:A .
ex:D rdfs:subClassOf ex:C .

ex:alex rdf:type ex:A .

ex:john rdf:type ex:B .
ex:john rdf:type ex:D .

ex:mike rdf:type ex:G .
ex:mike rdf:type ex:F .
ex:mike rdf:type ex:E .

ex:bob ex:name "Bob" .
ex:name rdfs:domain ex:A .

ex:charles rdf:type ex:A .
ex:charles rdf:type ex:C .
ex:charles adb:collection "Z" .
```

Given the RDF TTL Snippet above, we can derive the following ArangoDB Collection mappings:

* `ex:alex ("A")`: This RDF Resource only has one associated `rdf:type` statement.

* `ex:john ("D")`: This RDF Resource has 2 `rdf:type` statements, but `ex:D` is "deeper"  than `ex:B` when considering the `rdfs:subClassOf` Taxonomy.

* `ex:mike ("E")`: This RDF Resource has multiple `rdf:type` statements, with none belonging to the `rdfs:subClassOf` Taxonomy. Therefore, Alphabetical Sorting is used.

* `ex:bob ("A")`: Although this RDF Resource has no `rdf:type` statement associated with it, we can infer from the domain of the property it uses (ex:name) that it is of type ex:A. 

* `ex:charles ("Z")`: Although this RDF Resource has 2 `rdf:type` statements, the `adb:collection` statement will be used to designed `ex:charles` into collection "Z".

Time Complexity: `O(N)`, where `N` is the number of triples in the RDF Graph

## <div align="center"> RDF List to JSON List conversion process (PGT) </div>

**Objective**: The PGT algorithm transforms DataType Property Statements into Node properties. This should apply even to RDF Lists that contain Literals. The challenge is that RDF Lists are made of BNodes that connect the list structure together, making them difficult to unpack (imagine a Linked List structure but every "link" is presented separately). For example, the statement `ex:List1 ex:contents ("one" "two" "three") .` should translate into the ArangoDB JSON Document below:

```json
{
  "uri": "http://example.com/List1",
  "contents": ["one", "two", "three"] 
}
```

Although the statement `ex:List1 ex:contents ("one" "two" "three") .` may seem simple to convert into a JSON list, its real RDF representation looks like this:

```
ex:List1 ex:contents [
    rdf:first "one" ; rdf:rest [
        rdf:first "two" ; rdf:rest [
            rdf:first "three" ;
            rdf:rest rdf:nil
        ]
    ]
] .
```

In other words, `( "one" "two" "three" )` is just syntactic sugar.

What makes this more challenging is that iterating through the RDF Graph via RDFLib will not return the RDF List statements in an ordered way. In other words, iterating through the RDF List will look like this:

```
>>> for triple in g:
...   print(triple)
...
(rdflib.term.BNode('n005eb457e8804bfda5f391077e691cb4b2'), rdflib.term.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#first'), rdflib.term.Literal('two'))
(rdflib.term.BNode('n005eb457e8804bfda5f391077e691cb4b1'), rdflib.term.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#rest'), rdflib.term.BNode('n005eb457e8804bfda5f391077e691cb4b2'))
(rdflib.term.BNode('n005eb457e8804bfda5f391077e691cb4b3'), rdflib.term.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#rest'), rdflib.term.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#nil'))
(rdflib.term.URIRef('http://example.com/List1'), rdflib.term.URIRef('http://example.com/contents'), rdflib.term.BNode('n005eb457e8804bfda5f391077e691cb4b1'))
(rdflib.term.BNode('n005eb457e8804bfda5f391077e691cb4b3'), rdflib.term.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#first'), rdflib.term.Literal('three'))
(rdflib.term.BNode('n005eb457e8804bfda5f391077e691cb4b1'), rdflib.term.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#first'), rdflib.term.Literal('one'))
(rdflib.term.BNode('n005eb457e8804bfda5f391077e691cb4b2'), rdflib.term.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#rest'), rdflib.term.BNode('n005eb457e8804bfda5f391077e691cb4b3'))
```

In other words, RDF Lists must be "reconstructed" using recursive methods before being ingested into ArangoDB if we want to convert RDF Lists into JSON Lists (PGT only).

**Solution**

The solution is to build a simplified representation of any RDF Lists within the RDF Graph during the first PGT iteration, and then introduce a post-processing stage to recursively unpack their complicated structure & insert the list data into the associated ArangoDB Documents. 

For every triple `T (s,p,o)` within the RDF Graph, the simplified representation is collected during the PGT iteration by checking if:

1. The subject `s` of `T` is of type `BNode`, and the predicate `p` of `T` is either equal to `rdf:first`, `rdf:rest`, `rdf:_n` (where n is a positive integer), or `rdf:li` (see https://www.w3.org/TR/rdf-schema/#ch_containermembershipproperty). 

2. The object `o` of `T` is of type `BNode`, and there exists a statement in an RDF Graph of any of the following forms: `(o, rdf:first, _)`, `(o, rdf:rest, _)`, `(o, rdf:_n, _)` or `(o, rdf:li, _)`.

This simplified representation uses a combination of the Dictionary & Linked List data structures, allowing easy-access and efficient parsing.

Once a parsable & simplified representation of these RDF Lists have been constructed during the PGT iteration, the post-processing stage iterates through the simplified RDF List structures, and recursively unpacks the nested structures of those lists.

An RDF List can contain either Literals, BNodes, or URIRefs as list elements.
- If a list element is of type `BNode` or `URIRef`, an ArangoDB Edge is created out of the subject, predicate, and list element.

- If a list element is of type `Literal`, a form of string manipulation is used. For example, given the RDF Statement `ex:Doc ex:numbers (1 (2 3)) .`, the equivalent ArangoDB List is constructed via a string-based solution: `"[" → "[1" → "[1, [" → "[1, [2," → "[1, [2, 3" → "[1, [2, 3]" → "[1, [2, 3]]"`

Time Complexity: `O(N)`, where `N` is the number of triples in the RDF Graph

## <div align="center"> Graph Contextualization (RPT & PGT) </div>

**Objective**: When transforming an RDF Graph into a Property Graph, it should be possible to enhance the Terminology Box of the original RDF Graph such that the graph is fully “contextualized” within the ArangoDB Graph.

**Solution**

This is achieved by: 
1. Providing the user with an option to load the OWL, RDF, and RDFS Ontologies in their RDF Graph
2. Processing every RDF Predicate within the RDF Graph as its own ArangoDB Document
3. Providing RDFS Domain & Range Inference on all RDF Resources that have no existing RDF Type relationship
4. Providing RDFS Domain & Range Introspection on all RDF Predicates that have no existing RDFS Domain or RDFS Range statements

In order to provide Domain/Range Inference & Introspection, a pre-processing stage is implemented in both PGT & RPT Transformations to build:
1. A dictionary mapping the domain & ranges of every predicate
2. A dictionary mapping the “explicit” type relationships of every RDF Resource (i.e all RDF Resource that have statements of the form `resource rdf:type __ .`
3. A dictionary mapping the “inference” type relationships of every RDF Resource.   

A demo of Graph Contextualization can be accessed under the RDF to ArangoDB w/ Graph Contextualization section of the ArangoRDF Jupyter Notebook on Google Colab: https://colab.research.google.com/github/ArangoDB-Community/ArangoRDF/blob/arangordf-overhaul/examples/ArangoRDF.ipynb#scrollTo=P9oGi91RJbAI

Time Complexity: `O(N)`, where `N` is the number of triples in the RDF Graph

# Refactoring
> Does this feature require or benefit from refactoring of existing code?

ArangoRDF was originally designed by David Vidovich, an employee at Mission Solutions Group (a customer of ArangoDB). David no longer works at MSG, so most of the existing ArangoRDF code has been discarded.

# Upgrades
> Does the feature support rolling upgrades in a cluster and work with mixed versions of ArangoDB?

Yes, as long as the Python Driver is maintained.

# Operability

## Documentation
> What documentation changes are required?
- API Docstrings
- Jupyter Notebook
- ArangoDB Docs: https://www.arangodb.com/docs/stable/data-science-arango-rdf-adapter.html

## Metrics
> What metrics are important to users when using this feature?
- ArangoDB Ingestion Time
- Processing Time
- Memory consumed

# Testing
> How to test (manual/automatic)
- A series of unit tests have been developed for ArangoRDF. See `arangordf/tests/test_main.py`

> Which key elements to test
- Transformation algorithms
- Stress testing
- Domain/range inference & introspection
- Ontology support 

> What tests should be added to the performance tests suite and what thresholds should be considered a regression/issue?
- TBD

# Planning (TODO)
* Should be done at a super late stage in the life cycle of this document.  
Add a list of Epics/Feature Issues and link them*

# Done Checklist (TODO)
* Implementation in all relevant branches
* Tests including Performance Tests
* Documentation
* Changelog entry and short description in Release Notes (i.e., release-notes-new-featuresXX.md)
* Support by all required ArangoDB components
* Operability metrics and scripts

---
*=mandatory