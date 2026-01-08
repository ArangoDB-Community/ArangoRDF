RDF to ArangoDB (LPG)
---------------------
`Your RDF data as a classic Labeled-Property-Graph`

What is LPG?
============

**The Labeled Property Graph (LPG) model represents every RDF resource (subjects & objects)**
**as a vertex, every RDF predicate as an edge _label_, and every RDF literal value as a
vertex _property_.**

In ArangoRDF, the LPG transformation is implemented by the
:py:meth:`arango_rdf.main.ArangoRDF.rdf_to_arangodb_by_lpg` method.  Internally this
function re-uses the PGT transformation but forces a single vertex collection
(default ``Node``) and a single edge collection (default ``Edge``).  Afterwards you can
(optionally) migrate the ``rdf:type`` edges into an attribute on the vertices so that
vertex "labels" become first-class properties – a very common pattern for LPG systems.

Consider the following RDF graph:

.. code-block:: turtle

    @prefix ex: <http://example.com/> .
    @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

    ex:Alice a ex:Person ;
            ex:name  "Alice" ;
            ex:age   25 .

    ex:Bob   a ex:Person ;
            ex:name  "Bob" ;
            ex:age   30 .

    ex:Alice ex:friend ex:Bob .

    ex:Person rdfs:subClassOf ex:Human .

Running the LPG transformation produces a graph with:

* **2 vertices** in the ``Node`` collection (``ex:Alice`` & ``ex:Bob``)
* **3 edges** in the ``Edge`` collection:

  - ``(Alice) -[:friend]-> (Bob)``
  - ``(Alice) -[:type]->  (Person)``
  - ``(Bob)   -[:type]->  (Person)``

The type information is encoded as ordinary edges so that the conversion stays pure RDF
-> PG.  If you would rather keep node labels as attributes, run the migration helper
shown below.

Basic Usage
===========

.. code-block:: python

    from rdflib import Graph
    from arango import ArangoClient
    from arango_rdf import ArangoRDF

    rdf_graph = Graph()
    rdf_graph.parse("/path/to/data.ttl", format="turtle")

    db     = ArangoClient().db()
    adbrdf = ArangoRDF(db)

    # 1. Convert RDF → LPG
    adbrdf.rdf_to_arangodb_by_lpg(
        name="DemoGraph",          # name of the ArangoDB Graph
        rdf_graph=rdf_graph,        # your RDF data
        # resource_collection_name="Node",      # optional – defaults to "Node"
        # predicate_collection_name="Edge",     # optional – defaults to "Edge"
    )

    # 2. OPTIONAL – turn :type edges into the vertex attribute "_type"
    adbrdf.migrate_edges_to_attributes(
        graph_name="DemoGraph",
        edge_collection_name="Edge",
        attribute_name="_type",                   # the attribute to write
        filter_clause="e._label == 'type'"        # only copy rdf:type edges
    )

After the migration each vertex has an ``_type`` array property –
``["Person"]`` in this example – and the original ``rdf:type`` edges remain untouched.
Delete them if you do not need them any more.

In addition to the **edge_collection_name** parameter, it is possible to traverse the vertices of the 2nd Order edge collection to apply
the same attribute (but at the 2nd Order) to the original target verticies. In PGT, a common use case is to
set **edge_collection_name** to **"type"** and **second_order_edge_collection_name**
to **"subClassOf"** for inferring the **_type** attribute.

In LPG, this can be done with ``second_order_filter_clause``:

.. code-block:: python

    adbrdf.migrate_edges_to_attributes(
        graph_name="DemoGraph",
        edge_collection_name="Edge",
        attribute_name="_type",
        filter_clause="e._label == 'type'",
        second_order_edge_collection_name="Edge",
        second_order_filter_clause="e._label == 'subClassOf'"
        second_order_depth=10,
    )

After this migration, the ``_type`` attribute of ``ex:Alice`` and ``ex:Bob`` will be adjusted to ``["Person", "Human"]``.


LPG Collection Mapping Process
==============================

The **LPG Collection Mapping Process** is defined as the algorithm used to map
RDF Resources to ArangoDB Collections. In LPG, the mapping rules are intentionally simple:

1. **Vertex Collection** – All RDF resources (IRIs & blank nodes) are stored in the
   collection **``Node``** (customisable via ``resource_collection_name``).
2. **Edge Collection** – One single edge collection, **``Edge``** (customisable via
   ``predicate_collection_name``) holds every predicate edge.  The local part of the
   predicate IRI is written to the ``_label`` attribute.
3. **Class & Property meta-collections** – If the source RDF graph contains
   ``rdf:type`` declarations for Classes or Properties (e.g. ``ex:Person
   rdf:type rdfs:Class``), those resources are placed in the dedicated **``Class``**
   and **``Property``** vertex collections so that schema information stays
   discoverable.
4. **Literal values** – Objects that are RDF literals are stored as ordinary
   key/value pairs on the **subject vertex document**.

Because everything lives in two main collections, traversals and visualisations are
straight-forward and behave like a classic labeled property graph.


Further Reading
===============

* API Reference –
  :py:meth:`arango_rdf.main.ArangoRDF.rdf_to_arangodb_by_lpg`
* Migration helper –
  :py:meth:`arango_rdf.main.ArangoRDF.migrate_edges_to_attributes`
* Full list of public APIs – see the `specs <./specs.html>`_ page.
