ArangoDB to RDF
---------------

ArangoRDF provides three methods to export ArangoDB Graphs to RDF format:

1. ``arangodb_graph_to_rdf`` - Export by graph name (simplest)
2. ``arangodb_collections_to_rdf`` - Export by collection names
3. ``arangodb_to_rdf`` - Export with fine-grained control via metagraph

All three methods return an ``rdflib.Graph`` object containing the RDF representation
of your ArangoDB data.


Quick Start
===========

.. code-block:: python

   from rdflib import Graph
   from arango import ArangoClient
   from arango_rdf import ArangoRDF

   db = ArangoClient().db()
   adbrdf = ArangoRDF(db)

   # Export entire graph by name
   rdf_graph = adbrdf.arangodb_graph_to_rdf("MyGraph", rdf_graph=Graph())

   # Serialize to different formats
   print(rdf_graph.serialize(format="turtle"))
   rdf_graph.serialize("output.ttl", format="turtle")
   rdf_graph.serialize("output.xml", format="xml")


Export Methods
==============

1. Export by Graph Name
~~~~~~~~~~~~~~~~~~~~~~~

The simplest approach - exports all vertex and edge collections defined in the
ArangoDB graph:

.. code-block:: python

   rdf_graph = adbrdf.arangodb_graph_to_rdf(
       name="MyGraph",
       rdf_graph=Graph()
   )

2. Export by Collection Names
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Export specific vertex and edge collections:

.. code-block:: python

   rdf_graph = adbrdf.arangodb_collections_to_rdf(
       name="MyGraph",
       rdf_graph=Graph(),
       v_cols={"Person", "Company"},      # Vertex collections
       e_cols={"knows", "worksAt"},       # Edge collections
   )

3. Export with Metagraph
~~~~~~~~~~~~~~~~~~~~~~~~

Fine-grained control over which attributes to export from each collection:

.. code-block:: python

   rdf_graph = adbrdf.arangodb_to_rdf(
       name="MyGraph",
       rdf_graph=Graph(),
       metagraph={
           "vertexCollections": {
               "Person": {"name", "age", "email"},  # Only these attributes
               "Company": {"name", "founded"},
           },
           "edgeCollections": {
               "knows": {"since"},
               "worksAt": {"role", "startDate"},
           },
       },
       explicit_metagraph=True,  # Only include specified attributes
   )

Set ``explicit_metagraph=False`` to include all attributes while still filtering
collections:

.. code-block:: python

   rdf_graph = adbrdf.arangodb_to_rdf(
       name="MyGraph",
       rdf_graph=Graph(),
       metagraph={
           "vertexCollections": {
               "Person": set(),   # Empty set = all attributes
               "Company": set(),
           },
           "edgeCollections": {
               "knows": set(),
           },
       },
       explicit_metagraph=False,  # Include all attributes
   )


Conversion Options
==================

List Conversion Mode
~~~~~~~~~~~~~~~~~~~~

Controls how ArangoDB arrays are converted to RDF:

+---------------+------------------------------------------------------------------+
| Mode          | Description                                                      |
+===============+==================================================================+
| ``static``    | Each array element becomes a separate triple (default)           |
+---------------+------------------------------------------------------------------+
| ``collection``| Uses RDF Collection structure (``rdf:first``, ``rdf:rest``)      |
+---------------+------------------------------------------------------------------+
| ``container`` | Uses RDF Container structure (``rdf:_1``, ``rdf:_2``, etc.)      |
+---------------+------------------------------------------------------------------+
| ``serialize`` | Serializes array as JSON string literal (best for round-tripping)|
+---------------+------------------------------------------------------------------+

.. code-block:: python

   # Example: Array [1, 2, 3] with different modes

   # static (default): Creates 3 separate triples
   # :subject :predicate 1 .
   # :subject :predicate 2 .
   # :subject :predicate 3 .

   # collection: RDF Collection structure
   # :subject :predicate _:b1 .
   # _:b1 rdf:first 1 ; rdf:rest _:b2 .
   # _:b2 rdf:first 2 ; rdf:rest _:b3 .
   # _:b3 rdf:first 3 ; rdf:rest rdf:nil .

   # serialize: JSON string
   # :subject :predicate "[1, 2, 3]" .

   rdf_graph = adbrdf.arangodb_graph_to_rdf(
       "MyGraph",
       rdf_graph=Graph(),
       list_conversion_mode="collection",
   )

Dict Conversion Mode
~~~~~~~~~~~~~~~~~~~~

Controls how nested ArangoDB objects are converted to RDF:

+---------------+------------------------------------------------------------------+
| Mode          | Description                                                      |
+===============+==================================================================+
| ``static``    | Creates BNode with properties for each key (default)             |
+---------------+------------------------------------------------------------------+
| ``serialize`` | Serializes object as JSON string literal (best for round-tripping)|
+---------------+------------------------------------------------------------------+

.. code-block:: python

   # Example: {"city": "NYC", "zip": "10001"} with different modes

   # static (default): Creates BNode structure
   # :subject :address _:b1 .
   # _:b1 :city "NYC" ; :zip "10001" .

   # serialize: JSON string
   # :subject :address "{\"city\": \"NYC\", \"zip\": \"10001\"}" .

   rdf_graph = adbrdf.arangodb_graph_to_rdf(
       "MyGraph",
       rdf_graph=Graph(),
       dict_conversion_mode="serialize",
   )


Round-Tripping Support
======================

ArangoRDF supports round-tripping (ArangoDB → RDF → ArangoDB) with special options
to preserve ArangoDB-specific information.

Preserving Collection Names
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``include_adb_v_col_statements=True`` to generate ``adb:collection`` statements:

.. code-block:: python

   rdf_graph = adbrdf.arangodb_graph_to_rdf(
       "MyGraph",
       rdf_graph=Graph(),
       include_adb_v_col_statements=True,
   )

   # Generates statements like:
   # <http://...#doc123> <http://www.arangodb.com/collection> "Person" .

Preserving Document Keys
~~~~~~~~~~~~~~~~~~~~~~~~

Use ``include_adb_v_key_statements=True`` to preserve vertex document keys:

.. code-block:: python

   rdf_graph = adbrdf.arangodb_graph_to_rdf(
       "MyGraph",
       rdf_graph=Graph(),
       include_adb_v_key_statements=True,
   )

   # Generates statements like:
   # <http://...#doc123> <http://www.arangodb.com/key> "doc123" .

Preserving Edge Keys
~~~~~~~~~~~~~~~~~~~~

Use ``include_adb_e_key_statements=True`` to preserve edge document keys.
Note: This imposes triple reification on all edges.

.. code-block:: python

   rdf_graph = adbrdf.arangodb_graph_to_rdf(
       "MyGraph",
       rdf_graph=Graph(),
       include_adb_e_key_statements=True,
   )

Preserving Namespace Prefixes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Store namespace prefixes in an ArangoDB collection for round-trip reconstruction:

.. code-block:: python

   # When exporting from ArangoDB (after a previous RDF import with PGT)
   rdf_graph = adbrdf.arangodb_graph_to_rdf(
       "MyGraph",
       rdf_graph=Graph(),
       namespace_collection_name="Namespaces",  # Collection created during PGT import
   )


Inferring RDF Types
===================

Use ``infer_type_from_adb_v_col=True`` to generate ``rdf:type`` statements based
on the ArangoDB collection name:

.. code-block:: python

   rdf_graph = adbrdf.arangodb_graph_to_rdf(
       "MyGraph",
       rdf_graph=Graph(),
       infer_type_from_adb_v_col=True,
   )

   # A document in the "Person" collection generates:
   # <http://...#doc123> rdf:type <http://.../Person> .


Ignoring Attributes
===================

Exclude specific attributes from the export:

.. code-block:: python

   rdf_graph = adbrdf.arangodb_graph_to_rdf(
       "MyGraph",
       rdf_graph=Graph(),
       metagraph={
           "vertexCollections": {"Person": set()},
           "edgeCollections": {"knows": set()},
       },
       explicit_metagraph=False,
       ignored_attributes={"_internal_field", "created_at", "updated_at"},
   )

Note: ``ignored_attributes`` cannot be used when ``explicit_metagraph=True``.


Complete Example
================

.. code-block:: python

   from rdflib import Graph
   from arango import ArangoClient
   from arango_rdf import ArangoRDF

   # Connect to ArangoDB
   db = ArangoClient(hosts="http://localhost:8529").db(
       "_system", username="root", password=""
   )
   adbrdf = ArangoRDF(db)

   # Export with all options for round-tripping
   rdf_graph = adbrdf.arangodb_graph_to_rdf(
       name="MyGraph",
       rdf_graph=Graph(),
       list_conversion_mode="serialize",      # Best for round-tripping
       dict_conversion_mode="serialize",      # Best for round-tripping
       include_adb_v_col_statements=True,     # Preserve collection names
       include_adb_v_key_statements=True,     # Preserve vertex keys
   )

   # Print as Turtle
   print(rdf_graph.serialize(format="turtle"))

   # Save to file
   rdf_graph.serialize("export.ttl", format="turtle")

   # Get statistics
   print(f"Exported {len(rdf_graph)} triples")


Parameter Reference
===================

+--------------------------------+-------------------+------------------------------------------------+
| Parameter                      | Default           | Description                                    |
+================================+===================+================================================+
| ``name``                       | (required)        | ArangoDB graph name                            |
+--------------------------------+-------------------+------------------------------------------------+
| ``rdf_graph``                  | (required)        | Target rdflib Graph object                     |
+--------------------------------+-------------------+------------------------------------------------+
| ``list_conversion_mode``       | ``"static"``      | How to convert arrays                          |
+--------------------------------+-------------------+------------------------------------------------+
| ``dict_conversion_mode``       | ``"static"``      | How to convert nested objects                  |
+--------------------------------+-------------------+------------------------------------------------+
| ``infer_type_from_adb_v_col``  | ``False``         | Generate rdf:type from collection name         |
+--------------------------------+-------------------+------------------------------------------------+
| ``include_adb_v_col_statements``| ``False``        | Include adb:collection statements              |
+--------------------------------+-------------------+------------------------------------------------+
| ``include_adb_v_key_statements``| ``False``        | Include adb:key for vertices                   |
+--------------------------------+-------------------+------------------------------------------------+
| ``include_adb_e_key_statements``| ``False``        | Include adb:key for edges (uses reification)   |
+--------------------------------+-------------------+------------------------------------------------+
| ``namespace_collection_name``  | ``None``          | Collection storing namespace prefixes          |
+--------------------------------+-------------------+------------------------------------------------+
| ``ignored_attributes``         | ``None``          | Set of attributes to exclude                   |
+--------------------------------+-------------------+------------------------------------------------+

