Concurrent Imports
------------------

ArangoRDF supports concurrent imports using Python's ``concurrent.futures`` module,
allowing you to import multiple RDF graphs into the same ArangoDB graph in parallel.

This can significantly speed up imports when you have multiple RDF files to process.

Basic Usage
===========

.. code-block:: python

   from concurrent.futures import ThreadPoolExecutor, as_completed
   from rdflib import Graph, Namespace, Literal
   from arango import ArangoClient
   from arango_rdf import ArangoRDF

   db = ArangoClient().db()

   # Create multiple RDF graphs
   EX = Namespace("http://example.org/")

   g1 = Graph()
   g1.add((EX.Alice, EX.knows, EX.Bob))
   g1.add((EX.Alice, EX.name, Literal("Alice")))

   g2 = Graph()
   g2.add((EX.Bob, EX.knows, EX.Charlie))
   g2.add((EX.Bob, EX.name, Literal("Bob")))

   graphs = [g1, g2]

   def import_rdf(rdf_graph: Graph) -> None:
       # Each thread MUST create its own ArangoRDF instance
       adbrdf = ArangoRDF(db, enable_rich=False)
       adbrdf.rdf_to_arangodb_by_pgt(
           "MyGraph",
           rdf_graph,
           overwrite_graph=False,
           flatten_reified_triples=False,
           overwrite_mode="ignore",
           raise_on_document_error=False,
       )

   with ThreadPoolExecutor(max_workers=4) as executor:
       futures = [executor.submit(import_rdf, g) for g in graphs]
       for future in as_completed(futures):
           future.result()  # Raises exception if import failed


Requirements & Limitations
==========================

There are several important requirements and limitations to be aware of when
using concurrent imports:

1. Disable Rich Progress Bars (``enable_rich=False``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Rich progress bars interfere with concurrent/multiprocessing modules. Always
set ``enable_rich=False`` when creating ``ArangoRDF`` instances for concurrent use:

.. code-block:: python

   adbrdf = ArangoRDF(db, enable_rich=False)

2. Create Separate ArangoRDF Instances Per Thread
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each thread **must** create its own ``ArangoRDF`` instance. Do not share a single
instance across threads, as this will cause race conditions and errors.

.. code-block:: python

   # CORRECT: Create instance inside the thread function
   def import_rdf(rdf_graph):
       adbrdf = ArangoRDF(db, enable_rich=False)  # New instance per thread
       adbrdf.rdf_to_arangodb_by_pgt(...)

   # WRONG: Sharing instance across threads
   adbrdf = ArangoRDF(db, enable_rich=False)
   def import_rdf(rdf_graph):
       adbrdf.rdf_to_arangodb_by_pgt(...)  # Shared instance - will fail!

3. Disable Triple Reification (``flatten_reified_triples=False``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The triple reification flattening process uses SPARQL queries internally, which
rely on ``pyparsing`` - a library that is **not thread-safe**. When using concurrent
imports, you must disable this feature:

.. code-block:: python

   adbrdf.rdf_to_arangodb_by_pgt(
       ...,
       flatten_reified_triples=False,  # Required for thread safety
   )

If your RDF data contains reified triples and you need to flatten them, you must
process those graphs sequentially (not concurrently).

4. Handle Write-Write Conflicts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When multiple threads insert data into the same ArangoDB graph concurrently,
they may attempt to insert the same document (e.g., a shared predicate like
``ex:name``). This causes write-write conflicts.

To handle this gracefully, use:

.. code-block:: python

   adbrdf.rdf_to_arangodb_by_pgt(
       ...,
       overwrite_mode="ignore",          # Skip documents that already exist
       raise_on_document_error=False,    # Don't raise on conflicts
   )

This tells ArangoDB to silently skip duplicate documents rather than failing.

5. Don't Overwrite the Graph (``overwrite_graph=False``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When importing to the same graph from multiple threads, ensure you don't
overwrite the graph on each import:

.. code-block:: python

   adbrdf.rdf_to_arangodb_by_pgt(
       "MyGraph",
       rdf_graph,
       overwrite_graph=False,  # Append to existing graph
   )


Complete Example
================

Here's a complete example that follows all the requirements:

.. code-block:: python

   from concurrent.futures import ThreadPoolExecutor, as_completed
   from rdflib import Graph, Namespace, Literal
   from arango import ArangoClient
   from arango_rdf import ArangoRDF

   # Setup
   db = ArangoClient().db()
   graph_name = "ConcurrentImportExample"

   # Clean up any existing graph
   if db.has_graph(graph_name):
       db.delete_graph(graph_name, drop_collections=True)

   # Create sample RDF graphs
   EX = Namespace("http://example.org/")

   graphs = []
   for i in range(10):
       g = Graph()
       person = EX[f"Person{i}"]
       g.add((person, EX.name, Literal(f"Person {i}")))
       g.add((person, EX.knows, EX.Person0))
       graphs.append(g)

   def import_rdf(rdf_graph: Graph) -> str:
       """Import a single RDF graph - called from each thread."""
       # Create a new ArangoRDF instance for this thread
       adbrdf = ArangoRDF(db, enable_rich=False)
       
       adbrdf.rdf_to_arangodb_by_pgt(
           graph_name,
           rdf_graph,
           overwrite_graph=False,
           flatten_reified_triples=False,
           overwrite_mode="ignore",
           raise_on_document_error=False,
       )
       return "success"

   # Import all graphs concurrently
   results = []
   with ThreadPoolExecutor(max_workers=4) as executor:
       futures = [executor.submit(import_rdf, g) for g in graphs]
       for future in as_completed(futures):
           try:
               results.append(future.result())
           except Exception as e:
               print(f"Import failed: {e}")

   print(f"Successfully imported {len(results)} graphs")


Summary of Required Parameters
==============================

When using concurrent imports, always use these parameters:

+-------------------------------+-------------------+----------------------------------------+
| Parameter                     | Value             | Reason                                 |
+===============================+===================+========================================+
| ``enable_rich``               | ``False``         | Rich interferes with concurrency       |
+-------------------------------+-------------------+----------------------------------------+
| ``flatten_reified_triples``   | ``False``         | SPARQL parser is not thread-safe       |
+-------------------------------+-------------------+----------------------------------------+
| ``overwrite_graph``           | ``False``         | Append to graph, don't recreate        |
+-------------------------------+-------------------+----------------------------------------+
| ``overwrite_mode``            | ``"ignore"``      | Skip duplicate documents               |
+-------------------------------+-------------------+----------------------------------------+
| ``raise_on_document_error``   | ``False``         | Don't fail on write conflicts          |
+-------------------------------+-------------------+----------------------------------------+

