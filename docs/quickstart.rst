Quickstart
----------

.. raw:: html

   <a href="https://colab.research.google.com/github/ArangoDB-Community/ArangoRDF/blob/main/examples/ArangoRDF.ipynb" target="_parent"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/></a>

.. code-block:: python

   from rdflib import Graph
   from arango import ArangoClient
   from arango_rdf import ArangoRDF

   db = ArangoClient().db()

   adbrdf = ArangoRDF(db)

   def beatles():
      g = Graph()
      g.parse("https://raw.githubusercontent.com/ArangoDB-Community/ArangoRDF/main/tests/data/rdf/beatles.ttl", format="ttl")
      return g

**ArangoDB to RDF**

.. code-block:: python

   # 1. RDF-Topology Preserving Transformation (RPT)
   adbrdf.rdf_to_arangodb_by_rpt(name="BeatlesRPT", rdf_graph=beatles(), overwrite_graph=True)

   # 2. Property Graph Transformation (PGT) 
   adbrdf.rdf_to_arangodb_by_pgt(name="BeatlesPGT", rdf_graph=beatles(), overwrite_graph=True)

**RDF to ArangoDB**

.. code-block:: python

   # 1. Graph to RDF
   rdf_graph = adbrdf.arangodb_graph_to_rdf(name="BeatlesRPT", rdf_graph=Graph())

   # 2. Collections to RDF
   rdf_graph_2 = adbrdf.arangodb_collections_to_rdf(
      name="BeatlesRPT",
      rdf_graph=Graph(),
      v_cols={"BeatlesRPT_URIRef", "BeatlesRPT_Literal", "BeatlesRPT_BNode"},
      e_cols={"BeatlesRPT_Statement"}
   )

   # 3. Metagraph to RDF
   rdf_graph_3 = adbrdf.arangodb_to_rdf(
      name="BeatlesPGT",
      rdf_graph=Graph(),
      metagraph={
         "vertexCollections": {
               "Album": {"name", "date"},
               "SoloArtist": {}
         },
         "edgeCollections": {
               "artist": {}
         }
      }
   )