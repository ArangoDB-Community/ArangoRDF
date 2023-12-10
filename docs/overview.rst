Getting Started
---------------

Here is an example showing how the **arango-rdf** package can be used:

.. testcode::

    from rdflib import Graph
    from arango import ArangoClient
    from arango_rdf import ArangoRDF

    # Initialize the ArangoDB db client.
    db = ArangoClient(hosts='http://localhost:8529).db('_system', username='root', password='passwd')

    # Initialize ArangoRDF
    arangordf = ArangoRDF(db)

    # Create a new RDF graph.
    rdf_graph = Graph()
    rdf_graph.parse("https://raw.githubusercontent.com/stardog-union/stardog-tutorials/master/music/beatles.ttl")

    # RDF to ArangoDB
    adb_graph_rpt = adbrdf.rdf_to_arangodb_by_rpt("BeatlesRPT", rdf_graph, overwrite_graph=True)
    adb_graph_pgt = adbrdf.rdf_to_arangodb_by_pgt("BeatlesPGT", rdf_graph, overwrite_graph=True)

    # ArangoDB to RDF
    rdf_graph_2, adb_mapping = adbrdf.arangodb_graph_to_rdf("Beatles", Graph())
