# NOTE: Temporarily Out of Service

# from arango_rdf import ArangoRDF
# from .conftest import PROJECT_DIR

# Test the loading of the RDFS/OWL/SKOS metamodel
# ToDo implement this an option on kg.init_rdf_collection
# ToDo use metamodel files for RDFS, OWL, and SKOS that can optionally be loaded
# kg = ArangoRDF(
#     "http://localhost:8529", "root", "openSesame", "ontologyTest", "arangoRDF"
# )
# kg.init_rdf_collections(bnode="Blank")
# kg.import_rdf(f"{PROJECT_DIR}/examples/data/rdfowl.ttl")
# kg.import_rdf(f"{PROJECT_DIR}/examples/data/skos.ttl")
# kg2 = ArangoRDF("http://localhost:8529", "root", "openSesame",   "ontologyTest", "arangoRDFOWL")
# kg2.init_ontology_collections()
# #kg2.import_ontology("test.owl", format="turtle")
# kg2.import_ontology("metamodel.ttl", format="turtle")
