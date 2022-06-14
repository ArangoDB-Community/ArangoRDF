from arangoSemantics import ArangoSemantics
# Test the loading of the RDFS/OWL/SKOS metamodel
# ToDo implement this an option on kg.init_rdf_collection
# ToDo use metamodel files for RDFS, OWL, and SKOS that can optionally be loaded
kg = ArangoSemantics("http://localhost:8529", "root", "openSesame",   "ontologyTest", "arangoSemanticsRDF")
kg.init_rdf_collections(bnode="Blank")
kg.import_rdf("./metadata/rdfowl.ttl")
kg.import_rdf("./metadata/skos.ttl")
# kg2 = ArangoSemantics("http://localhost:8529", "root", "openSesame",   "ontologyTest", "arangoSemanticsOWL")
# kg2.init_ontology_collections()
# #kg2.import_ontology("test.owl", format="turtle")
# kg2.import_ontology("metamodel.ttl", format="turtle")