from arangoSemantics import ArangoSemantics


kg = ArangoSemantics("http://localhost:8529", "root", "openSesame",   "ontologyTest", "arangoSemanticsRDF")

kg.init_rdf_collections(bnode="Blank")
kg.import_rdf("iao.owl")

kg2 = ArangoSemantics("http://localhost:8529", "root", "openSesame",   "ontologyTest", "arangoSemanticsOWL")
kg2.init_ontology_collections()
#kg2.import_ontology("test.owl", format="turtle")
kg2.import_ontology("iao.owl")
