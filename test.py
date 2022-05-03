from arangoSemantics import ArangoSemantics


kg = ArangoSemantics("http://0.0.0.0:8529", "root", "password", "ontologyTest", "arangoSemantic")

# kg.init_rdf_collections(bnode="Blank")
# kg.import_rdf("iao.owl")

kg.init_ontology_collections()

#kg.import_ontology("test.owl", format="turtle")
kg.import_ontology("iao.owl")