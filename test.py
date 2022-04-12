from arangoSemantics import ArangoSemantics


kg = ArangoSemantics("http://0.0.0.0:8529", "root", "password", "rdfTest", "arangoSemantic")

kg.init_rdf_collections(bnode="Blank")

kg.import_rdf("iao.owl")