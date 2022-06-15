from arangoSemantics import ArangoSemantics

# #rdf import
kg = ArangoSemantics("http://0.0.0.0:8529", "root", "password", "rdf", "arangoSemantic")
kg.init_rdf_collections(bnode="Blank")
kg.import_rdf("iao.owl", format="turtle")

# #ontology import
kg2 = ArangoSemantics("http://0.0.0.0:8529", "root", "password", "ontologyImport", "arangoSemantic")
kg2.init_ontology_collections()
kg2.import_ontology("iao.owl", format="turtle")

#export to file
kg3 = ArangoSemantics("http://0.0.0.0:8529", "root", "password", "rdf", "arangoSemantic")
kg3.export("rdfExport.owl", "ttl")  
