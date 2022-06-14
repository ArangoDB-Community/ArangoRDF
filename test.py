from arangoSemantics import ArangoSemantics


kg = ArangoSemantics("http://0.0.0.0:8529", "root", "password", "import5", "arangoSemantic")

#rdf import
kg.init_rdf_collections(bnode="Blank")
kg.import_rdf("testExport3.owl", format="turtle")

#ontology import
# kg.init_ontology_collections()
#kg.import_ontology("iao.owl")

#export to file
# kg.export("testExport3.owl", "ttl")  
