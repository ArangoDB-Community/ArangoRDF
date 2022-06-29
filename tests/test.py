from arango_rdf import ArangoRDF
from tests import PROJECT_DIR

# rdf import
kg = ArangoRDF(
    "http://0.0.0.0:8529",
    username="root",
    password="",
    database="rdf",
    graph="rdf_music",
)
kg.init_rdf_collections(bnode="Blank")
kg.import_rdf(f"{PROJECT_DIR}/examples/data/music_schema.ttl", format="ttl")
kg.import_rdf(f"{PROJECT_DIR}/examples/data/beatles.ttl", format="ttl")
kg.import_rdf(f"{PROJECT_DIR}/examples/data/iao.owl")

# ontology import
kg2 = ArangoRDF("http://0.0.0.0:8529", "root", "", "ontologyImport", "ontology_iao")
kg2.init_ontology_collections()
kg2.import_ontology(f"{PROJECT_DIR}/examples/data/iao.owl")

# export to file
kg.export(f"{PROJECT_DIR}/examples/data/rdfExport.ttl", format="ttl")

# Re-import Export
kg.import_rdf(f"{PROJECT_DIR}/examples/data/rdfExport.ttl", format="ttl")
