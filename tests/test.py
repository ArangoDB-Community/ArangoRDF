from arango import ArangoClient
from arango_rdf import ArangoRDF

db = ArangoClient(hosts="http://localhost:8529").db("_system", username="root", password="")

adb_rdf = ArangoRDF(db, graph="rdf_music")

# RDF Import
adb_rdf.init_rdf_collections(bnode="Blank")
adb_rdf.import_rdf("./examples/data/music_schema.ttl", format="ttl")
adb_rdf.import_rdf("./examples/data/beatles.ttl", format="ttl")

# RDF Export
adb_rdf.export(f"./examples/data/rdfExport.ttl", format="ttl")

# Re-import RDF Export
adb_rdf.import_rdf(f"./examples/data/rdfExport.ttl", format="ttl")

# Ontology Import
adb_rdf_2 = ArangoRDF(db, graph="ontology_iao")
adb_rdf_2.init_ontology_collections()
adb_rdf_2.import_ontology("./examples/data/iao.owl")