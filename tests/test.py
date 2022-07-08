# from arango import ArangoClient
# from arango_rdf import ArangoRDF

# client = ArangoClient(hosts="http://localhost:8529")

# system = client.db("_system", username="root", password="")
# # if system.has_database("v2") is False:
# #     system.create_database("v2")

# # temp = client.db("v2")

# adb_rdf = ArangoRDF(system, graph="rdf_music")

# # config_latest = adb_rdf.get_config_by_latest()
# # print(config_latest)

# # config_timestamp = adb_rdf.get_config_by_timestamp(1657249683)
# # print(config_timestamp)

# # RDF Import
# # adb_rdf.init_rdf_collections(bnode="Blank")
# # adb_rdf.import_rdf("./examples/data/music_schema.ttl", format="ttl")
# # adb_rdf.import_rdf("./examples/data/beatles.ttl", format="ttl")

# # # RDF Export
# # adb_rdf.export(f"./examples/data/rdfExport.ttl", format="ttl")

# # # Re-import RDF Export
# # adb_rdf.import_rdf(f"./examples/data/rdfExport.ttl", format="ttl")

# # # Ontology Import
# # adb_rdf_2 = ArangoRDF(system, graph="ontology_iao")
# # adb_rdf_2.init_ontology_collections()
# # adb_rdf_2.import_ontology("./examples/data/iao.owl")

# print("done")
