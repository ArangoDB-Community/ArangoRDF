from arango import ArangoClient
from arango_rdf import ArangoRDF

db = ArangoClient(hosts="http://localhost:8529").db(
    "rdf", username="root", password="openSesame"
)

# Clean up existing data and collections
if db.has_graph("default_graph"):
    db.delete_graph("default_graph", drop_collections=True, ignore_missing=True)

# Initializes default_graph and sets RDF graph identifier (ArangoDB sub_graph)
# Optional: sub_graph (stores graph name as the 'graph' attribute on all edges in Statement collection)
# Optional: default_graph (name of ArangoDB Named Graph, defaults to 'default_graph',
#           is root graph that contains all collections/relations)
adb_rdf = ArangoRDF(db, sub_graph="http://data.sfgov.org/ontology") 
print("initialized graph")
config = {"normalize_literals": False}  # default: False

# RDF Import
adb_rdf.init_rdf_collections(bnode="Blank")
print("initialized collections")

print("importing ontology...")
# Start with importing the ontology
adb_rdf.import_rdf("./examples/data/airport-ontology.owl", format="xml", config=config)
print("Ontology imported")

print("importing aircraft data...")
# Next, let's import the actual graph data
adb_rdf.import_rdf(f"./examples/data/sfo-aircraft-partial.ttl", format="ttl", config=config)

print("aircraft data imported")

# RDF Export
# WARNING:
# Exports ALL collections of the database,
# currently does not account for default_graph or sub_graph
# Results may vary, minifying may occur
print("exporting data...")
adb_rdf.export(f"./examples/data/rdfExport.xml", format="xml")
print("export complete")

# Drop graph and ALL documents and collections to test import from exported data
if db.has_graph("default_graph"):
    db.delete_graph("default_graph", drop_collections=True, ignore_missing=True)

# Re-initialize our RDF Graph
# Initializes default_graph and sets RDF graph identifier (ArangoDB sub_graph)
adb_rdf = ArangoRDF(db, sub_graph="http://data.sfgov.org/ontology")
print("re-initialized graph")

adb_rdf.init_rdf_collections(bnode="Blank")
print("re-initialized collections")

config = adb_rdf.get_config_by_latest() # gets the last config saved
# config = adb_rdf.get_config_by_key_value('graph', 'music')
# config = adb_rdf.get_config_by_key_value('AnyKeySuppliedInConfig', 'SomeValue')

# Re-import Exported data
print("re-importing data...")
adb_rdf.import_rdf(f"./examples/data/rdfExport.xml", format="xml", config=config)

print("done")
