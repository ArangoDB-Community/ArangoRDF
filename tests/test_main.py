from time import time

import pytest

from arango_rdf import ArangoRDF

from .conftest import PROJECT_DIR, db


def test_constructor() -> None:
    ArangoRDF(db, default_graph="temp_graph")  # create the graph
    assert db.has_graph("temp_graph")
    ArangoRDF(db, default_graph="temp_graph")  # re-use the graph


def test_full_cycle_aircraft_without_normalize_literals() -> None:
    graph_name = "default_graph"
    ################################# Import #################################
    # Clean up existing data and collections
    db.delete_graph(graph_name, drop_collections=True, ignore_missing=True)

    # Initializes default_graph and sets RDF graph identifier (ArangoDB sub_graph)
    # Optional: sub_graph (stores graph name as the 'graph' attribute on all edges in Statement collection)
    # Optional: default_graph (name of ArangoDB Named Graph, defaults to 'default_graph',
    #           is root graph that contains all collections/relations)
    adb_rdf = ArangoRDF(
        db, default_graph=graph_name, sub_graph="http://data.sfgov.org/ontology"
    )
    assert db.has_graph(graph_name)
    print("initialized graph")
    config = {"normalize_literals": False}  # default: False

    adb_rdf.init_rdf_collections(bnode="Blank")
    assert db.has_collection("Blank")
    assert db.has_collection("IRI")
    assert db.has_collection("Literal")
    assert db.has_collection("Statement")
    print("initialized collections")

    print("importing ontology...")

    timestamp = round(time())
    adb_rdf.import_rdf(
        f"{PROJECT_DIR}/examples/data/airport-ontology.owl",
        format="xml",
        config=config,
        save_config=True,
    )
    print("Ontology imported")

    assert db.collection("Blank").count() == 59
    assert db.collection("IRI").count() == 84
    assert db.collection("Literal").count() == 148
    assert db.collection("Statement").count() == 476

    assert db.has_collection("configurations")
    assert adb_rdf.get_config_by_latest()["timestamp"] >= timestamp

    print("importing aircraft data...")
    # Next, let's import the actual graph data
    adb_rdf.import_rdf(
        f"{PROJECT_DIR}/examples/data/sfo-aircraft-partial.ttl",
        format="ttl",
        config=config,
        save_config=False,
    )

    assert db.collection("Blank").count() == 59
    assert db.collection("IRI").count() == 90
    assert db.collection("Literal").count() == 306
    assert db.collection("Statement").count() == 648

    print("aircraft data imported")

    ################################# Export #################################
    print("exporting data...")
    adb_rdf.export(f"{PROJECT_DIR}/examples/data/rdfExport.xml", format="xml")
    print("export complete")

    ################################# Re-import ##############################
    db.delete_graph(graph_name, drop_collections=True, ignore_missing=True)

    # Re-initialize our RDF Graph
    # Initializes default_graph and sets RDF graph identifier (ArangoDB sub_graph)
    adb_rdf = ArangoRDF(db, sub_graph="http://data.sfgov.org/ontology")
    print("re-initialized graph")

    adb_rdf.init_rdf_collections(bnode="Blank")
    print("re-initialized collections")

    config = adb_rdf.get_config_by_latest()  # gets the last config saved
    # config = adb_rdf.get_config_by_key_value('graph', 'music')
    # config = adb_rdf.get_config_by_key_value('AnyKeySuppliedInConfig', 'SomeValue')

    # Re-import Exported data
    print("re-importing data...")
    adb_rdf.import_rdf(f"./examples/data/rdfExport.xml", format="xml", config=config)

    assert db.collection("Blank").count() == 59
    assert db.collection("IRI").count() == 90
    assert db.collection("Literal").count() == 306
    assert db.collection("Statement").count() == 648

    print("done")


def test_full_cycle_aircraft_with_normalize_literals() -> None:
    graph_name = "default_graph"
    ################################# Import #################################
    db.delete_graph(graph_name, drop_collections=True, ignore_missing=True)

    adb_rdf = ArangoRDF(
        db, default_graph=graph_name, sub_graph="http://data.sfgov.org/ontology"
    )
    config = {"normalize_literals": True}

    adb_rdf.init_rdf_collections(bnode="Blank")
    print("initialized collections")

    print("importing ontology...")
    adb_rdf.import_rdf(
        f"{PROJECT_DIR}/examples/data/airport-ontology.owl",
        format="xml",
        config=config,
        save_config=True,
    )

    assert db.collection("Blank").count() == 59
    assert db.collection("IRI").count() == 84
    assert db.collection("Literal").count() == 78
    assert db.collection("Statement").count() == 427

    print("importing aircraft data...")
    adb_rdf.import_rdf(
        f"{PROJECT_DIR}/examples/data/sfo-aircraft-partial.ttl",
        format="ttl",
        config=config,
        save_config=False,
    )

    assert db.collection("Blank").count() == 59
    assert db.collection("IRI").count() == 90
    assert db.collection("Literal").count() == 87
    assert db.collection("Statement").count() == 450

    print("aircraft data imported")

    ################################# Export #################################
    print("exporting data...")
    adb_rdf.export(f"{PROJECT_DIR}/examples/data/rdfExport.xml", format="xml")
    print("export complete")

    ################################# Re-import ##############################
    db.delete_graph(graph_name, drop_collections=True, ignore_missing=True)

    # Re-initialize our RDF Graph
    # Initializes default_graph and sets RDF graph identifier (ArangoDB sub_graph)
    adb_rdf = ArangoRDF(db, sub_graph="http://data.sfgov.org/ontology")
    print("re-initialized graph")

    adb_rdf.init_rdf_collections(bnode="Blank")
    print("re-initialized collections")

    config = adb_rdf.get_config_by_latest()  # gets the last config saved
    # config = adb_rdf.get_config_by_key_value('graph', 'music')
    # config = adb_rdf.get_config_by_key_value('AnyKeySuppliedInConfig', 'SomeValue')

    # Re-import Exported data
    print("re-importing data...")
    adb_rdf.import_rdf(f"./examples/data/rdfExport.xml", format="xml", config=config)

    assert db.collection("Blank").count() == 59
    assert db.collection("IRI").count() == 90
    assert db.collection("Literal").count() == 87
    assert db.collection("Statement").count() == 450

    print("done")


def test_get_config() -> None:
    adb_rdf = ArangoRDF(db, sub_graph="http://data.sfgov.org/ontology")
    config_1 = adb_rdf.get_config_by_latest()
    assert config_1["latest"] == True
    assert "_id" not in config_1
    assert "_key" not in config_1
    assert "_rev" not in config_1

    config_2 = adb_rdf.get_config_by_key_value("timestamp", config_1["timestamp"])
    assert config_1 == config_2

    config_3 = adb_rdf.get_config_by_key_value("normalize_literals", False)
    assert config_3["normalize_literals"] == False
