import logging
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, Tuple

from arango import ArangoClient, DefaultHTTPClient
from arango.database import StandardDatabase
from rdflib import ConjunctiveGraph as RDFConjunctiveGraph
from rdflib import Graph as RDFGraph

from arango_rdf import ArangoRDF

db: StandardDatabase
PROJECT_DIR = Path(__file__).parent.parent
adbrdf: ArangoRDF

META_GRAPH_SIZE = 663
# META_GRAPH_NON_LITERAL_STATEMENTS = 450
META_GRAPH_NON_LITERAL_STATEMENTS = 435
# META_GRAPH_CONTEXTUALIZE_STATEMENTS = 22
META_GRAPH_CONTEXTUALIZE_STATEMENTS = 7
# META_GRAPH_POST_CONTEXTUALIZE_SIZE = 685
META_GRAPH_POST_CONTEXTUALIZE_SIZE = 670
META_GRAPH_ALL_RESOURCES = 132
META_GRAPH_UNKNOWN_RESOURCES = 11
META_GRAPH_IDENTIFIED_RESOURCES = 121


def pytest_addoption(parser: Any) -> None:
    parser.addoption("--url", action="store", default="http://localhost:8529")
    parser.addoption("--dbName", action="store", default="_system")
    parser.addoption("--username", action="store", default="root")
    parser.addoption("--password", action="store", default="")


def pytest_configure(config: Any) -> None:
    con = {
        "url": config.getoption("url"),
        "username": config.getoption("username"),
        "password": config.getoption("password"),
        "dbName": config.getoption("dbName"),
    }

    print("----------------------------------------")
    print("URL: " + con["url"])
    print("Username: " + con["username"])
    print("Password: " + con["password"])
    print("Database: " + con["dbName"])
    print("----------------------------------------")

    class NoTimeoutHTTPClient(DefaultHTTPClient):  # type: ignore
        REQUEST_TIMEOUT = None

    global db
    db = ArangoClient(hosts=con["url"], http_client=NoTimeoutHTTPClient()).db(
        con["dbName"], con["username"], con["password"], verify=True
    )

    global adbrdf
    adbrdf = ArangoRDF(db, logging_lvl=logging.DEBUG)

    if db.has_graph("fraud-detection") is False:
        arango_restore(con, "tests/data/adb/fraud_dump")
        db.delete_collection("Class")
        db.delete_collection("Relationship")
        db.create_graph(
            "fraud-detection",
            edge_definitions=[
                {
                    "edge_collection": "accountHolder",
                    "from_vertex_collections": ["customer"],
                    "to_vertex_collections": ["account"],
                },
                {
                    "edge_collection": "transaction",
                    "from_vertex_collections": ["account"],
                    "to_vertex_collections": ["account"],
                },
            ],
            orphan_collections=["bank", "branch"],
        )

    if db.has_graph("imdb") is False:
        arango_restore(con, "tests/data/adb/imdb_dump")
        db.create_graph(
            "imdb",
            edge_definitions=[
                {
                    "edge_collection": "Ratings",
                    "from_vertex_collections": ["Users"],
                    "to_vertex_collections": ["Movies"],
                },
            ],
        )


def arango_restore(con: Dict[str, Any], path_to_data: str) -> None:
    restore_prefix = "./tools/" if os.getenv("GITHUB_ACTIONS") else ""
    protocol = "http+ssl://" if "https://" in con["url"] else "tcp://"
    url = protocol + con["url"].partition("://")[-1]

    subprocess.check_call(
        f'chmod -R 755 ./tools/arangorestore && {restore_prefix}arangorestore \
            -c none --server.endpoint {url} --server.database {con["dbName"]} \
                --server.username {con["username"]} \
                    --server.password "{con["password"]}" \
                        --input-directory "{PROJECT_DIR}/{path_to_data}"',
        cwd=f"{PROJECT_DIR}/tests",
        shell=True,
    )


def pytest_exception_interact(node: Any, call: Any, report: Any) -> None:
    try:
        if report.failed:
            params: Dict[str, Any] = node.callspec.params

            graph_name = params.get("name")
            if graph_name:
                db.delete_graph(graph_name, drop_collections=True, ignore_missing=True)

    except AttributeError:
        print(node)
        print(dir(node))
        print("Could not delete graph")


def get_rdf_graph(path: str, use_dataset_class: bool = False) -> RDFGraph:
    g = RDFConjunctiveGraph() if path.endswith(".trig") else RDFGraph()
    g.parse(f"{PROJECT_DIR}/tests/data/rdf/{path}")
    return g


def get_meta_graph() -> RDFConjunctiveGraph:
    g = RDFConjunctiveGraph()
    for ns in os.listdir(f"{PROJECT_DIR}/arango_rdf/meta"):
        g.parse(f"{PROJECT_DIR}/arango_rdf/meta/{ns}", format="trig")

    return g


def get_adb_graph_count(name: str) -> Tuple[int, int]:
    adb_graph = db.graph(name)

    v_count = 0
    for v in db.graph(name).vertex_collections():
        v_count += adb_graph.vertex_collection(v).count()

    e_count = 0
    for e_d in adb_graph.edge_definitions():
        e_count += adb_graph.edge_collection(e_d["edge_collection"]).count()

    return (v_count, e_count)


def outersect_graphs(rdf_graph_a: RDFGraph, rdf_graph_b: RDFGraph):
    assert rdf_graph_a and rdf_graph_b
    return rdf_graph_a - rdf_graph_b
