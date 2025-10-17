import os
import pytest
import subprocess
from pathlib import Path
from typing import Any, Dict, Set, Tuple

from arango import ArangoClient, DefaultHTTPClient
from arango.database import StandardDatabase
from rdflib import BNode
from rdflib import ConjunctiveGraph as RDFConjunctiveGraph
from rdflib import Graph as RDFGraph
from rdflib import Literal, URIRef

from arango_rdf import ArangoRDF

con: Dict[str, Any]
db: StandardDatabase
adbrdf: ArangoRDF
PROJECT_DIR = Path(__file__).parent.parent


def pytest_addoption(parser: Any) -> None:
    parser.addoption("--url", action="store", default="http://localhost:8529")
    parser.addoption("--username", action="store", default="root")
    parser.addoption("--password", action="store", default="")
    parser.addoption("--dbName", action="store", default="_system")


def pytest_configure(config: Any) -> None:
    global con
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

    class NoTimeoutHTTPClient(DefaultHTTPClient):
        REQUEST_TIMEOUT = None

    global db
    db = ArangoClient(hosts=con["url"], http_client=NoTimeoutHTTPClient()).db(
        con["dbName"], con["username"], con["password"], verify=True
    )

    global adbrdf
    adbrdf = ArangoRDF(db)


@pytest.fixture(autouse=True)
def reset_arango_db():
    global db
    for g in db.graphs():
        db.delete_graph(g["name"], drop_collections=True)

    for c in db.collections():
        if c["system"] == False:
            db.delete_collection(c["name"])


def arango_restore(path_to_data: str) -> None:
    global con
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
                global db
                db.delete_graph(graph_name, drop_collections=True, ignore_missing=True)

    except AttributeError:
        print(node)
        print(dir(node))
        print("Could not delete graph")


def get_rdf_graph(path: str) -> RDFGraph:
    g = RDFConjunctiveGraph() if path.endswith(".trig") else RDFGraph()
    g.parse(f"{PROJECT_DIR}/tests/data/rdf/{path}")
    return g


def get_meta_graph() -> RDFConjunctiveGraph:
    g = RDFConjunctiveGraph()
    for ns in os.listdir(f"{PROJECT_DIR}/arango_rdf/meta"):
        g.parse(f"{PROJECT_DIR}/arango_rdf/meta/{ns}", format="trig")

    return g


def get_adb_graph_count(name: str) -> Tuple[int, int]:
    global db
    adb_graph = db.graph(name)

    e_cols = {col["edge_collection"] for col in adb_graph.edge_definitions()}

    v_count = 0
    for v in db.graph(name).vertex_collections():
        if v in e_cols:
            continue

        v_count += adb_graph.vertex_collection(v).count()

    e_count = 0
    for e_d in adb_graph.edge_definitions():
        e_count += adb_graph.edge_collection(e_d["edge_collection"]).count()

    return (v_count, e_count)


def subtract_graphs(rdf_graph_a: RDFGraph, rdf_graph_b: RDFGraph) -> RDFGraph:
    assert rdf_graph_a and rdf_graph_b
    return rdf_graph_a - rdf_graph_b


def get_uris(rdf_graph: RDFGraph, include_predicates: bool = False) -> Set[URIRef]:
    global adbrdf

    unique_uris = set()
    for s, p, o in rdf_graph.triples((None, None, None)):
        if isinstance(s, URIRef):
            unique_uris.add(s)

        if include_predicates and isinstance(p, URIRef):
            if p != adbrdf.adb_col_uri:
                unique_uris.add(p)

        if isinstance(o, URIRef):
            unique_uris.add(o)

    return unique_uris


def get_bnodes(rdf_graph: RDFGraph, include_predicates: bool = False) -> Set[BNode]:
    unique_bnodes = set()
    for s, p, o in rdf_graph.triples((None, None, None)):
        if isinstance(s, BNode):
            unique_bnodes.add(s)
        if include_predicates and isinstance(p, BNode):
            unique_bnodes.add(p)
        if isinstance(o, BNode):
            unique_bnodes.add(o)

    return unique_bnodes


def get_literals(rdf_graph: RDFGraph) -> Set[Literal]:
    literals = set()
    for _, _, o in rdf_graph.triples((None, None, None)):
        if isinstance(o, Literal):
            literals.add(o)

    return literals


def get_literal_statements(rdf_graph: RDFGraph) -> Set[Tuple[URIRef, URIRef, Literal]]:
    literal_statements = set()
    for s, p, o in rdf_graph.triples((None, None, None)):
        if isinstance(o, Literal):
            literal_statements.add((s, p, o))

    return literal_statements
