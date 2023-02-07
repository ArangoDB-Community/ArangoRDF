from pathlib import Path
from typing import Any, Dict
import subprocess
import os
import logging

from arango_rdf import ArangoRDF
from rdflib import Graph, Dataset
from arango import ArangoClient, DefaultHTTPClient
from arango.database import StandardDatabase

db: StandardDatabase
PROJECT_DIR = Path(__file__).parent.parent


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

    # db.delete_graph("imdb", drop_collections=True, ignore_missing=True)
    # arango_restore(con, "tests/data/adb/imdb_dump")
    # db.create_graph(
    #     "imdb",
    #     edge_definitions=[
    #         {
    #             "edge_collection": "Ratings",
    #             "from_vertex_collections": ["Users"],
    #             "to_vertex_collections": ["Movies"],
    #         },
    #     ],
    # )


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


def get_rdf_graph(path: str):
    g = Dataset() if path.endswith(".trig") else Graph()
    g.parse(f"{PROJECT_DIR}/tests/data/rdf/{path}")
    return g
