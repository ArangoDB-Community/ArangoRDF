#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from abc import ABC
from typing import Any, Set

from arango.graph import Graph as ADBGraph
from rdflib import Graph as RDFGraph

from .typings import ADBMetagraph


class Abstract_ArangoRDF(ABC):
    def __init__(self) -> None:
        raise NotImplementedError  # pragma: no cover

    def rdf_to_arangodb_by_rpt(
        self,
        name: str,
        rdf_graph: RDFGraph,
        overwrite_graph: bool = False,
        load_base_ontology: bool = False,
        **import_options: Any,
    ) -> ADBGraph:
        raise NotImplementedError  # pragma: no cover

    def rdf_to_arangodb_by_pgt(
        self,
        name: str,
        rdf_graph: RDFGraph,
        overwrite_graph: bool = False,
        load_base_ontology: bool = False,
        **import_options: Any,
    ) -> ADBGraph:
        raise NotImplementedError  # pragma: no cover

    def arangodb_to_rdf(
        self,
        name: str,
        rdf_graph: RDFGraph,
        metagraph: ADBMetagraph,
        **query_options: Any,
    ) -> RDFGraph:
        raise NotImplementedError  # pragma: no cover

    def arangodb_collections_to_rdf(
        self,
        name: str,
        rdf_graph: RDFGraph,
        v_cols: Set[str],
        e_cols: Set[str],
        **query_options: Any,
    ) -> RDFGraph:
        raise NotImplementedError  # pragma: no cover

    def arangodb_graph_to_rdf(
        self, name: str, rdf_graph: RDFGraph, **query_options: Any
    ) -> RDFGraph:
        raise NotImplementedError  # pragma: no cover

    def __fetch_adb_docs(self) -> None:
        raise NotImplementedError  # pragma: no cover

    def __insert_adb_docs(self) -> None:
        raise NotImplementedError  # pragma: no cover

    @property
    def VALID_ADB_KEY_CHARS(self) -> Set[str]:
        return {
            "_",
            "-",
            ":",
            ".",
            "@",
            "(",
            ")",
            "+",
            ",",
            "=",
            ";",
            "$",
            "!",
            "*",
            "'",
            "%",
        }
