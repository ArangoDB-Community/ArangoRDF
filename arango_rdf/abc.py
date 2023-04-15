#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from abc import ABC
from typing import Any, Optional, Set, Tuple

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
        contextualize_graph: bool,
        overwrite_graph: bool,
        batch_size: Optional[int],
        **import_options: Any,
    ) -> ADBGraph:
        raise NotImplementedError  # pragma: no cover

    def rdf_to_arangodb_by_pgt(
        self,
        name: str,
        rdf_graph: RDFGraph,
        contextualize_graph: bool = False,
        overwrite_graph: bool = False,
        batch_size: Optional[int] = None,
        adb_mapping: Optional[RDFGraph] = None,
        **import_options: Any,
    ) -> ADBGraph:
        raise NotImplementedError  # pragma: no cover

    def arangodb_to_rdf(
        self,
        name: str,
        rdf_graph: RDFGraph,
        metagraph: ADBMetagraph,
        list_conversion_mode: str,
        **export_options: Any,
    ) -> Tuple[RDFGraph, RDFGraph]:
        raise NotImplementedError  # pragma: no cover

    def arangodb_collections_to_rdf(
        self,
        name: str,
        rdf_graph: RDFGraph,
        v_cols: Set[str],
        e_cols: Set[str],
        list_conversion_mode: str,
        **export_options: Any,
    ) -> Tuple[RDFGraph, RDFGraph]:
        raise NotImplementedError  # pragma: no cover

    def arangodb_graph_to_rdf(
        self,
        name: str,
        rdf_graph: RDFGraph,
        list_conversion_mode: str,
        **export_options: Any,
    ) -> Tuple[RDFGraph, RDFGraph]:
        raise NotImplementedError  # pragma: no cover

    def __fetch_adb_docs(self) -> None:
        raise NotImplementedError  # pragma: no cover

    def __insert_adb_docs(self) -> None:
        raise NotImplementedError  # pragma: no cover
