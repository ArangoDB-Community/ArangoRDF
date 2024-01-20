#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from abc import ABC
from typing import Any, Optional, Set, Tuple, Union

from arango.graph import Graph as ADBGraph
from rdflib import BNode
from rdflib import Graph as RDFGraph
from rdflib import URIRef

from .typings import ADBMetagraph
from .utils import Tree


class AbstractArangoRDF(ABC):
    def __init__(self) -> None:
        raise NotImplementedError  # pragma: no cover

    def rdf_to_arangodb_by_rpt(
        self,
        name: str,
        rdf_graph: RDFGraph,
        contextualize_graph: bool,
        flatten_reified_triples: bool,
        use_hashed_literals_as_keys: bool,
        overwrite_graph: bool,
        batch_size: Optional[int],
        **adb_import_kwargs: Any,
    ) -> ADBGraph:
        raise NotImplementedError  # pragma: no cover

    def rdf_to_arangodb_by_pgt(
        self,
        name: str,
        rdf_graph: RDFGraph,
        adb_col_statements: Optional[RDFGraph],
        write_adb_col_statements: bool,
        contextualize_graph: bool,
        flatten_reified_triples: bool,
        overwrite_graph: bool,
        batch_size: Optional[int],
        **adb_import_kwargs: Any,
    ) -> ADBGraph:
        raise NotImplementedError  # pragma: no cover

    def arangodb_to_rdf(
        self,
        name: str,
        rdf_graph: RDFGraph,
        metagraph: ADBMetagraph,
        explicit_metagraph: bool,
        list_conversion_mode: str,
        dict_conversion_mode: str,
        infer_type_from_adb_v_col: bool,
        include_adb_v_col_statements: bool,
        include_adb_v_key_statements: bool,
        include_adb_e_key_statements: bool,
        **adb_export_kwargs: Any,
    ) -> Tuple[RDFGraph, RDFGraph]:
        raise NotImplementedError  # pragma: no cover

    def arangodb_collections_to_rdf(
        self,
        name: str,
        rdf_graph: RDFGraph,
        v_cols: Set[str],
        e_cols: Set[str],
        list_conversion_mode: str,
        dict_conversion_mode: str,
        infer_type_from_adb_v_col: bool,
        include_adb_v_col_statements: bool,
        include_adb_v_key_statements: bool,
        include_adb_e_key_statements: bool,
        **adb_export_kwargs: Any,
    ) -> Tuple[RDFGraph, RDFGraph]:
        raise NotImplementedError  # pragma: no cover

    def arangodb_graph_to_rdf(
        self,
        name: str,
        rdf_graph: RDFGraph,
        list_conversion_mode: str,
        dict_conversion_mode: str,
        infer_type_from_adb_v_col: bool,
        include_adb_v_col_statements: bool,
        include_adb_v_key_statements: bool,
        include_adb_e_key_statements: bool,
        **adb_export_kwargs: Any,
    ) -> Tuple[RDFGraph, RDFGraph]:
        raise NotImplementedError  # pragma: no cover

    def __fetch_adb_docs(self) -> None:
        raise NotImplementedError  # pragma: no cover

    def __insert_adb_docs(self) -> None:
        raise NotImplementedError  # pragma: no cover


class AbstractArangoRDFController(ABC):
    def identify_best_class(
        self,
        rdf_resource: Union[URIRef, BNode],
        class_set: Set[str],
        subclass_tree: Tree,
    ) -> str:
        raise NotImplementedError  # pragma: no cover
