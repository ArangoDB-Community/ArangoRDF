#!/usr/bin/env python3
import json
import logging
import os
import re
from ast import literal_eval
from collections import defaultdict
from datetime import date, time
from pathlib import Path
from typing import Any, Callable, DefaultDict, Dict, List, Optional, Set, Tuple, Union

import farmhash
from arango.collection import StandardCollection
from arango.cursor import Cursor
from arango.database import AsyncDatabase, StandardDatabase
from arango.graph import Graph as ADBGraph
from isodate import Duration
from rdflib import RDF, RDFS, XSD, BNode
from rdflib import ConjunctiveGraph as RDFConjunctiveGraph
from rdflib import Dataset as RDFDataset
from rdflib import Graph as RDFGraph
from rdflib import Literal, URIRef
from rich.console import Group
from rich.live import Live
from rich.progress import Progress

from .abc import AbstractArangoRDF
from .controller import ArangoRDFController
from .exception import ArangoRDFImportException
from .typings import (
    ADBDocs,
    ADBMetagraph,
    Json,
    PredicateScope,
    RDFListData,
    RDFListHeads,
    RDFTerm,
    RDFTermMeta,
    TypeMap,
)
from .utils import (
    Node,
    NoOpLive,
    NoOpProgress,
    Tree,
    empty_func,
    get_bar_progress,
    get_import_spinner_progress,
    get_spinner_progress,
    logger,
)

PROJECT_DIR = Path(__file__).parent


class ArangoRDF(AbstractArangoRDF):
    """ArangoRDF: Transform RDF Graphs into
    ArangoDB Graphs & vice-versa.

    Implemented using concepts referred in
    https://arxiv.org/abs/2210.05781.

    :param db: A python-arango database instance
    :type db: arango.database.Database
    :param logging_lvl: Defaults to logging.INFO. Other useful options are
        logging.DEBUG (more verbose), and logging.WARNING (less verbose).
    :type logging_lvl: str | int
    :param rdf_attribute_prefix: The prefix for RDF attributes (e.g., _uri, _value,
        _rdftype, etc.). Defaults to the original "_" symbol, but please NOTE
        that using an underscore "_", results in these attributes being treated
        as ArangoDB system attributes. Using "$" is an alternative non-system prefix.
    :type rdf_attribute_prefix: str
    :param insert_async: If True, will insert documents asynchronously.
        Defaults to False.
    :type insert_async: bool
    :param enable_pgt_cache: If True, will enable the PGT term metadata cache to avoid
        repeated computations. Defaults to False. Not always useful, especially when
        terms are not repeated alot in the RDF graph.
    :type enable_pgt_cache: bool
    :param enable_rich: If True, will enable rich progress bars and spinners.
        Defaults to True. Set to False when using multiprocessing or concurrent
        modules, as rich can interfere with them.
    :type enable_rich: bool
    :raise TypeError: On invalid parameter types
    """

    def __init__(
        self,
        db: StandardDatabase,
        controller: ArangoRDFController = ArangoRDFController(),
        logging_lvl: Union[str, int] = logging.INFO,
        rdf_attribute_prefix: str = "_",
        insert_async: bool = False,
        enable_pgt_cache: bool = False,
        enable_rich: bool = True,
    ):
        self.set_logging(logging_lvl)

        if not isinstance(db, StandardDatabase):
            msg = "**db** parameter must inherit from arango.database.StandardDatabase"
            raise TypeError(msg)

        if not isinstance(controller, ArangoRDFController):
            msg = "**controller** parameter must inherit from ArangoRDFController"
            raise TypeError(msg)

        self.db: StandardDatabase = db
        self.async_db: AsyncDatabase = db.begin_async_execution(return_result=False)
        self.insert_async = insert_async

        self.controller: ArangoRDFController = controller
        self.controller.db = db
        self.controller.async_db = self.async_db

        # Set the RDF attribute prefix
        self.__rdf_attribute_prefix = rdf_attribute_prefix

        # RDF attribute names using the configurable prefix
        self.__rdf_uri_attr = f"{rdf_attribute_prefix}uri"
        self.__rdf_value_attr = f"{rdf_attribute_prefix}value"
        self.__rdf_type_attr = f"{rdf_attribute_prefix}rdftype"
        self.__rdf_label_attr = f"{rdf_attribute_prefix}label"
        self.__rdf_sub_graph_uri_attr = f"{rdf_attribute_prefix}sub_graph_uri"
        self.__rdf_lang_attr = f"{rdf_attribute_prefix}lang"
        self.__rdf_datatype_attr = f"{rdf_attribute_prefix}datatype"

        # Pre-compile regex patterns for container predicates
        rdf_ns = str(RDF)
        self.__container_pattern_n = re.compile(f"^{re.escape(rdf_ns)}_[0-9]+$")
        self.__container_pattern_li = re.compile(f"^{re.escape(rdf_ns)}li$")

        # Work-in-progress feature to enhance the Terminology Box of an RDF Graph
        # when importing to ArangoDB.
        self.__contextualize_graph = False

        # Represents the ArangoDB Graph Edge Definitions
        self.__e_col_map: DefaultDict[str, DefaultDict[str, Set[str]]]

        # An RDF predicate used to identify
        # the ArangoDB Collection of an arbitrary RDF Resource.
        # e.g (<http://example.com/Bob> <http://www.arangodb.com/collection> "Person")
        self.adb_col_uri = URIRef("http://www.arangodb.com/collection")

        # An RDF predicate used to identify
        # the ArangoDB Key of an arbitrary RDF Resource.
        # e.g (<http://example.com/Bob> <http://www.arangodb.com/key> "4502")
        self.adb_key_uri = URIRef("http://www.arangodb.com/key")

        # Cache for PGT term metadata to avoid repeated computations
        self.enable_pgt_cache = enable_pgt_cache
        self.pgt_term_metadata_cache: Dict[str, RDFTermMeta] = {}

        # Rich progress bar configuration
        self.enable_rich = enable_rich

        # RDF Graph for maintaining the ArangoDB Collections & Keys
        # of the RDF Resources
        self.__adb_col_statements = RDFGraph()
        self.__adb_key_statements = RDFGraph()
        self.__adb_ns = "http://www.arangodb.com/"

        # An RDF Conjunctive Graph representing the
        # Ontology files found under the `arango_rdf/meta/` directory.
        # Essential for fully contextualizing an RDF Graph in ArangoDB.
        self.__meta_graph = RDFConjunctiveGraph()
        for ns in os.listdir(f"{PROJECT_DIR}/meta"):
            self.__meta_graph.parse(f"{PROJECT_DIR}/meta/{ns}", format="trig")

        # A mapping of Reified Subjects to their corresponding ArangoDB Edge.
        self.__reified_subject_map: Dict[Union[URIRef, BNode], Tuple[str, str, str]]

        # Commonly used URIs
        self.__rdfs_resource_str = str(RDFS.Resource)
        self.__rdfs_class_str = str(RDFS.Class)
        self.__rdfs_literal_str = str(RDFS.Literal)
        self.__rdfs_domain_str = str(RDFS.domain)
        self.__rdfs_range_str = str(RDFS.range)
        self.__rdf_type_str = str(RDF.type)
        self.__rdf_property_str = str(RDF.Property)

        # Commonly used ArangoDB Keys (derived from the commonly used URIs)
        self.__rdf_type_key = self.rdf_id_to_adb_key(self.__rdf_type_str)
        self.__rdf_property_key = self.rdf_id_to_adb_key(self.__rdf_property_str)
        self.__rdfs_domain_key = self.rdf_id_to_adb_key(self.__rdfs_domain_str)
        self.__rdfs_range_key = self.rdf_id_to_adb_key(self.__rdfs_range_str)

        # Custom ArangoDB Collections
        self.__resource_collection: Optional[StandardCollection] = None
        self.__predicate_collection: Optional[StandardCollection] = None
        self.__uri_map_collection: Optional[StandardCollection] = None

        logger.info(f"Instantiated ArangoRDF with database '{db.name}'")

    @property
    def rdf_attribute_prefix(self) -> str:
        return self.__rdf_attribute_prefix  # pragma: no cover

    def set_logging(self, level: Union[int, str]) -> None:
        logger.setLevel(level)

    def _get_spinner_progress(self, text: str) -> Union[Progress, NoOpProgress]:
        """Get a spinner progress bar or no-op version based on enable_rich."""
        if self.enable_rich:
            return get_spinner_progress(text)
        return NoOpProgress()

    def _get_bar_progress(self, text: str, color: str) -> Union[Progress, NoOpProgress]:
        """Get a bar progress or no-op version based on enable_rich."""
        if self.enable_rich:
            return get_bar_progress(text, color)
        return NoOpProgress()

    def _get_import_spinner_progress(self, text: str) -> Union[Progress, NoOpProgress]:
        """Get an import spinner progress or no-op version based on enable_rich."""
        if self.enable_rich:
            return get_import_spinner_progress(text)
        return NoOpProgress()

    def _live_context(self, *renderables: Any) -> Union[Live, NoOpLive]:
        """Get a Live context manager or no-op version based on enable_rich."""
        if self.enable_rich:
            return Live(Group(*renderables))
        return NoOpLive()

    ###########################
    # Public: ArangoDB -> RDF #
    ###########################

    def arangodb_to_rdf(
        self,
        name: str,
        rdf_graph: RDFGraph,
        metagraph: ADBMetagraph,
        explicit_metagraph: bool = True,
        list_conversion_mode: str = "static",
        dict_conversion_mode: str = "static",
        infer_type_from_adb_v_col: bool = False,
        include_adb_v_col_statements: bool = False,
        include_adb_v_key_statements: bool = False,
        include_adb_e_key_statements: bool = False,
        namespace_collection_name: Optional[str] = None,
        ignored_attributes: Optional[Set[str]] = None,
        **adb_export_kwargs: Any,
    ) -> RDFGraph:
        """Create an RDF Graph from an ArangoDB Graph via its Metagraph.

        :param name: The name of the ArangoDB Graph
        :type name: str
        :param rdf_graph: The target RDF Graph to insert into.
        :type rdf_graph: rdflib.graph.Graph
        :param metagraph: An dictionary of dictionaries defining the ArangoDB Vertex
            & Edge Collections whose entries will be inserted into the RDF Graph.
        :type metagraph: arango_rdf.typings.ADBMetagraph
        :param explicit_metagraph: Only keep the document attributes specified in
            **metagraph** when importing to RDF (is True by default). Otherwise,
            all document attributes are included. Defaults to True.
        :type explicit_metagraph: bool
        :param list_conversion_mode: Specify how ArangoDB JSON lists
            **within** and ArangoDB Document are processed into the RDF Graph.
            If "serialize", JSON Objects will be serialized into RDF Literals.
            If "collection", ArangoDB lists will be processed using the RDF Collection
            structure. If "container", ArangoDB lists will be processed using the RDF
            Container structure. If "static", elements within lists will be processed as
            individual statements. Defaults to "static".
            NOTE: "serialize" is recommended if round-tripping is desired, but
            **only** if round-tripping via **PGT**.
        :type list_conversion_mode: str
        :param dict_conversion_mode: Specify how ArangoDB JSON Objects
            **within** an ArangoDB Document are processed into the RDF Graph.
            If "serialize", JSON Objects will be serialized into RDF Literals.
            If "static", elements within dictionaries will be processed as individual
            statements with the help of BNodes. Defaults to "static".
            NOTE: "serialize" is recommended if round-tripping is desired, but
            **only** if round-tripping via **PGT**.
        :type dict_conversion_mode: str
        :param infer_type_from_adb_v_col: Specify whether `rdf:type` statements
            of the form `resource rdf:type adb_v_col .` should be inferred upon
            transferring ArangoDB Vertices into RDF.
        :type infer_type_from_adb_v_col: bool
        :param include_adb_v_col_statements: Specify whether `adb:collection`
            statements of the form `adb_vertex adb:collection adb_v_col .` should
            be generated upon transferring ArangoDB Documents into RDF. This can be used
            to maintain document collections when a user is interested in
            round-tripping.
        :type include_adb_v_col_statements: bool
        :param include_adb_v_key_statements: Specify whether `adb:key` statements
            of the form `adb_vertex adb:key adb_vertex["key"] .` should be generated
            upon transferring ArangoDB Documennts into RDF. This can be used to
            maintain document keys when a user is interested in round-tripping.
        :type include_adb_v_key_statements: bool
        :param include_adb_e_key_statements: Specify whether `adb:key` statements
            of the form `adb_edge adb:key adb_edge["key"] .` should be generated upon
            transferring ArangoDB Edges into RDF. This can be used to
            maintain edge keys when a user is interested in round-tripping.
            NOTE: Enabling this option will impose Triple Reification on all
            ArangoDB Edges.
        :type include_adb_e_key_statements: bool
        :param namespace_collection_name: The name of the ArangoDB Collection
            to store the namespace prefixes of **rdf_graph**. Useful for re-constructing
            the original RDF Graph from the ArangoDB Graph. Defaults to None,
            which means that the namespace prefixes will not be stored.
        :type namespace_collection_name: str | None
        :param ignored_attributes: The set of ArangoDB Document attributes to ignore
            when transferring ArangoDB Documents into RDF. Defaults to None,
            which means that all attributes will be transferred. Cannot be used
            if **explicit_metagraph** is True.
        :type ignored_attributes: Set[str] | None
        :param adb_export_kwargs: Keyword arguments to specify AQL query options when
            fetching documents from the ArangoDB instance. Full parameter list:
            https://docs.python-arango.com/en/main/specs.html#arango.aql.AQL.execute
        :type adb_export_kwargs: Any
        :return: The RDF representation of the ArangoDB Graph.
        :rtype: rdflib.graph.Graph
        """
        if explicit_metagraph and ignored_attributes:
            msg = "**ignored_attributes** cannot be used if **explicit_metagraph** is True"  # noqa: E501
            raise ValueError(msg)

        list_conversion_modes = {"serialize", "collection", "container", "static"}
        if list_conversion_mode not in list_conversion_modes:
            msg = f"Invalid **list_conversion_mode** parameter: {list_conversion_mode}"
            raise ValueError(msg)

        if dict_conversion_mode not in {"serialize", "static"}:
            msg = f"Invalid **dict_conversion_mode** parameter: {dict_conversion_mode}"
            raise ValueError(msg)

        self.__rdf_graph = rdf_graph
        self.__graph_supports_quads = isinstance(self.__rdf_graph, RDFConjunctiveGraph)

        self.__graph_ns = f"{self.db._conn._url_prefixes[0]}/{name}"
        self.__rdf_graph.bind("adb", self.__adb_ns)
        self.__rdf_graph.bind(name, f"{self.__graph_ns}/")

        self.__list_conversion = list_conversion_mode
        self.__dict_conversion = dict_conversion_mode
        self.__infer_type_from_adb_v_col = infer_type_from_adb_v_col
        self.__include_adb_v_col_statements = include_adb_v_col_statements
        self.__include_adb_v_key_statements = include_adb_v_key_statements
        self.__include_adb_e_key_statements = include_adb_e_key_statements

        # Maps ArangoDB Document IDs to RDFLib Terms (i.e URIRef, Literal, BNode)
        self.__term_map: Dict[str, RDFTerm] = {}

        # Maps ArangoDB Document IDs to URIRefs
        # Essential for preserving the original URIs of ArangoDB
        # Document Properties that were once in an RDF Graph
        self.__uri_map: Dict[str, URIRef] = {}

        # Set of keys to ignore when "unpacking" ArangoDB Documents
        self.adb_key_blacklist = {
            "_id",
            "_key",
            "_rev",
            "_from",
            "_to",
            self.__rdf_type_attr,
            self.__rdf_uri_attr,
            self.__rdf_value_attr,
            self.__rdf_label_attr,
            self.__rdf_sub_graph_uri_attr,
        }

        adb_e_cols = set(metagraph.get("edgeCollections", {}))

        #######################
        # PGT: Round-Tripping #
        #######################

        # Map the labels of the Property Collection to their corresponding URIs
        # e.g has_friend --> http://example.com/has_friend
        if self.db.has_collection("Property"):
            doc: Json
            for doc in self.db.collection("Property"):
                if doc.keys() >= {self.__rdf_uri_attr, self.__rdf_label_attr}:
                    # TODO: What if 2+ URIs have the same local name?
                    self.__uri_map[doc[self.__rdf_label_attr]] = URIRef(
                        doc[self.__rdf_uri_attr]
                    )

        # Re-bind the namespace prefixes of **rdf_graph**
        if namespace_collection_name:
            if not self.db.has_collection(namespace_collection_name):
                m = f"Namespace Collection '{namespace_collection_name}' does not exist"  # noqa: E501
                raise ValueError(m)

            for doc in self.db.collection(namespace_collection_name):
                self.__rdf_graph.bind(doc["prefix"], doc["uri"])

        ######################
        # Vertex Collections #
        ######################

        for v_col, atribs in metagraph["vertexCollections"].items():
            if v_col in adb_e_cols:
                continue

            logger.debug(f"Preparing '{v_col}' vertices")

            v_col_namespace = f"{self.__graph_ns}/{v_col}"
            v_col_uri = URIRef(v_col_namespace)
            self.__rdf_graph.bind(v_col, f"{v_col_namespace}#")

            # 1. Fetch ArangoDB vertices
            v_col_cursor, v_col_size = self.__fetch_adb_docs(
                v_col,
                False,
                atribs,
                explicit_metagraph,
                ignored_attributes,
                **adb_export_kwargs,
            )

            # 2. Process ArangoDB vertices
            self.__process_adb_cursor(
                "#97C423",
                v_col_cursor,
                v_col_size,
                self.__process_adb_vertex,
                v_col,
                v_col_uri,
            )

        ####################
        # Edge Collections #
        ####################

        for e_col, atribs in metagraph.get("edgeCollections", {}).items():
            logger.debug(f"Preparing '{e_col}' edges")

            e_col_namespace = f"{self.__graph_ns}/{e_col}"
            e_col_uri = URIRef(e_col_namespace)
            self.__rdf_graph.bind(e_col, f"{e_col_namespace}#")

            # 1. Fetch ArangoDB edges
            e_col_cursor, e_col_size = self.__fetch_adb_docs(
                e_col,
                True,
                atribs,
                explicit_metagraph,
                ignored_attributes,
                **adb_export_kwargs,
            )

            # 2. Process ArangoDB edges
            self.__process_adb_cursor(
                "#5E3108",
                e_col_cursor,
                e_col_size,
                self.__process_adb_edge,
                e_col,
                e_col_uri,
            )

        logger.info(f"Created RDF '{name}' Graph")
        return self.__rdf_graph

    def arangodb_collections_to_rdf(
        self,
        name: str,
        rdf_graph: RDFGraph,
        v_cols: Set[str],
        e_cols: Set[str],
        list_conversion_mode: str = "static",
        dict_conversion_mode: str = "static",
        infer_type_from_adb_v_col: bool = False,
        include_adb_v_col_statements: bool = False,
        include_adb_v_key_statements: bool = False,
        include_adb_e_key_statements: bool = False,
        namespace_collection_name: Optional[str] = None,
        ignored_attributes: Optional[Set[str]] = None,
        **adb_export_kwargs: Any,
    ) -> RDFGraph:
        """Create an RDF Graph from an ArangoDB Graph via its Collection Names.

        :param name: The name of the ArangoDB Graph
        :type name: str
        :param rdf_graph: The target RDF Graph to insert into.
        :type rdf_graph: rdflib.graph.Graph
        :param v_cols: The set of ArangoDB Vertex Collections to import to RDF.
        :type v_cols: Set[str]
        :param e_cols: The set of ArangoDB Edge Collections to import to RDF.
        :type e_cols: Set[str]
        :param list_conversion_mode: Specify how ArangoDB JSON lists
            **within** and ArangoDB Document are processed into the RDF Graph.
            If "serialize", JSON Objects will be serialized into RDF Literals.
            If "collection", ArangoDB lists will be processed using the RDF Collection
            structure. If "container", ArangoDB lists will be processed using the RDF
            Container structure. If "static", elements within lists will be processed as
            individual statements. Defaults to "static".
            NOTE: "serialize" is recommended if round-tripping is desired, but
            **only** if round-tripping via **PGT**.
        :type list_conversion_mode: str
        :param dict_conversion_mode: Specify how ArangoDB JSON Objects
            **within** an ArangoDB Document are processed into the RDF Graph.
            If "serialize", JSON Objects will be serialized into RDF Literals.
            If "static", elements within dictionaries will be processed as individual
            statements with the help of BNodes. Defaults to "static".
            NOTE: "serialize" is recommended if round-tripping is desired, but
            **only** if round-tripping via **PGT**.
        :type dict_conversion_mode: str
        :param infer_type_from_adb_v_col: Specify whether `rdf:type` statements
            of the form `resource rdf:type adb_v_col .` should be inferred upon
            transferring ArangoDB Vertices into RDF.
        :type infer_type_from_adb_v_col: bool
        :param include_adb_v_col_statements: Specify whether `adb:collection`
            statements of the form `adb_vertex adb:collection adb_v_col .` should
            be generated upon transferring ArangoDB Documents into RDF. This can be used
            to maintain document collections when a user is interested in
            round-tripping.
        :type include_adb_v_col_statements: bool
        :param include_adb_v_key_statements: Specify whether `adb:key` statements
            of the form `adb_vertex adb:key adb_vertex["key"] .` should be generated
            upon transferring ArangoDB Documennts into RDF. This can be used to
            maintain document keys when a user is interested in round-tripping.
        :type include_adb_v_key_statements: bool
        :param include_adb_e_key_statements: Specify whether `adb:key` statements
            of the form `adb_edge adb:key adb_edge["key"] .` should be generated upon
            transferring ArangoDB Edges into RDF. This can be used to
            maintain edge keys when a user is interested in round-tripping.
            NOTE: Enabling this option will impose Triple Reification on all
            ArangoDB Edges.
        :type include_adb_e_key_statements: bool
        :param namespace_collection_name: The name of the ArangoDB Collection
            to store the namespace prefixes of **rdf_graph**. Useful for re-constructing
            the original RDF Graph from the ArangoDB Graph. Defaults to None,
            which means that the namespace prefixes will not be stored.
        :type namespace_collection_name: str | None
        :param ignored_attributes: The set of ArangoDB Document attributes to ignore
            when transferring ArangoDB Documents into RDF. Defaults to None,
            which means that all attributes will be transferred.
        :type ignored_attributes: Set[str] | None
        :param adb_export_kwargs: Keyword arguments to specify AQL query options when
            fetching documents from the ArangoDB instance. Full parameter list:
            https://docs.python-arango.com/en/main/specs.html#arango.aql.AQL.execute
        :type adb_export_kwargs: Any
        :return: The RDF representation of the ArangoDB Graph.
        :rtype: rdflib.graph.Graph
        """
        metagraph: ADBMetagraph = {
            "vertexCollections": {col: set() for col in v_cols},
            "edgeCollections": {col: set() for col in e_cols},
        }

        explicit_metagraph = False

        return self.arangodb_to_rdf(
            name,
            rdf_graph,
            metagraph,
            explicit_metagraph,
            list_conversion_mode,
            dict_conversion_mode,
            infer_type_from_adb_v_col,
            include_adb_v_col_statements,
            include_adb_v_key_statements,
            include_adb_e_key_statements,
            namespace_collection_name,
            ignored_attributes,
            **adb_export_kwargs,
        )

    def arangodb_graph_to_rdf(
        self,
        name: str,
        rdf_graph: RDFGraph,
        list_conversion_mode: str = "static",
        dict_conversion_mode: str = "static",
        infer_type_from_adb_v_col: bool = False,
        include_adb_v_col_statements: bool = False,
        include_adb_v_key_statements: bool = False,
        include_adb_e_key_statements: bool = False,
        namespace_collection_name: Optional[str] = None,
        ignored_attributes: Optional[Set[str]] = None,
        **adb_export_kwargs: Any,
    ) -> RDFGraph:
        """Create an RDF Graph from an ArangoDB Graph via its Graph Name.

        :param name: The name of the ArangoDB Graph
        :type name: str
        :param rdf_graph: The target RDF Graph to insert into.
        :type rdf_graph: rdflib.graph.Graph
        :param list_conversion_mode: Specify how ArangoDB JSON lists
            **within** and ArangoDB Document are processed into the RDF Graph.
            If "serialize", JSON Objects will be serialized into RDF Literals.
            If "collection", ArangoDB lists will be processed using the RDF Collection
            structure. If "container", ArangoDB lists will be processed using the RDF
            Container structure. If "static", elements within lists will be processed as
            individual statements. Defaults to "static".
            NOTE: "serialize" is recommended if round-tripping is desired, but
            **only** if round-tripping via **PGT**.
        :type list_conversion_mode: str
        :param dict_conversion_mode: Specify how ArangoDB JSON Objects
            **within** an ArangoDB Document are processed into the RDF Graph.
            If "serialize", JSON Objects will be serialized into RDF Literals.
            If "static", elements within dictionaries will be processed as individual
            statements with the help of BNodes. Defaults to "static".
            NOTE: "serialize" is recommended if round-tripping is desired, but
            **only** if round-tripping via **PGT**.
        :type dict_conversion_mode: str
        :param infer_type_from_adb_v_col: Specify whether `rdf:type` statements
            of the form `resource rdf:type adb_v_col .` should be inferred upon
            transferring ArangoDB Vertices into RDF.
        :type infer_type_from_adb_v_col: bool
        :param include_adb_v_col_statements: Specify whether `adb:collection`
            statements of the form `adb_vertex adb:collection adb_v_col .` should
            be generated upon transferring ArangoDB Documents into RDF. This can be used
            to maintain document collections when a user is interested in
            round-tripping.
        :type include_adb_v_col_statements: bool
        :param include_adb_v_key_statements: Specify whether `adb:key` statements
            of the form `adb_vertex adb:key adb_vertex["key"] .` should be generated
            upon transferring ArangoDB Documennts into RDF. This can be used to
            maintain document keys when a user is interested in round-tripping.
        :type include_adb_v_key_statements: bool
        :param include_adb_e_key_statements: Specify whether `adb:key` statements
            of the form `adb_edge adb:key adb_edge["key"] .` should be generated upon
            transferring ArangoDB Edges into RDF. This can be used to
            maintain edge keys when a user is interested in round-tripping.
            NOTE: Enabling this option will impose Triple Reification on all
            ArangoDB Edges.
        :type include_adb_e_key_statements: bool
        :param namespace_collection_name: The name of the ArangoDB Collection
            to store the namespace prefixes of **rdf_graph**. Useful for re-constructing
            the original RDF Graph from the ArangoDB Graph. Defaults to None,
            which means that the namespace prefixes will not be stored.
        :type namespace_collection_name: str | None
        :param ignored_attributes: The set of ArangoDB Document attributes to ignore
            when transferring ArangoDB Documents into RDF. Defaults to None,
            which means that all attributes will be transferred.
        :type ignored_attributes: Set[str] | None
        :param adb_export_kwargs: Keyword arguments to specify AQL query options when
            fetching documents from the ArangoDB instance. Full parameter list:
            https://docs.python-arango.com/en/main/specs.html#arango.aql.AQL.execute
        :type adb_export_kwargs: Any
        :return: The RDF representation of the ArangoDB Graph.
        :rtype: rdflib.graph.Graph
        """
        graph = self.db.graph(name)
        v_cols = {col for col in graph.vertex_collections()}
        e_cols = {col["edge_collection"] for col in graph.edge_definitions()}

        return self.arangodb_collections_to_rdf(
            name,
            rdf_graph,
            v_cols,
            e_cols,
            list_conversion_mode,
            dict_conversion_mode,
            infer_type_from_adb_v_col,
            include_adb_v_col_statements,
            include_adb_v_key_statements,
            include_adb_e_key_statements,
            namespace_collection_name,
            ignored_attributes,
            **adb_export_kwargs,
        )

    #################################
    # Public: RDF -> ArangoDB (RPT) #
    #################################

    def rdf_to_arangodb_by_rpt(
        self,
        name: str,
        rdf_graph: RDFGraph,
        contextualize_graph: bool = False,
        flatten_reified_triples: bool = True,
        use_hashed_literals_as_keys: bool = True,
        overwrite_graph: bool = False,
        batch_size: Optional[int] = None,
        **adb_import_kwargs: Any,
    ) -> ADBGraph:
        """Create an ArangoDB Graph from an RDF Graph using
        the RDF-topology Preserving Transformation (RPT) Algorithm.

        RPT preserves the RDF Graph structure by transforming
        each RDF statement into a Property Graph Edge. More info on
        RPT can be found in the package's README file, or in the following
        paper: https://arxiv.org/pdf/2210.05781.pdf.

        This method will store the RDF Resources of
        **rdf_graph** under the following ArangoDB Collections:

        1. ``{Name}_URIRef``: Vertex collection for ``rdflib.term.URIRef`` resources.
        2. ``{Name}_BNode``: Vertex collection for ``rdflib.term.BNode`` resources.
        3. ``{Name}_Literal``: Vertex collection for ``rdflib.term.Literal`` resources.
        4. ``{Name}_Statement``: Edge collection for all triples/quads.

        :param name: The name of the RDF Graph
        :type name: str
        :param rdf_graph: The RDF Graph object. NOTE: This object is modified
            in-place in order for PGT to work. Do not expect the original state of
            **rdf_graph** to be preserved.
        :type: rdf_graph: rdflib.graph.Graph
        :param contextualize_graph: A work-in-progress flag that seeks
            to enhance the Terminology Box of **rdf_graph** by providing
            the following features:

            1) Loading Meta Ontologies (i.e OWL, RDF, RDFS, etc.)  into the RDF Graph
            2) Providing Domain & Range Inference
            3) Providing Domain & Range Introspection
        :type contextualize_graph: bool
        :param flatten_reified_triples: If set to False, will preserve the RDF
            structure of reified triples. If set to True, will convert any reified
            triple into a "regular" Property Graph Edge. Defaults to True.
        :type flatten_reified_triples: bool
        :param use_hashed_literals_as_keys: If set to False, will not use the
            hashed value of an RDF Literal as its ArangoDB Document Key (i.e
            a randomly-generated key will instead be used). If set to True, all
            RDF Literals with the same value will be represented as one single
            ArangoDB Document. Defaults to True.
        :type use_hashed_literals_as_keys: bool
        :param overwrite_graph: Overwrites the ArangoDB graph identified
            by **name** if it already exists, and drops its associated collections.
            Defaults to False.
        :type overwrite_graph: bool
            Defaults to False.
        :param batch_size: If specified, runs the ArangoDB Data Ingestion
            process for every **batch_size** RDF triples/quads within **rdf_graph**.
            Defaults to `len(rdf_graph)`.
        :type batch_size: int | None
        :param adb_import_kwargs: Keyword arguments to specify additional
            parameters for ArangoDB document insertion. Full parameter list:
            https://docs.python-arango.com/en/main/specs.html#arango.collection.Collection.insert_many
        :param adb_import_kwargs: Any
        :return: The ArangoDB Graph API wrapper.
        :rtype: arango.graph.Graph
        """
        if isinstance(rdf_graph, RDFDataset):  # pragma: no cover
            m = """
                Invalid type for **rdf_graph**: ArangoRDF does not yet
                support RDF Graphs of type rdflib.graph.Dataset. Consider
                using rdflib.graph.ConjunctiveGraph if using quads instead
                of triples is required.
            """
            raise TypeError(m)

        self.__rdf_graph = rdf_graph
        self.__adb_key_statements = self.extract_adb_key_statements(rdf_graph)

        # Create the ArangoDB documents buffer for this transformation
        adb_docs: ADBDocs = defaultdict(lambda: defaultdict(dict))

        # Reset the ArangoDB Config
        self.__contextualize_graph = contextualize_graph
        self.__use_hashed_literals_as_keys = use_hashed_literals_as_keys

        # Set the RPT ArangoDB Collection names
        self.__URIREF_COL = f"{name}_URIRef"
        self.__BNODE_COL = f"{name}_BNode"
        self.__LITERAL_COL = f"{name}_Literal"
        self.__STATEMENT_COL = f"{name}_Statement"

        # Builds the ArangoDB Edge Definitions of the (soon to be) ArangoDB Graph
        self.__e_col_map = defaultdict(lambda: defaultdict(set))
        self.__e_col_map[self.__STATEMENT_COL] = defaultdict(set)

        if overwrite_graph:
            self.db.delete_graph(name, ignore_missing=True, drop_collections=True)

        self.__predicate_collection = None
        self.__resource_collection = None
        self.__uri_map_collection = None

        #################################
        # Graph Contextualization (WIP) #
        #################################

        # NOTE: Graph Contextualization is an experimental work-in-progress
        contextualize_statement_func = empty_func
        if contextualize_graph:

            def contextualize_statement_func(
                s_meta: RDFTermMeta,
                p_meta: RDFTermMeta,
                o_meta: RDFTermMeta,
                sg_str: str,
            ) -> None:
                return self.__rpt_contextualize_statement(
                    adb_docs, s_meta, p_meta, o_meta, sg_str
                )

            self.__rdf_graph = self.__load_meta_ontology(self.__rdf_graph)

            with self._get_spinner_progress(
                "(RDF → ADB): Graph Contextualization"
            ) as rp:
                rp.add_task("")

                self.__explicit_type_map = self.__build_explicit_type_map()
                self.__subclass_tree = self.__build_subclass_tree()
                self.__predicate_scope = self.__build_predicate_scope()
                self.__domain_range_map = self.__build_domain_range_map()
                self.__type_map = self.__combine_type_map_and_dr_map()

        ###########################
        # Flatten Reified Triples #
        ###########################

        self.__reified_subject_map = {}
        if flatten_reified_triples:
            self.__flatten_reified_triples(
                adb_docs,
                self.__rpt_process_subject_predicate_object,
                contextualize_statement_func,
                batch_size,
                adb_import_kwargs,
            )

        #############
        # RPT: Main #
        #############

        s: RDFTerm  # Subject
        p: URIRef  # Predicate
        o: RDFTerm  # Object

        total = len(self.__rdf_graph)
        batch_size = batch_size or total
        bar_progress = self._get_bar_progress("(RDF → ADB): RPT", "#BF23C4")
        bar_progress_task = bar_progress.add_task("", total=total)
        spinner_progress = self._get_import_spinner_progress("    ")

        statements = (
            self.__rdf_graph.quads
            if isinstance(rdf_graph, RDFConjunctiveGraph)
            else self.__rdf_graph.triples
        )

        with self._live_context(bar_progress, spinner_progress):
            for i, (s, p, o, *sg) in enumerate(statements((None, None, None)), 1):
                logger.debug(f"RPT: {s} {p} {o} {sg}")

                self.__rpt_process_subject_predicate_object(
                    adb_docs, s, p, o, sg, None, contextualize_statement_func
                )

                if i % batch_size == 0:
                    bar_progress.update(bar_progress_task, advance=batch_size)
                    self.__insert_adb_docs(
                        adb_docs, spinner_progress, **adb_import_kwargs
                    )

            last_advance = total % batch_size if batch_size > 0 else 0
            bar_progress.update(bar_progress_task, advance=last_advance)
            self.__insert_adb_docs(adb_docs, spinner_progress, **adb_import_kwargs)

        return self.__rpt_create_adb_graph(name)

    #################################
    # Public: RDF -> ArangoDB (PGT) #
    #################################

    def rdf_to_arangodb_by_pgt(
        self,
        name: str,
        rdf_graph: RDFGraph,
        adb_col_statements: Optional[RDFGraph] = None,
        write_adb_col_statements: bool = True,
        contextualize_graph: bool = False,
        flatten_reified_triples: bool = True,
        overwrite_graph: bool = False,
        batch_size: Optional[int] = None,
        namespace_collection_name: Optional[str] = None,
        uri_map_collection_name: Optional[str] = None,
        resource_collection_name: Optional[str] = None,
        predicate_collection_name: Optional[str] = None,
        **adb_import_kwargs: Any,
    ) -> ADBGraph:
        """Create an ArangoDB Graph from an RDF Graph using
        the Property Graph Transformation (PGT) Algorithm.

        PGT ensures that datatype property statements (i.e statements whose
        objects are Literals) are mapped to document properties in the
        Property Graph. `Learn more about PGT here
        <./rdf_to_arangodb_pgt.html>`_.

        Contrary to RPT, this method will rely on
        the nature of the RDF Resource/Statement to determine which ArangoDB
        Collection it belongs to. This process is referred to as the
        ArangoDB Collection Mapping Process. `Learn more about the PGT ArangoDB
        Collection Mapping Process here
        <./rdf_to_arangodb_pgt.html#arangodb-collection-mapping-process>`_.

        Contrary to RPT, regardless of whether **contextualize_graph** is set to
        True or not, all RDF Predicates within every RDF Statement in **rdf_graph**
        will be processed as their own ArangoDB Document, and will be stored under
        the "Property" Vertex Collection.

        :param name: The name of the RDF Graph
        :type name: str
        :param rdf_graph: The RDF Graph object. NOTE: This object
            is modified in-place in order for PGT to work. Do not
            expect the original state of **rdf_graph** to be preserved.
        :type: rdf_graph: rdflib.graph.Graph
        :param adb_col_statements: An optional RDF Graph containing
            ArangoDB Collection statements of the form
            `adb_vertex http://arangodb/collection "adb_v_col" .`.
            Useful for creating a custom ArangoDB Collection mapping
            of RDF Resources within **rdf_graph**. Defaults to None.
            NOTE:  Cannot be used in conjunction with collection statements in
            **rdf_graph**.
        :type adb_col_statements: rdflib.graph.Graph | None
        :param write_adb_col_statements: Run the ArangoDB Collection
            Mapping Process for **rdf_graph** to write the ArangoDB
            Collection statements of the form
            `adb_vertex http://arangodb/collection "adb_v_col" . `
            into **adb_col_statements**. This parameter is ignored if
            **contextualize_graph** is set to True, as the ArangoDB
            Collection Mapping Process is required for Graph Contextualization.
            See :func:`write_adb_col_statements` for more information.
        :type write_adb_col_statements: bool
        :param contextualize_graph: A work-in-progress flag that seeks
            to enhance the Terminology Box of **rdf_graph** by providing
            the following features:

            1) Loading Meta Ontologies (i.e OWL, RDF, RDFS, etc.)  into the RDF Graph
            2) Providing Domain & Range Inference
            3) Providing Domain & Range Introspection
        :type contextualize_graph: bool
        :param flatten_reified_triples: If set to False, will preserve the RDF
            structure of any Reified Triple. If set to True, will "flatten" any reified
            triples into a regular Property Graph Edge. Defaults to True.

            `Learn more about Triple Reification here <./reification.html>`_.
        :type flatten_reified_triples: bool
        :param overwrite_graph: Overwrites the ArangoDB graph identified
            by **name** if it already exists, and drops its associated collections.
            Defaults to False.
        :type overwrite_graph: bool
        :param batch_size: If specified, runs the ArangoDB Data Ingestion
            process for every **batch_size** RDF triples/quads within **rdf_graph**.
            Defaults to None.
        :type batch_size: int | None
        :param namespace_collection_name: The name of the ArangoDB Collection
            to store the namespace prefixes of **rdf_graph**. Useful for re-constructing
            the original RDF Graph from the ArangoDB Graph. Defaults to None,
            which means that the namespace prefixes will not be stored.
            Not included in the ArangoDB Graph Edge Definitions.
        :type namespace_collection_name: str | None
        :param uri_map_collection_name: If specified, in addition to storing the URIs of
            **rdf_graph** in their respective collection, the URIs will also be stored in
            the specified ArangoDB Collection to map to the collection name they correspond to.
            This could be then used for multi-file imports, allowing ArangoRDF to
            check if the URIs of **rdf_graph** have already been imported into the
            ArangoDB Graph to avoid going through the ArangoDB Collection Mapping
            Process (for that URI) again. Not included in the ArangoDB Graph
            Edge Definitions. Cannot be used in conjunction with **resource_collection_name**.
        :type uri_map_collection_name: str | None
        :param resource_collection_name: If specified, will use this name as the
            ArangoDB Collection to store **all** RDF Resources, except Class and Property.
            This is useful for cases where you want to combine both RPT and PGT behavior, where
            rdf:type statements are stored as both edges and optionally as a property (i.e _types list),
            but not used for the ArangoDB Collection Mapping Process. Defaults to None.
            Cannot be used in conjunction with **uri_map_collection_name**.
        :type resource_collection_name: str | None
        :param predicate_collection_name: If specified, will use this name as the
            ArangoDB Collection to store all Edges. This is useful for cases
            where you want to combine both RPT and PGT behavior, where the predicate
            label is **not** used as the ArangoDB Collection name, but rather as a
            property of the Edge. Defaults to None.
        :type predicate_collection_name: str | None
        :param adb_import_kwargs: Keyword arguments to specify additional
            parameters for the ArangoDB Data Ingestion process.
            The full parameter list is
            `here <https://docs.python-arango.com/en/main/specs.html#arango.collection.Collection.insert_many>`_. # noqa: E501
        :return: The ArangoDB Graph API wrapper.
        :rtype: arango.graph.Graph
        """
        if isinstance(rdf_graph, RDFDataset):  # pragma: no cover
            m = """
                Invalid type for **rdf_graph**: ArangoRDF does not yet
                support RDF Graphs of type rdflib.graph.Dataset. Consider
                using rdflib.graph.ConjunctiveGraph if using quads instead
                of triples is required.
            """
            raise TypeError(m)

        namespace_prefixes = []
        if namespace_collection_name:
            namespace_prefixes = [
                (prefix, uri) for prefix, uri in rdf_graph.namespaces()
            ]

        self.__rdf_graph = rdf_graph
        self.__adb_key_statements = self.extract_adb_key_statements(rdf_graph)
        self.__uri_map_collection = None
        if uri_map_collection_name:
            if resource_collection_name:
                m = "Cannot specify both **uri_map_collection_name** and **resource_collection_name**."  # noqa: E501
                raise ValueError(m)

            if not self.db.has_collection(uri_map_collection_name):
                self.__create_collection(uri_map_collection_name)

            self.__uri_map_collection = self.db.collection(uri_map_collection_name)

        self.__resource_collection = None
        if resource_collection_name:
            if not self.db.has_collection(resource_collection_name):
                self.__create_collection(resource_collection_name)

            self.__resource_collection = self.db.collection(resource_collection_name)

        self.__predicate_collection = None
        if predicate_collection_name:
            if not self.db.has_collection(predicate_collection_name):
                self.__create_collection(predicate_collection_name, edge=True)

            self.__predicate_collection = self.db.collection(predicate_collection_name)

        # Create the ArangoDB documents buffer for this transformation
        adb_docs: ADBDocs = defaultdict(lambda: defaultdict(dict))

        # Reset the ArangoDB Config
        self.__contextualize_graph = contextualize_graph

        # A unique set of instance variables to
        # convert RDF Lists into JSON Lists during the PGT Process
        self.__rdf_list_heads: RDFListHeads = defaultdict(lambda: defaultdict(dict))
        self.__rdf_list_data: RDFListData = defaultdict(lambda: defaultdict(dict))
        self.__rdf_list_subjects: Set[RDFTerm] = set()
        self.__rdf_collection_subjects: Set[RDFTerm] = set()
        self.__rdf_container_subjects: Set[RDFTerm] = set()

        # The ArangoDB Collection name of all unidentified RDF Resources
        self.__UNKNOWN_RESOURCE = f"{name}_UnknownResource"

        # Builds the ArangoDB Edge Definitions of the (soon to be) ArangoDB Graph
        self.__e_col_map = defaultdict(lambda: defaultdict(set))

        self.__pgt_remove_blacklisted_statements()

        #################################
        # Graph Contextualization (WIP) #
        #################################

        # NOTE: Graph Contextualization is an experimental work-in-progress
        contextualize_statement_func = empty_func
        if contextualize_graph:

            def contextualize_statement_func(
                s_meta: RDFTermMeta,
                p_meta: RDFTermMeta,
                o_meta: RDFTermMeta,
                sg_str: str,
            ) -> None:
                return self.__pgt_contextualize_statement(
                    adb_docs, s_meta, p_meta, o_meta, sg_str
                )

            self.__rdf_graph = self.__load_meta_ontology(self.__rdf_graph)

            if self.__predicate_collection is not None:
                col = self.__predicate_collection.name
            else:
                col = "type"
                for label in ["domain", "range"]:
                    self.__e_col_map[label]["from"].add("Property")
                    self.__e_col_map[label]["to"].add("Class")

            self.__e_col_map[col]["from"].add("Property")
            self.__e_col_map[col]["from"].add("Class")
            self.__e_col_map[col]["to"].add("Class")

        ##################################
        # ArangoDB Collection Statements #
        ##################################

        rdf_graph_has_adb_col_statements = (None, self.adb_col_uri, None) in rdf_graph
        if adb_col_statements and rdf_graph_has_adb_col_statements:
            m = "Cannot specify both **adb_col_statements** and **rdf_graph** with ArangoDB Collection statements."  # noqa: E501
            raise ValueError(m)

        elif adb_col_statements:
            self.__adb_col_statements = adb_col_statements

        elif rdf_graph_has_adb_col_statements:
            self.__adb_col_statements = self.extract_adb_col_statements(
                self.__rdf_graph
            )

        else:
            self.__adb_col_statements = RDFGraph()

        if overwrite_graph:
            self.db.delete_graph(name, ignore_missing=True, drop_collections=True)

        if write_adb_col_statements or contextualize_graph:
            # Enabling Graph Contextualization forces
            # us to run the ArangoDB Collection Mapping algorithm
            # regardless of **write_adb_col_statements**
            self.__adb_col_statements = self.write_adb_col_statements(
                self.__rdf_graph, self.__adb_col_statements, uri_map_collection_name
            )

        ###########################
        # Flatten Reified Triples #
        ###########################

        self.__reified_subject_map = {}
        if flatten_reified_triples:
            self.__flatten_reified_triples(
                adb_docs,
                self.__pgt_process_subject_predicate_object,
                contextualize_statement_func,
                batch_size,
                adb_import_kwargs,
            )

        ##############################
        # PGT: Pre-compute RDF lists #
        ##############################

        self.__precompute_rdf_list_info()

        ###########################
        # PGT: Prepare Statements #
        ###########################

        statements = (
            self.__rdf_graph.quads
            if isinstance(self.__rdf_graph, RDFConjunctiveGraph)
            else self.__rdf_graph.triples
        )

        literal_statements = defaultdict(list)
        non_literal_statements = defaultdict(list)

        with self._get_spinner_progress("(RDF → ADB): PGT [Prepare Statements]") as rp:
            rp.add_task("")

            for s, p, o, *sg in statements((None, None, None)):
                if isinstance(o, Literal) and s not in self.__rdf_list_subjects:
                    literal_statements[(s, p)].append((o, sg))
                else:
                    non_literal_statements[(s, p)].append((o, sg))

        ###########################
        # PGT: Literal Statements #
        ###########################

        self.__pgt_parse_literal_statements(
            adb_docs,
            literal_statements,
            contextualize_statement_func,
            batch_size,
            adb_import_kwargs,
        )

        ###############################
        # PGT: Non-Literal Statements #
        ###############################

        self.__pgt_parse_non_literal_statements(
            adb_docs,
            non_literal_statements,
            contextualize_statement_func,
            batch_size,
            adb_import_kwargs,
        )

        ##################
        # PGT: RDF Lists #
        ##################

        bar_progress = self._get_bar_progress("(RDF → ADB): PGT [Lists]", "#EF7D00")
        spinner_progress = self._get_import_spinner_progress("    ")
        with self._live_context(bar_progress, spinner_progress):
            self.__pgt_process_rdf_lists(adb_docs, bar_progress)
            self.__insert_adb_docs(adb_docs, spinner_progress, **adb_import_kwargs)

        ###########################
        # PGT: Namespace Prefixes #
        ###########################

        if namespace_collection_name:
            if not self.db.has_collection(namespace_collection_name):
                self.__create_collection(namespace_collection_name)

            docs = [
                {"prefix": prefix, "uri": uri, "_key": self.hash(uri)}
                for prefix, uri in namespace_prefixes
            ]

            db = self.db if self.insert_async else self.async_db

            result = db.collection(namespace_collection_name).insert_many(
                docs, overwrite=True, raise_on_document_error=True
            )

            logger.debug(result)

        return self.__pgt_create_adb_graph(name)

    def __precompute_rdf_list_info(self) -> None:
        """Pre-compute RDF list information for optimization.

        Collects all RDF list subjects categorized by type once
        to avoid repeated computation in processing loops.
        """
        # Pre-compute collection subjects (RDF.first, RDF.rest)
        for s in self.__rdf_graph.subjects(RDF.first, None):
            self.__rdf_collection_subjects.add(s)
        for s in self.__rdf_graph.subjects(RDF.rest, None):
            self.__rdf_collection_subjects.add(s)

        # Pre-compute container subjects (container predicates _1, li, etc.)
        for s, p, _ in self.__rdf_graph:
            if isinstance(s, BNode):
                p_str = str(p)
                container_pattern_n = self.__container_pattern_n.match(p_str)
                container_pattern_li = self.__container_pattern_li.match(p_str)
                if container_pattern_n or container_pattern_li:
                    self.__rdf_container_subjects.add(s)

        self.__rdf_list_subjects = (
            self.__rdf_collection_subjects | self.__rdf_container_subjects
        )

    def __is_rdf_list_statement(self, s: RDFTerm, p: URIRef) -> str:
        """Returns the list type or empty string if not a list statement.

        :param s: The RDF Subject
        :param p: The RDF Predicate
        :return: The list type or empty string if not a list statement
        """
        if s in self.__rdf_collection_subjects and p in {RDF.first, RDF.rest}:
            return "_COLLECTION_BNODE"

        if s in self.__rdf_container_subjects:
            return "_CONTAINER_BNODE"

        return ""

    def write_adb_col_statements(
        self,
        rdf_graph: RDFGraph,
        adb_col_statements: Optional[RDFGraph] = None,
        uri_map_collection_name: Optional[str] = None,
    ) -> RDFGraph:
        """RDF -> ArangoDB (PGT): Run the ArangoDB Collection Mapping Process for
        **rdf_graph** to map RDF Resources to their respective ArangoDB Collection.

        The PGT Algorithm relies on the ArangoDB Collection Mapping Process to
        identify the ArangoDB Collection of every RDF Resource. Using this method prior
        to running :func:`rdf_to_arangodb_by_pgt` allows you to visualize and
        modify the mapping. `Learn more about the PGT ArangoDB
        Collection Mapping Process here
        <./rdf_to_arangodb_pgt.html#arangodb-collection-mapping-process>`_.

        NOTE: Running this method prior to :func:`rdf_to_arangodb_by_pgt`
        is unnecessary if the user is not interested in
        viewing/modifying the ArangoDB Mapping.

        NOTE: There can only be 1 `adb:collection` statement
        associated to each RDF Resource.

        :param rdf_graph: The RDF Graph object.
        :type rdf_graph: rdflib.graph.Graph
        :param adb_col_statements: An existing RDF Graph containing
            `adb:collection` statements. If not provided, a new RDF Graph
            will be created. Defaults to None.
            NOTE: The ArangoDB Collection Mapping Process
            relies heavily on mapping certain RDF Resources to the
            `"Class"` and `"Property"` ArangoDB Collections. Therefore,
            it is currently not possible to overwrite any RDF Resources
            that belong to these collections.
        :type adb_col_statements: rdflib.graph.Graph | None
        :type adb_col_statements: Optional[rdflib.graph.Graph]
        """
        self.__adb_col_statements = adb_col_statements or RDFGraph()
        self.__adb_col_statements.bind("adb", self.__adb_ns)

        self.__rdf_graph = rdf_graph
        self.controller.rdf_graph = rdf_graph

        with self._get_spinner_progress("(RDF → ADB): Write Col Statements") as rp:
            rp.add_task("")

            # 0. Add URI Collection statements
            if uri_map_collection_name:
                if not self.db.has_collection(uri_map_collection_name):
                    m = f"URI collection '{uri_map_collection_name}' does not exist"
                    raise ValueError(m)

                for doc in self.db.collection(uri_map_collection_name):
                    uri = URIRef(doc[self.__rdf_uri_attr])
                    collection = str(doc["collection"])
                    self.__add_adb_col_statement(uri, collection, True)

            # 1. RDF.type statements
            self.__explicit_type_map = self.__build_explicit_type_map(
                self.__add_adb_col_statement
            )

            # 2. RDF.subClassOf Statements
            self.__subclass_tree = self.__build_subclass_tree(
                self.__add_adb_col_statement
            )

            # 3. Domain & Range Statements
            self.__predicate_scope = self.__build_predicate_scope(
                self.__add_adb_col_statement
            )

            self.__domain_range_map = self.__build_domain_range_map()

            # 4. (Optional) Create the type map for Graph Contextualization
            if self.__contextualize_graph:
                self.__type_map = self.__combine_type_map_and_dr_map()

            # If the resource collection is not None, we don't need to run the
            # ArangoDB Collection Mapping Process to completion, since we will
            # be using the resource collection for all RDF Resources except for
            # Class and Property.
            if self.__resource_collection is not None:
                return self.__adb_col_statements

            # 5. Finalize **adb_col_statements**
            for rdf_map in [self.__explicit_type_map, self.__domain_range_map]:
                for rdf_resource, class_set in rdf_map.items():
                    t = (rdf_resource, None, None)
                    if t in self.__adb_col_statements or len(class_set) == 0:
                        continue  # pragma: no cover # (false negative)

                    best_class = self.controller.identify_best_class(
                        rdf_resource, class_set, self.__subclass_tree
                    )

                    adb_col = self.rdf_id_to_adb_label(best_class)

                    self.__add_adb_col_statement(rdf_resource, adb_col)

        return self.__adb_col_statements

    def migrate_unknown_resources(
        self, graph_name: str, uri_map_collection_name: str, **kwargs: Any
    ) -> Tuple[int, int]:
        """RDF -> ArangoDB (PGT): Migrate all UnknownResource statements to their
        respective ArangoDB Collection.

        NOTE: This method is only available if the user has passed a
        value to the **uri_map_collection_name** parameter of the
        :func:`rdf_to_arangodb_by_pgt` method.

        This method will migrate all UnknownResource statements to their
        respective ArangoDB Collection based on if the same RDF Resource
        exists in the **uri_map_collection_name**.

        Recommended to run this method after :func:`rdf_to_arangodb_by_pgt`
        if the user is not interested in maintaining the UnknownResource
        statements.

        :param graph_name: The name of the graph to migrate the Unknown Resources from.
        :type graph_name: str
        :param uri_map_collection_name: The name of the URI collection to migrate
            the Unknown Resources to.
        :type uri_map_collection_name: str
        :param kwargs: Keyword arguments passed to the AQL Query execution.
        :type kwargs: Any

        :return: The number of Unknown Resources migrated and the number
            of edges updated.
        :rtype: Tuple[int, int]
        """
        ur_collection_name = f"{graph_name}_UnknownResource"

        ur_collection = self.db.collection(ur_collection_name)
        uri_map_collection = self.db.collection(uri_map_collection_name)

        if ur_collection.count() == 0:
            logger.info("No Unknown Resources to migrate")
            return 0, 0

        if uri_map_collection.count() == 0:
            logger.info("No URI Collection to migrate to")
            return 0, 0

        old_ur_count = ur_collection.count()

        query = """
           FOR doc IN @@UR
            LET collection = FIRST(
                FOR uri IN @@URI
                    FILTER doc._key == uri._key
                    LIMIT 1
                    RETURN uri.collection
            )
            FILTER collection

            LET edges = (
                FOR v,e IN 1..1 ANY doc GRAPH @graph
                    LET key_to_modify = e._from == doc._id ? "_from" : "_to"
                    COLLECT e_col = PARSE_IDENTIFIER(e._id).collection
                    INTO edges_to_modify = {
                        _key: e._key,
                        [key_to_modify]: CONCAT(collection, "/", doc._key)
                    }

                    RETURN {e_col, edges_to_modify}
            )

            LET data = UNSET(doc, "_id", "_rev")
            REMOVE doc IN @@UR
            RETURN {data, collection, edges}
        """

        bind_vars = {
            "@UR": ur_collection_name,
            "@URI": uri_map_collection_name,
            "graph": graph_name,
        }

        cursor = self.db.aql.execute(query, bind_vars=bind_vars, stream=True, **kwargs)

        edge_count = 0

        with self._get_spinner_progress("(RDF → ADB): Migrate Unknown Resources") as sp:
            sp.add_task("")

            while not cursor.empty():
                for result in cursor.batch():
                    data = result["data"]
                    collection = result["collection"]

                    for edge_data in result["edges"]:
                        edge_collection = edge_data["e_col"]
                        edges_to_modify = edge_data["edges_to_modify"]
                        edge_count += len(edges_to_modify)

                        result = self.db.collection(edge_collection).update_many(
                            edges_to_modify,
                            merge=True,
                            raise_on_document_error=True,
                        )

                        logger.debug(result)

                    self.db.collection(collection).update(data, merge=True, silent=True)

                cursor.batch().clear()
                if cursor.has_more():
                    cursor.fetch()

        new_ur_count = ur_collection.count()

        ur_count_diff = old_ur_count - new_ur_count

        m = f"Migrated {ur_count_diff} Unknown Resources & updated {edge_count} edges"  # noqa: E501
        logger.info(m)

        return ur_count_diff, edge_count

    def migrate_edges_to_attributes(
        self,
        graph_name: str,
        edge_path: list[str],
        attribute_name: Optional[str] = None,
        edge_direction: str = "OUTBOUND",
        max_depth: int = 1,
        sort_clause: Optional[str] = None,
        return_clause: Optional[str] = None,
        filter_clause: Optional[str] = None,
        traversal_options: Optional[dict[str, Any]] = None,
    ) -> int:
        """RDF --> ArangoDB (PGT): Migrate all edges in the specified edge collection to
        attributes. This method is useful when combined with the
        **resource_collection_name** parameter of the :func:`rdf_to_arangodb_by_pgt`
        method.

        NOTE: It is recommended to run this method with **edge_path** set
        to **["type"]** after :func:`rdf_to_arangodb_by_pgt` if the user has set the
        **resource_collection_name** parameter.

        :param graph_name: The name of the graph to migrate the edges from.
        :type graph_name: str
        :param edge_path: The path of the edges to migrate. The first element is the
            starting edge collection, the last element is the ending edge collection.
            Can also include edge direction traversal
            (e.g ["OUTBOUND type", "OUTBOUND subClassOf"]).
        :type edge_path: list[str]
        :param edge_direction: The default traversal direction of the edges to migrate.
            Defaults to **OUTBOUND**.
        :type edge_direction: str
        :param max_depth: The maximum depth of the edge path to migrate.
            Defaults to 1.
        :type max_depth: int
        :param attribute_name: The name of the attribute to migrate the edges to.
            Defaults to **edge_path[0]**, prefixed with the
            **rdf_attribute_prefix** parameter set in the constructor.
        :type attribute_name: Optional[str]
        :param sort_clause: A SORT statement to order the traversed vertices.
            Defaults to f"v.{self.__rdf_attribute_prefix}label". If set to None,
            the vertex values will be ordered based on their traversal order.
        :type sort_clause: Optional[str]
        :param return_clause: A RETURN statement to return the specific value
            to add as an attribute from the traversed vertices.
            Defaults to f"v.{self.__rdf_attribute_prefix}label".
            Another option can be f"v.{self.__rdf_attribute_prefix}uri".
        :type return_clause: str
        :param filter_clause: A FILTER statement to filter the traversed
            edges & target vertices. Defaults to None.
        :type filter_clause: Optional[str]
        :param traversal_options: A dictionary of traversal options to pass to the
            AQL query. Defaults to None.
        :type traversal_options: Optional[dict[str, Any]]
        :return: The number of documents updated.
        :rtype: int
        """

        if not self.db.has_graph(graph_name):
            raise ValueError(f"Graph '{graph_name}' does not exist")

        if edge_direction.upper() not in {"OUTBOUND", "INBOUND", "ANY"}:
            raise ValueError(f"Invalid edge direction: {edge_direction}")

        graph = self.db.graph(graph_name)

        # Remove potential INBOUND/OUTBOUND/ANY prefix
        # (e.g ["OUTBOUND type", "OUTBOUND subClassOf"])
        edge_path_cleaned = [e_col.split(" ")[-1] for e_col in edge_path]
        start_edge_collection = edge_path_cleaned[0]

        start_node_collections = []
        all_e_ds = []
        for e_d in graph.edge_definitions():
            if e_d["edge_collection"] == start_edge_collection:
                start_node_collections = e_d["from_vertex_collections"]

            if e_d["edge_collection"] in edge_path_cleaned:
                all_e_ds.append(e_d)

        if not all_e_ds:
            m = f"No edge definitions found for '{edge_path}' in graph '{graph_name}'. Cannot migrate edges to attributes."  # noqa: E501
            raise ValueError(m)

        if attribute_name is None:
            attribute_name = f"{self.__rdf_attribute_prefix}{start_edge_collection}"

        if sort_clause is None:
            sort_clause = f"v.{self.__rdf_label_attr}"

        if return_clause is None:
            return_clause = f"v.{self.__rdf_label_attr}"

        if traversal_options is None:
            traversal_options = {
                "uniqueVertices": "path",
                "uniqueEdges": "path",
            }

        with_cols = {col for e_d in all_e_ds for col in e_d["to_vertex_collections"]}
        with_cols_str = "WITH " + ", ".join(with_cols)
        e_cols = ", ".join(edge_path_cleaned)

        count = 0
        for v_col in start_node_collections:
            query = f"""
                {with_cols_str}
                FOR doc IN @@v_col
                    LET labels = (
                        FOR v, e IN 1..{max_depth} {edge_direction} doc {e_cols}
                        OPTIONS {json.dumps(traversal_options)}
                            {f"FILTER {filter_clause}" if filter_clause else ""}
                            {f"SORT {sort_clause}" if sort_clause else ""}
                            RETURN {return_clause}
                    )

                    UPDATE doc WITH {{{attribute_name}: labels}} IN @@v_col
            """

            self.db.aql.execute(query, bind_vars={"@v_col": v_col})

            count += self.db.collection(v_col).count()

        m = f"Propagated {count} type statements as attributes"
        logger.info(m)

        return count

    #################################
    # Public: RDF -> ArangoDB (LPG) #
    #################################

    def rdf_to_arangodb_by_lpg(
        self,
        name: str,
        rdf_graph: RDFGraph,
        resource_collection_name: str = "Node",
        predicate_collection_name: str = "Edge",
        **pgt_kwargs: Any,
    ) -> ADBGraph:
        """RDF -> ArangoDB (LPG): Convert an RDF Graph into an ArangoDB Graph using
        the Labeled Property Graph (LPG) model.

        NOTE: It is highly recommend to use the :func:`migrate_edges_to_attributes`
        method after this function to apply the RDF type statements as attributes
        to the ArangoDB Documents in order to follow the LPG model.

        .. code-block:: python

            from arango_rdf import ArangoRDF

            adbrdf = ArangoRDF(db)

            adbrdf.rdf_to_arangodb_by_lpg("Test", rdf_graph)

            # Traverse all edges in the "Edge" collection labeled as "type",
            # and apply the RDF type statements as a list of strings to to the
            # ArangoDB Documents.
            adbrdf.migrate_edges_to_attributes(
                "Test", "Edge", "_type", filter_clause="e._label == 'type'"
            )

        This function is just a wrapper around the :func:`rdf_to_arangodb_by_pgt`
        method, but with the following differences:
        - Parameter **resource_collection_name** is required, defaults to **"Node"**
        - Parameter **predicate_collection_name** is required, defaults to **"Edge"**

        :param name: The name of the ArangoDB Graph.
        :type name: str
        :param rdf_graph: The RDF Graph to convert.
        :type rdf_graph: RDFGraph
        :param resource_collection_name: The name of the ArangoDB Collection to store
            the RDF Resources in.
        :type resource_collection_name: str
        :param predicate_collection_name: The name of the ArangoDB Collection to store
            the RDF Predicates in.
        :type predicate_collection_name: str
        :param pgt_kwargs: Keyword arguments to pass to the
            :func:`rdf_to_arangodb_by_pgt` method.
        :type pgt_kwargs: Any
        :return: The ArangoDB Graph.
        :rtype: arango.graph.Graph
        """

        if not resource_collection_name:
            raise ValueError("Parameter **resource_collection_name** is required")

        if not predicate_collection_name:
            raise ValueError("Parameter **predicate_collection_name** is required")

        return self.rdf_to_arangodb_by_pgt(
            name,
            rdf_graph,
            resource_collection_name=resource_collection_name,
            predicate_collection_name=predicate_collection_name,
            **pgt_kwargs,
        )

    ###########################################
    # Public: RDF -> ArangoDB (RPT, PGT, LPG) #
    ###########################################

    def rdf_id_to_adb_key(self, rdf_id: str, rdf_term: Optional[RDFTerm] = None) -> str:
        """RDF -> ArangoDB: Convert an RDF Resource ID string into an ArangoDB Key via
        some hashing function.

        If **rdf_term** is provided, then the value of
        the statement `rdf_term adb:key "<ArangoDB Document Key>" .` will be used
        as the ArangoDB Key (assuming that said statement exists).

        Current hashing function used: FarmHash

        :param rdf_id: The string representation of an RDF Resource
        :type rdf_id: str
        :param rdf_term: The optional RDF Term to check if it has an
            adb:key statement associated to it.
        :type rdf_term: Optional[URIRef | BNode | Literal]
        :return: The ArangoDB _key equivalent of **rdf_id**
        :rtype: str
        """
        if adb_key := self.__adb_key_statements.value(rdf_term, self.adb_key_uri):
            return str(adb_key)

        return self.hash(rdf_id)

    def hash(self, rdf_id: str) -> str:
        """RDF -> ArangoDB: Hash an RDF Resource ID string into an ArangoDB Key via
        some hashing function.

        Current hashing function used: FarmHash

        List of hashing functions tested & benchmarked:
        - Built-in hash() function
        - Hashlib MD5
        - xxHash
        - MurmurHash
        - CityHash
        - FarmHash

        :param rdf_id: The string representation of an RDF Resource
        :type rdf_id: str
        :return: The ArangoDB _key equivalent of **rdf_id**
        :rtype: str
        """

        # hash(rdf_id) # NOTE: not platform/session independent!
        # hashlib.md5(rdf_id.encode()).hexdigest()
        # xxhash.xxh64(rdf_id.encode()).hexdigest()
        # mmh3.hash64(rdf_id, signed=False)[0]
        # cityhash.CityHash64(item)
        # farmhash.Fingerprint64(rdf_id)

        return str(farmhash.Fingerprint64(rdf_id))

    def rdf_id_to_adb_label(self, rdf_id: str) -> str:
        """RDF -> ArangoDB: Return the suffix of an RDF URI.

        The suffix can (1) be used as an ArangoDB Collection name,
        or (2) be used as the `_label` property value for an ArangoDB Document.

        For example:
        - `http://example.com/Person -> "Person"`
        - `http://example.com/Person#Bob -> "Bob"`
        - `http://example.com/Person:Bob -> "Bob"`

        :param rdf_id: The string representation of a URIRef
        :type rdf_id: str
        :return: The suffix of the RDF URI string
        :rtype: str
        """
        return re.split("/|#|:", rdf_id)[-1] or rdf_id

    ############################
    # Private: ArangoDB -> RDF #
    ############################

    def __fetch_adb_docs(
        self,
        col: str,
        is_edge: bool,
        attributes: Set[str],
        explicit_metagraph: bool,
        ignored_attributes: Optional[Set[str]],
        **adb_export_kwargs: Any,
    ) -> Tuple[Cursor, int]:
        """ArangoDB -> RDF: Fetches ArangoDB documents within a collection.

        :param col: The ArangoDB collection.
        :type col: str
        :param is_edge: True if **col** is an edge collection.
        :type is_edge: bool
        :param attributes: The set of document attributes.
        :type attributes: Set[str]
        :param explicit_metagraph: If True, only return the set of **attributes**
            specified when fetching the documents of the collection **col**.
            If False, all document attributes are included.
        :type explicit_metagraph: bool
        :param ignored_attributes: The set of ArangoDB Document attributes to ignore
            when transferring ArangoDB Documents into RDF. Defaults to None,
            which means that all attributes will be transferred. Cannot be used
            if **explicit_metagraph** is True.
        :type ignored_attributes: Set[str] | None
        :param adb_export_kwargs: Keyword arguments to specify AQL query options when
            fetching documents from the ArangoDB instance.
        :type adb_export_kwargs: Any
        :return: The document cursor along with the total collection size.
        :rtype: Tuple[arango.cursor.Cursor, int]
        """
        if explicit_metagraph and ignored_attributes:
            msg = "**ignored_attributes** cannot be used if **explicit_metagraph** is True"  # noqa: E501
            raise ValueError(msg)

        aql_return_value = "doc"

        if explicit_metagraph:
            default_keys = ["_id", "_key"]
            default_keys += ["_from", "_to"] if is_edge else []
            aql_return_value = f"KEEP(doc, {list(attributes) + default_keys})"

        if ignored_attributes:
            aql_return_value = f"UNSET(doc, {list(ignored_attributes)})"

        col_size: int = self.db.collection(col).count()

        with self._get_spinner_progress(
            f"(ADB → RDF): Export '{col}' ({col_size})"
        ) as sp:
            sp.add_task("")

            cursor: Cursor = self.db.aql.execute(
                f"FOR doc IN @@col RETURN {aql_return_value}",
                bind_vars={"@col": col},
                **{**adb_export_kwargs, **{"stream": True}},
            )

            return cursor, col_size

    def __process_adb_cursor(
        self,
        progress_color: str,
        cursor: Cursor,
        col_size: int,
        process_adb_doc: Callable[..., None],
        col: str,
        col_uri: URIRef,
    ) -> None:
        """ArangoDB -> RDF: Processes the ArangoDB Cursors for vertices and edges.

        :param progress_color: The progress bar color.
        :type progress_color: str
        :param cursor: The ArangoDB cursor for the current **col**.
        :type cursor: arango.cursor.Cursor
        :param col_size: The size of **col**.
        :type col_size: int
        :param process_adb_doc: The function to process the cursor data.
        :type process_adb_doc: Callable
        :param col: The ArangoDB collection for the current **cursor**.
        :type col: str
        :param col_uri: The URIRef associated to the ArangoDB Collection.
        :type col_uri: URIRef
        """

        progress = self._get_bar_progress(f"(ADB → RDF): '{col}'", progress_color)
        progress_task_id = progress.add_task("", total=col_size)

        with self._live_context(progress):
            while not cursor.empty():
                for doc in cursor.batch():
                    process_adb_doc(doc, col, col_uri)
                    progress.advance(progress_task_id)

                cursor.batch().clear()
                if cursor.has_more():
                    cursor.fetch()

    def __process_adb_vertex(
        self,
        adb_v: Json,
        v_col: str,
        v_col_uri: URIRef,
    ) -> RDFTerm:
        """ArangoDB -> RDF: Processes an ArangoDB vertex.

        Does the following:
        1. Extracts the RDF Term representing the ArangoDB vertex.
        2. Extracts the Subgraph URI value from the vertex (if any)
        3. Unpacks any vertex properties of **adb_v**
        4. Infers the RDF Type of **adb_v** if enabled
        5. Includes the ArangoDB Collection name of **adb_v** if enabled
        6. Includes the ArangoDB Document Key of **adb_v** if enabled

        :param adb_v: The ArangoDB vertex.
        :type adb_v: Dict[str, Any]
        :param v_col: The ArangoDB vertex collection.
        :type v_col: str
        :param v_col_uri: The URIRef associated to the ArangoDB Collection.
        :type v_col_uri: URIRef
        :return: The RDF representation of the ArangoDB vertex.
        :rtype: URIRef | BNode | Literal
        """
        term = self.__adb_doc_to_rdf_term(adb_v, v_col)
        self.__term_map[adb_v["_id"]] = term

        if isinstance(term, Literal):
            return term

        sg = URIRef(adb_v.get(self.__rdf_sub_graph_uri_attr, "")) or None
        self.__unpack_adb_doc(adb_v, v_col, term, sg)

        if self.__infer_type_from_adb_v_col:
            self.__add_to_rdf_graph(term, RDF.type, v_col_uri)

        if self.__include_adb_v_col_statements:
            self.__add_to_rdf_graph(term, self.adb_col_uri, Literal(v_col))

        if self.__include_adb_v_key_statements:
            self.__add_to_rdf_graph(term, self.adb_key_uri, Literal(adb_v["_key"]))

        return term

    def __process_adb_edge(
        self,
        adb_e: Json,
        e_col: str,
        e_col_uri: URIRef,
        edge_is_referenced_by_another_edge: bool = False,
    ) -> URIRef:
        """ArangoDB -> RDF: Process an ArangoDB Edge

        Does the following:
        1. Extracts the (subjecct, predicate, object) values from **adb_e**
        2. Extracts the Subgraph URI value from the edge (if any)
        3. Adds the (subject, predicate, object) statement to the RDF Graph
        4. Unpacks any edge properties of **adb_e**
        5. Reifies the (subject, predicate, object) statement

        :param adb_e: The ArangoDB Edge
        :type adb_e: Json
        :param e_col: The ArangoDB Collection name of **adb_e**.
        :type e_col: str
        :param e_col_uri: The URIRef associated to the ArangoDB Collection
            of **adb_e**. Used if **adb_e** does not have a `_uri` attribute.
        :type e_col_uri: URIRef
        :param edge_is_referenced_by_another_edge: Set to True if the current edge
            is set as the "_from" or "_to" value of another arbitrary ArangoDB Edge.
        :type edge_is_referenced_by_another_edge: bool
        :return: The RDF representation of the ArangoDB Edge.
        :rtype: URIRef
        """
        _from: str = adb_e["_from"]
        _to: str = adb_e["_to"]
        _uri = adb_e.get(self.__rdf_uri_attr, "")

        subject = self.__get_rdf_term_of_adb_doc(_from)
        predicate = URIRef(_uri) or e_col_uri
        object = self.__get_rdf_term_of_adb_doc(_to)
        sg = URIRef(adb_e.get(self.__rdf_sub_graph_uri_attr, "")) or None

        # TODO: Revisit when rdflib introduces RDF-star support
        # edge_uri = (subject, predicate, object, sg)
        edge_uri = URIRef(f"{_uri or e_col_uri}#{adb_e['_key']}")

        self.__unpack_adb_doc(adb_e, e_col, edge_uri, sg)

        edge_has_property_data = len(adb_e.keys() - self.adb_key_blacklist) != 0
        if (
            edge_has_property_data
            or edge_is_referenced_by_another_edge
            or self.__include_adb_e_key_statements
        ):
            # Triple reification overwrites the existing triple (if any)
            # NOTE: Case 15_2 RPT is flaky due to this overwrite
            self.__rdf_graph.remove((subject, predicate, object))

            self.__reify_rdf_triple(
                edge_uri, adb_e["_key"], subject, predicate, object, sg
            )

        elif (edge_uri, None, None) not in self.__rdf_graph:
            self.__add_to_rdf_graph(subject, predicate, object, sg)

        return edge_uri

    def __adb_doc_to_rdf_term(self, doc: Json, col: str) -> RDFTerm:
        """ArangoDB -> RDF: Converts an ArangoDB Document into an RDF Term.

        :param doc: An arbitrary ArangoDB document.
        :type doc: Dict[str, Any]
        :param col: The ArangoDB Collection name of **doc**.
        :type col: str
        :return: The RDF Term representing the ArangoDB document
        :rtype: URIRef | BNode | Literal
        """
        key_map = {
            "URIRef": self.__rdf_uri_attr,
            "Literal": self.__rdf_value_attr,
            "BNode": "_key",
        }

        rdf_type = doc.get(self.__rdf_type_attr, "URIRef")  # Default to URIRef
        val = doc.get(key_map[rdf_type], f"{self.__graph_ns}/{col}#{doc['_key']}")

        if rdf_type == "URIRef":
            return URIRef(val)

        elif rdf_type == "BNode":
            return BNode(val)

        elif rdf_type == "Literal":
            if self.__rdf_lang_attr in doc:
                return Literal(val, lang=doc[self.__rdf_lang_attr])

            elif self.__rdf_datatype_attr in doc:
                return Literal(val, datatype=doc[self.__rdf_datatype_attr])

            else:
                return Literal(val)

        else:  # pragma: no cover
            raise ValueError(f"Unrecognized type '{rdf_type}' ({doc})")

    def __unpack_adb_doc(
        self, doc: Json, col: str, term: RDFTerm, sg: Optional[URIRef]
    ) -> None:
        """ArangoDB -> RDF: Transfer ArangoDB Document Properties of **doc**
        into the RDF Graph, as statements.

        :param doc: The ArangoDB Document
        :type doc: Dict[str, Any]
        :param col: The ArangoDB Collection name of **doc**.
        :type col: str
        :param term: The RDF representation of **doc**
        :type term: URIRef | BNode | Literal
        :param sg: The Sub Graph URI of **doc**, if any.
        :type sg: URIRef | None
        :return: Returns True if the ArangoDB Document has property data.
        :rtype: bool
        """
        for k in doc.keys() - self.adb_key_blacklist:
            val = doc[k]
            p = self.__uri_map.get(k, URIRef(f"{self.__graph_ns}/{k}"))
            self.__adb_val_to_rdf_val(col, term, p, val, sg)

            # if self.__include_adb_v_col_statements:
            #     self.__add_to_rdf_graph(p, self.adb_col_uri, Literal("Property"))

    def __add_to_rdf_graph(
        self, s: RDFTerm, p: URIRef, o: RDFTerm, sg: Optional[URIRef] = None
    ) -> None:
        """ArangoDB -> RDF: Insert (s,p,o) into the RDF Graph.

        :param s: The RDF Subject object of the (s,p,o) statement.
        :type s: URIRef | BNode
        :param p: The RDF Predicate object of the (s,p,o) statement.
        :type p: URIRef
        :param o: The RDF Object object of the (s,p,o) statement.
        :type o: URIRef | BNode | Literal
        :param sg: The Sub Graph URI of the (s,p,o) statement, if any.
        :type sg: URIRef | None
        """
        t = (s, p, o, sg) if sg and self.__graph_supports_quads else (s, p, o)
        self.__rdf_graph.add(t)

    def __get_rdf_term_of_adb_doc(self, doc_id: str) -> RDFTerm:
        """ArangoDB -> RDF: Returns the RDF Term representing an ArangoDB Document
        that was previously processed & placed into the `self.term_map`, or
        is missing from the `self.term_map`. The latter can happen when
        ArangoDB Edges refer to other ArangoDB Edges.

        :param doc_id: An arbitrary ArangoDB Document ID.
        :type doc: str
        :return: The RDF Term representing the ArangoDB document
        :rtype: URIRef | BNode | Literal
        """
        if term := self.__term_map.get(doc_id):
            return term

        # Expensive, but what else can we do?
        doc: Json = self.db.document({"_id": doc_id})
        col = doc_id.split("/")[0]
        col_uri = URIRef(f"{self.__graph_ns}{col}")

        if not doc:
            m = f"""
                Unable to find ArangoDB Document
                '{doc_id}' within Database {self.db.name}
            """
            raise ValueError(m)

        # **doc** is an ArangoDB Edge
        elif "_from" in doc:
            edge_uri = self.__process_adb_edge(
                doc, col, col_uri, edge_is_referenced_by_another_edge=True
            )

            # The edge is added as a term given that it's a HyperEdge
            self.__term_map[doc_id] = edge_uri

            return edge_uri

        # **doc** is an ArangoDB Vertex
        else:
            # term = self.__adb_doc_to_rdf_term(doc)
            # self.__term_map[doc_id] = term
            return self.__process_adb_vertex(doc, col, col_uri)

    def __reify_rdf_triple(
        self,
        edge_uri: URIRef,
        edge_key: str,
        s: RDFTerm,
        p: URIRef,
        o: RDFTerm,
        sg: Optional[URIRef] = None,
    ) -> None:
        """ArangoDB -> RDF: Reify an RDF Statement.

        Due to rdflib's missing support for RDF-star, triple reification
        is introduced as a workaround to support transforming ArangoDB Edges
        into RDF Statements without losing any edge properties.

        :param edge_uri: The URIRef representing the ArangoDB Edge,
            soon to be transformed into an RDF Statement.
        :type edge_uri: URIRef
        :param edge_key: The ArangoDB Document key of the ArangoDB Edge.
        :type edge_key: str
        :param s: The RDF Subject of the RDF Statement.
        :type s: URIRef | BNode
        :param p: The RDF Predicate of the RDF Statement.
        :type p: URIRef
        :param o: The RDF Object of the RDF Statement.
        :type o: URIRef | BNode | Literal
        :param sg: The Sub Graph URI of the (s,p,o) statement, if any.
        :type sg: URIRef | None
        """
        self.__add_to_rdf_graph(edge_uri, RDF.type, RDF.Statement, sg)
        self.__add_to_rdf_graph(edge_uri, RDF.subject, s, sg)
        self.__add_to_rdf_graph(edge_uri, RDF.predicate, p, sg)
        self.__add_to_rdf_graph(edge_uri, RDF.object, o, sg)

        if self.__include_adb_e_key_statements:
            self.__add_to_rdf_graph(edge_uri, self.adb_key_uri, Literal(edge_key))

        # if self.__include_adb_v_col_statements:
        #     self.__add_to_rdf_graph(p, self.adb_col_uri, Literal("Property"))

    def __adb_val_to_rdf_val(
        self, col: str, s: RDFTerm, p: URIRef, val: Any, sg: Optional[URIRef] = None
    ) -> None:
        """ArangoDB -> RDF: Insert an arbitrary ArangoDB Document Property
        value into the RDF Graph.

        If the ArangoDB document property **val** is of type list
        or dict, then a recursive process is introduced to unpack
        the ArangoDB document property into multiple RDF Statements.
        Otherwise, the ArangoDB Document Property is treated as
        a Literal in the context of RDF.

        :param col: The ArangoDB Collection name of **s**.
        :type col: str
        :param s: The RDF Subject of the to-be-inserted RDF Statement.
        :type s: URIRef | BNode
        :param p: The RDF Predicate of the to-be-inserted RDF Statement.
            This represents the ArangoDB Document Property key name.
        :type p: URIRef
        :param sub_key: The ArangoDB property key of the document
            that will be used to store the value.
        :type sub_key: str
        :param val: Some RDF value to insert.
        :type val: Any
        :param sg: The Sub Graph URI of the (s,p,val) statement, if any.
        :type sg: URIRef | None
        """

        if isinstance(val, list):
            if self.__list_conversion == "static":
                for v in val:
                    self.__adb_val_to_rdf_val(col, s, p, v, sg)

            elif self.__list_conversion == "collection":
                node: RDFTerm = BNode()
                self.__add_to_rdf_graph(s, p, node, sg)

                rest: RDFTerm
                for i, v in enumerate(val):
                    self.__adb_val_to_rdf_val(col, node, RDF.first, v)

                    rest = RDF.nil if i == len(val) - 1 else BNode()
                    self.__add_to_rdf_graph(node, RDF.rest, rest, sg)
                    node = rest

            elif self.__list_conversion == "container":
                bnode = BNode()
                self.__add_to_rdf_graph(s, p, bnode, sg)

                for i, v in enumerate(val, 1):
                    _n = URIRef(f"{RDF}_{i}")
                    self.__adb_val_to_rdf_val(col, bnode, _n, v, sg)

            else:  # serialize
                val = json.dumps(val)
                self.__add_to_rdf_graph(s, p, Literal(val), sg)

        elif isinstance(val, dict):
            if self.__dict_conversion == "static":
                bnode = BNode()
                self.__add_to_rdf_graph(s, p, bnode, sg)

                for k, v in val.items():
                    p = self.__uri_map.get(k, URIRef(f"{self.__graph_ns}/{k}"))
                    self.__adb_val_to_rdf_val(col, bnode, p, v, sg)

            else:  # serialize
                val = json.dumps(val)
                self.__add_to_rdf_graph(s, p, Literal(val), sg)

        else:
            # TODO: Datatype? Lang? Not yet sure how to handle this...
            self.__add_to_rdf_graph(s, p, Literal(val), sg)

    #############################
    # Public: ArangoDB <-> RDF  #
    #############################

    def extract_adb_col_statements(
        self, rdf_graph: RDFGraph, keep_adb_col_statements_in_rdf_graph: bool = False
    ) -> RDFGraph:
        """ArangoDB <-> RDF: Extracts `adb:collection` statements
        from an RDF Graph.

        :param rdf_graph: The RDF Graph to extract the statements from.
        :type rdf_graph: rdflib.graph.Graph
        :param keep_adb_col_statements_in_rdf_graph: Keeps the ArangoDB Collection
            statements in the original graph once extracted. Defaults to False.
        :type keep_adb_col_statements_in_rdf_graph: bool
        :return: The ArangoDB Collection Mapping graph.
        :rtype: rdflib.graph.Graph
        """
        return self.__extract_statements(
            (None, self.adb_col_uri, None),
            rdf_graph,
            keep_adb_col_statements_in_rdf_graph,
        )

    def extract_adb_key_statements(
        self, rdf_graph: RDFGraph, keep_adb_key_statements_in_rdf_graph: bool = False
    ) -> RDFGraph:
        """ArangoDB <-> RDF: Extracts the `adb:key` statements from an RDF Graph.

        :param rdf_graph: The RDF Graph to extract the statements from.
        :type rdf_graph: rdflib.graph.Graph
        :param keep_adb_col_statements_in_rdf_graph: Keeps the ArangoDB Collection
            Mapping statements in the original graph once extracted. Defaults to False.
        :type keep_adb_col_statements_in_rdf_graph: bool
        :return: The ArangoDB Collection Mapping graph.
        :rtype: rdflib.graph.Graph
        """
        return self.__extract_statements(
            (None, self.adb_key_uri, None),
            rdf_graph,
            keep_adb_key_statements_in_rdf_graph,
        )

    ##################################
    # Private: RDF -> ArangoDB (RPT) #
    ##################################

    def __rpt_process_subject_predicate_object(
        self,
        adb_docs: ADBDocs,
        s: RDFTerm,
        p: URIRef,
        o: RDFTerm,
        sg: Optional[List[Any]],
        reified_subject: Optional[Union[URIRef, BNode]],
        contextualize_statement_func: Callable[..., None],
    ) -> None:
        """RDF -> ArangoDB (RPT): Processes the RDF Statement (s, p, o)
        as an ArangoDB document for RPT.

        :param adb_docs: The ArangoDB documents buffer to populate.
        :type adb_docs: ADBDocs
        :param s: The RDF Subject of the RDF Statement.
        :type s: URIRef | BNode
        :param p: The RDF Predicate of the RDF Statement.
        :type p: URIRef
        :param o: The RDF Object of the RDF Statement.
        :type o: URIRef | BNode | Literal
        :param sg: The Sub Graph URI of the (s,p,o) statement, if any.
        :type sg: URIRef | None
        :param reified_subject: The RDF Subject of the RDF Statement
            (s, p, o) that was originally in Reified form. Only used
            during `ArangoRDF.__flatten_reified_triples()`.
        :type reified_subject: URIRef | BNode | None
        :param contextualize_statement_func: A function that contextualizes
            an RDF Statement. A no-op function is used if Graph Contextualization
            is disabled.
        :type contextualize_statement_func: Callable[..., None]
        """
        sg_str = self.__get_subgraph_str(sg)

        s_meta = self.__rpt_process_term(adb_docs, s)

        o_meta = self.__rpt_process_term(adb_docs, o)

        self.__rpt_process_statement(
            adb_docs, s_meta, p, o_meta, sg_str, reified_subject
        )

        contextualize_statement_func(s_meta, p, o_meta, sg_str)

    def __rpt_process_term(self, adb_docs: ADBDocs, t: RDFTerm) -> RDFTermMeta:
        """RDF -> ArangoDB (RPT): Process an RDF Term as an ArangoDB document
        via RPT Standards. Returns the ArangoDB Collection & Document Key associated
        to the RDF term, along with its string representation.

        :param adb_docs: The ArangoDB documents buffer to populate.
        :type adb_docs: ADBDocs
        :param t: The RDF Term to process
        :type t: URIRef | BNode | Literal
        :return: The RDF Term object, along with its associated ArangoDB
            Collection name, Document Key, and Document label.
        :rtype: Tuple[URIRef | BNode | Literal, str, str, str]
        """

        t_str = str(t)
        t_col = ""
        t_key = self.rdf_id_to_adb_key(t_str, t)
        t_label = ""

        if t in self.__reified_subject_map:
            t_col = self.__STATEMENT_COL

            # TODO: Populate adb docs? Or uncessary?

        elif isinstance(t, URIRef):
            t_col = self.__URIREF_COL
            t_label = self.rdf_id_to_adb_label(t_str)

            adb_docs[t_col][t_key] = {
                "_key": t_key,
                self.__rdf_uri_attr: t_str,
                self.__rdf_label_attr: t_label,
                self.__rdf_type_attr: "URIRef",
            }

        elif isinstance(t, BNode):
            t_col = self.__BNODE_COL

            adb_docs[t_col][t_key] = {
                "_key": t_key,
                self.__rdf_label_attr: "",
                self.__rdf_type_attr: "BNode",
            }

        elif isinstance(t, Literal):
            t_col = self.__LITERAL_COL
            t_value = self.__get_literal_val(t, t_str)
            t_label = t_value

            adb_docs[t_col][t_key] = {
                self.__rdf_value_attr: t_value,
                self.__rdf_label_attr: t_label,  # TODO: REVISIT
                self.__rdf_type_attr: "Literal",
            }

            if self.__use_hashed_literals_as_keys:
                adb_docs[t_col][t_key]["_key"] = t_key

            if t.language:
                adb_docs[t_col][t_key][self.__rdf_lang_attr] = t.language
            elif t.datatype:
                adb_docs[t_col][t_key][self.__rdf_datatype_attr] = str(t.datatype)

        else:
            raise ValueError(f"Unable to process {t}")  # pragma: no cover

        return t, t_col, t_key, t_label

    def __rpt_process_statement(
        self,
        adb_docs: ADBDocs,
        s_meta: RDFTermMeta,
        p: URIRef,
        o_meta: RDFTermMeta,
        sg_str: str,
        reified_subject: Optional[Union[URIRef, BNode]] = None,
    ) -> None:
        """RDF -> ArangoDB (RPT): Processes the RDF Statement (s, p, o)
        as an ArangoDB edge for RPT.

        :param adb_docs: The ArangoDB documents buffer to populate.
        :type adb_docs: ADBDocs
        :param s_meta: The RDF Term Metadata associated to **s**.
        :type s_meta: arango_rdf.typings.RDFTermMeta
        :param p: The RDF Predicate URIRef of the statement (s, p, o).
        :type p: URIRef
        :param o_meta: The RDF Term Metadata associated to **o**.
        :type o_meta: arango_rdf.typings.RDFTermMeta
        :param sg_str: The string representation of the sub-graph URIRef associated
            to this statement (if any).
        :type sg_str: str
        :param reified_subject: The RDF Subject of the RDF Statement
            (s, p, o) that was originally in Reified form. Only used
            during `ArangoRDF.__flatten_reified_triples()`.
        :type reified_subject: URIRef | BNode | None
        """
        _, s_col, s_key, _ = s_meta
        _, o_col, o_key, _ = o_meta

        p_str = str(p)
        p_key = self.rdf_id_to_adb_key(p_str)
        p_label = self.rdf_id_to_adb_label(p_str)

        _from = f"{s_col}/{s_key}"
        _to = f"{o_col}/{o_key}"

        if reified_subject:
            e_key = self.rdf_id_to_adb_key(str(reified_subject), reified_subject)
            self.__reified_subject_map[reified_subject] = (_from, p_label, _to)
        else:
            e_key = self.hash(f"{s_key}-{p_key}-{o_key}")

        self.__add_adb_edge(
            adb_docs,
            self.__STATEMENT_COL,
            e_key,
            _from,
            _to,
            p_str,
            p_label,
            sg_str,
        )

    def __rpt_contextualize_statement(
        self,
        adb_docs: ADBDocs,
        s_meta: RDFTermMeta,
        p: URIRef,
        o_meta: RDFTermMeta,
        sg_str: str,
    ) -> None:
        """RDF -> ArangoDB (RPT): Contextualizes the RDF Statement (s, p, o).

        :param adb_docs: The ArangoDB documents buffer to populate.
        :type adb_docs: ADBDocs
        :param s_meta: The RDF Term Metadata associated to **s**.
        :type s_meta: arango_rdf.typings.RDFTermMeta
        :param p: The RDF Predicate URIRef of the statement (s, p, o).
        :type p: URIRef
        :param o_meta: The RDF Term Metadata associated to **o**.
        :type o_meta: arango_rdf.typings.RDFTermMeta
        :param sg_str: The string representation of the sub-graph URIRef associated
            to this statement (if any).
        :type sg_str: str
        """
        p_meta = self.__rpt_process_term(adb_docs, p)
        self.__contextualize_statement(
            adb_docs, s_meta, p_meta, o_meta, sg_str, is_pgt=False
        )

    def __rpt_create_adb_graph(self, name: str) -> ADBGraph:
        """RDF -> ArangoDB (RPT): Create an ArangoDB graph based on
        an RPT Transformation.

        :param name: The ArangoDB Graph name
        :type name: str
        :return: The ArangoDB Graph API wrapper.
        :rtype: arango.graph.Graph
        """

        if self.db.has_graph(name):  # pragma: no cover
            return self.db.graph(name)

        edge_definitions = [
            {
                "edge_collection": self.__STATEMENT_COL,
                "from_vertex_collections": [
                    self.__URIREF_COL,
                    self.__BNODE_COL,
                ],
                "to_vertex_collections": [
                    self.__URIREF_COL,
                    self.__BNODE_COL,
                    self.__LITERAL_COL,
                ],
            }
        ]

        self.__create_graph(name, edge_definitions=edge_definitions)
        return self.db.graph(name)

    ##################################
    # Private: RDF -> ArangoDB (PGT) #
    ##################################

    def __pgt_remove_blacklisted_statements(self) -> None:
        """RDF -> ArangoDB (PGT): Removes RDF Statements that are blacklisted
        from being inserted into the ArangoDB instance.

        Useful for statements that add noise to the data. For example:

        ```
        ex:subject ex:predicate [
            rdf:type rdf:List ;
            rdf:first ex:item1 ;
            rdf:rest [
                rdf:type rdf:List ;
                rdf:first ex:item2 ;
                rdf:rest [
                    rdf:type rdf:List ;
                    rdf:first ex:item3 ;
                    rdf:rest rdf:nil
                ]
            ]
        ] .
        ```

        The above RDF List should simply translate to:

        ```
        subject = {
            "predicate": [item1, item2, item3]
        }
        ```
        """
        rdf_statement_blacklist = {
            (RDF.type, RDF.List),
            (RDF.type, RDF.Bag),
            (RDF.type, RDF.Seq),
        }

        for p, o in rdf_statement_blacklist:
            for s in self.__rdf_graph.subjects(p, o):
                self.__rdf_graph.remove((s, p, o))

    def __pgt_parse_literal_statements(
        self,
        adb_docs: ADBDocs,
        literal_statements: DefaultDict[
            Tuple[RDFTerm, URIRef], List[Tuple[Literal, List[Any]]]
        ],
        pgt_contextualize_statement_func: Callable[..., None],
        batch_size: Optional[int],
        adb_import_kwargs: Dict[str, Any],
    ) -> None:
        """RDF -> ArangoDB (PGT): Pre-processes all RDF Literal statements
        (i.e "Datatype Property Statements") within the RDF Graph.

        Essential for RDF -> ArangoDB (PGT) transformations, as RDF Literals
        are stored as ArangoDB Document properties.

        :param pgt_contextualize_statement_func: A function that contextualizes
            an RDF Statement. A no-op function is used if Graph Contextualization
            is disabled.
        :type pgt_contextualize_statement_func: Callable[..., None]
        :param batch_size: The batch size to use when inserting ArangoDB Documents.
            Defaults to None.
        :type batch_size: int | None
        :param adb_import_kwargs: The keyword arguments to pass to
            `ArangoRDF.__insert_adb_docs()`.
        :type adb_import_kwargs: Dict[str, Any]
        """

        s: RDFTerm
        p: URIRef
        o: Literal

        total = len(literal_statements)
        batch_size = batch_size or total
        bar_progress = self._get_bar_progress("(RDF → ADB): PGT [Literals]", "#EF7D00")
        bar_progress_task = bar_progress.add_task("", total=total)
        spinner_progress = self._get_import_spinner_progress("    ")

        with self._live_context(bar_progress, spinner_progress):
            for i, (k, v) in enumerate(literal_statements.items(), 1):
                s, p = k

                s_meta = self.__pgt_get_term_metadata(s)
                self.__pgt_process_rdf_term(adb_docs, s_meta)

                p_meta = self.__pgt_get_term_metadata(p)
                self.__pgt_process_rdf_term(adb_docs, p_meta)

                _, s_col, s_key, _ = s_meta
                _, _, _, p_label = p_meta

                for o, sg in v:
                    sg_str = self.__get_subgraph_str(sg)

                    o_meta = self.__pgt_get_term_metadata(o)
                    self.__pgt_process_rdf_literal(
                        adb_docs, o, s_col, s_key, p_label, sg_str
                    )

                    pgt_contextualize_statement_func(s_meta, p_meta, o_meta, sg_str)

                if i % batch_size == 0:
                    bar_progress.update(bar_progress_task, advance=batch_size)
                    self.__insert_adb_docs(
                        adb_docs, spinner_progress, **adb_import_kwargs
                    )

            last_advance = total % batch_size if batch_size > 0 else 0
            bar_progress.update(bar_progress_task, advance=last_advance)
            self.__insert_adb_docs(adb_docs, spinner_progress, **adb_import_kwargs)

    def __pgt_parse_non_literal_statements(
        self,
        adb_docs: ADBDocs,
        non_literal_statements: DefaultDict[
            Tuple[RDFTerm, URIRef], List[Tuple[RDFTerm, List[Any]]]
        ],
        contextualize_statement_func: Callable[..., None],
        batch_size: Optional[int],
        adb_import_kwargs: Dict[str, Any],
    ) -> None:
        """RDF -> ArangoDB (PGT): Processes all non-literal RDF statements.

        :param non_literal_statements: Dictionary mapping (s,p) pairs to lists
            of (o, sg) tuples for non-literal objects.
        :type non_literal_statements: DefaultDict[
            Tuple[RDFTerm, URIRef], List[Tuple[RDFTerm, List[Any]]]]
        ]
        :param contextualize_statement_func: A function that contextualizes
            an RDF Statement. A no-op function is used if Graph Contextualization
            is disabled.
        :type contextualize_statement_func: Callable[..., None]
        :param batch_size: The batch size to use when inserting ArangoDB Documents.
            Defaults to None.
        :type batch_size: int | None
        :param adb_import_kwargs: The keyword arguments to pass to
            `ArangoRDF.__insert_adb_docs()`.
        :type adb_import_kwargs: Dict[str, Any]
        """

        s: RDFTerm  # Subject
        p: URIRef  # Predicate
        o: RDFTerm  # Object

        total = len(non_literal_statements)
        batch_size = batch_size or total
        bar_progress = self._get_bar_progress(
            "(RDF → ADB): PGT [Non-Literals]", "#08479E"
        )
        bar_progress_task = bar_progress.add_task("", total=total)
        spinner_progress = self._get_import_spinner_progress("    ")

        with self._live_context(bar_progress, spinner_progress):
            for i, (k, v) in enumerate(non_literal_statements.items(), 1):
                s, p = k

                rdf_list_col = self.__is_rdf_list_statement(s, p)

                if rdf_list_col:
                    doc = self.__rdf_list_data[rdf_list_col][s]
                    predicate_label = self.rdf_id_to_adb_label(str(p))

                    for o, sg in v:
                        self.__pgt_rdf_val_to_adb_val(doc, predicate_label, o)

                else:
                    for o, sg in v:
                        self.__pgt_process_subject_predicate_object(
                            adb_docs, s, p, o, sg, None, contextualize_statement_func
                        )

                if i % batch_size == 0:
                    bar_progress.update(bar_progress_task, advance=batch_size)
                    self.__insert_adb_docs(
                        adb_docs, spinner_progress, **adb_import_kwargs
                    )

            last_advance = total % batch_size if batch_size > 0 else 0
            bar_progress.update(bar_progress_task, advance=last_advance)
            self.__insert_adb_docs(adb_docs, spinner_progress, **adb_import_kwargs)

    def __pgt_process_subject_predicate_object(
        self,
        adb_docs: ADBDocs,
        s: RDFTerm,
        p: URIRef,
        o: RDFTerm,
        sg: Optional[List[Any]],
        reified_subject: Optional[Union[URIRef, BNode]],
        contextualize_statement_func: Callable[..., None],
    ) -> None:
        """RDF -> ArangoDB (PGT): Processes the RDF Statement (s, p, o)
        as an ArangoDB document for PGT.

        :param adb_docs: The ArangoDB documents buffer to populate.
        :type adb_docs: ADBDocs
        :param s: The RDF Subject of the RDF Statement.
        :type s: URIRef | BNode
        :param p: The RDF Predicate of the RDF Statement.
        :type p: URIRef
        :param o: The RDF Object of the RDF Statement.
        :type o: URIRef | BNode | Literal
        :param sg: The Sub Graph URI of the (s,p,o) statement, if any.
        :type sg: URIRef | None
        :param reified_subject: The RDF Subject of the RDF Statement
            (s, p, o) that was originally in Reified form. Only used
            during `ArangoRDF.__flatten_reified_triples()`.
        :type reified_subject: URIRef | BNode | None
        :param contextualize_statement_func: A function that contextualizes
            an RDF Statement. A no-op function is used if Graph Contextualization
            is disabled.
        :type contextualize_statement_func: Callable[..., None]
        """
        sg_str = self.__get_subgraph_str(sg)

        s_meta = self.__pgt_get_term_metadata(s)
        self.__pgt_process_rdf_term(adb_docs, s_meta)

        p_meta = self.__pgt_get_term_metadata(p)
        self.__pgt_process_rdf_term(adb_docs, p_meta)

        o_meta = self.__pgt_get_term_metadata(o)
        self.__pgt_process_object(adb_docs, s_meta, p_meta, o_meta, sg_str)

        self.__pgt_process_statement(
            adb_docs, s_meta, p_meta, o_meta, sg_str, reified_subject
        )

        contextualize_statement_func(s_meta, p_meta, o_meta, sg_str)

    def __pgt_get_term_metadata(self, t: Union[URIRef, BNode, Literal]) -> RDFTermMeta:
        """RDF -> ArangoDB (PGT): Return the following PGT-relevant metadata
        associated to the RDF Term:
            1. The RDF Term (**term**)
            2. The Arangodb Collection of **term**
            3. The Arangodb Key of **term**
            4. The ArangoDB "label" value of **term** (i.e its localname)

        :param t: The RDF Term
        :type t: URIRef | BNode | Literal
        :return: The RDF Term object, along with its associated ArangoDB
            Collection name, Document Key, and Document label.
        :rtype: Tuple[URIRef | BNode | Literal, str, str, str]
        """
        # Quick return for Literals (no caching needed)
        if isinstance(t, Literal):
            return t, "", "", ""  # No other metadata needed

        t_str = str(t)

        if self.enable_pgt_cache:
            if t_str in self.pgt_term_metadata_cache:
                return self.pgt_term_metadata_cache[t_str]

        t_col = ""
        t_key = self.rdf_id_to_adb_key(t_str, t)
        t_label = self.rdf_id_to_adb_label(t_str)

        if data := self.__reified_subject_map.get(t):
            _, p_label, _ = data
            t_col = t_label = p_label

        else:
            t_col = self.__adb_col_statements.value(t, self.adb_col_uri)

            if self.__resource_collection is not None:
                if str(t_col) not in {"Class", "Property"}:
                    t_col = self.__resource_collection.name

            elif self.__uri_map_collection is not None:
                if t_col is None:
                    doc = self.__uri_map_collection.get(t_key)

                    if doc:
                        t_col = str(doc["collection"])
                        self.__add_adb_col_statement(t, t_col)  # for next iteration

            if t_col is None:
                logger.debug(f"Found unknown resource: {t} ({t_key})")
                t_col = self.__UNKNOWN_RESOURCE

        result = t, str(t_col), t_key, t_label
        if self.enable_pgt_cache:
            self.pgt_term_metadata_cache[t_str] = result

        return result

    def __pgt_rdf_val_to_adb_val(
        self,
        doc: Json,
        key: str,
        val: Any,
        process_val_as_serialized_list: bool = False,
    ) -> None:
        """RDF -> ArangoDB (PGT): Insert an arbitrary value into an arbitrary document.

        :param doc: An arbitrary document
        :type doc: Dict[str, Any]
        :param key: An arbitrary document property key.
        :type key: str
        :param val: The value associated to the document property **key**.
        :type val: Any
        :param process_val_as_serialized_list: If enabled, **val** is appended to
            a string representation of the current value of the document
            property. Defaults to False. Only used for
            `ArangoRDF.__pgt_process_rdf_lists()`.
        :type process_val_as_serialized_list: bool
        """
        # Special case for round-tripping
        # See "serialize" option in **list_conversion_mode**
        # and **dict_conversion_mode** (ArangoDB to RDF) for
        # more information.
        try:
            loads_val = json.loads(val)
        except (TypeError, json.JSONDecodeError):
            pass
        else:
            # Only consider the value if it's a list or dict
            if isinstance(loads_val, (list, dict)):
                val = loads_val

        # This flag is only active in ArangoRDF.__pgt_process_rdf_lists()
        if process_val_as_serialized_list:
            doc[key] += f"'{val}'," if isinstance(val, str) else f"{val},"
            return

        prev_val = doc.get(key)

        if prev_val is None:
            doc[key] = val
        elif isinstance(prev_val, list):
            prev_val.append(val)
        else:
            doc[key] = [prev_val, val]

    def __pgt_process_rdf_term(
        self,
        adb_docs: ADBDocs,
        t_meta: RDFTermMeta,
        s_col: str = "",
        s_key: str = "",
        p_label: str = "",
        sg_str: str = "",
        process_val_as_serialized_list: bool = False,
    ) -> None:
        """RDF -> ArangoDB (PGT): Process an RDF Term as an ArangoDB document by PGT.

        :param t_meta: The RDF Term Metadata associated to the RDF Term.
        :type t_meta: arango_rdf.typings.RDFTermMeta
        :param s_col: The ArangoDB document collection of the Subject associated
            to the RDF Term **t**. Only required if the RDF Term is of type Literal.
        :type s_col: str
        :param s_key: The ArangoDB document key of the Subject associated
            to the RDF Term **t**. Only required if the RDF Term is of type Literal.
        :type s_key: str
        :param p_label: The RDF Predicate Label key of the Predicate associated
            to the RDF Term **t**. Only required if the RDF Term is of type Literal.
        :type p_label: str
        :param sg_str: The string representation of the sub-graph URIRef associated
            to the RDF Term **t**.
        :type sg_str: str
        :param process_val_as_serialized_list: If enabled, the value of **t** is
            appended to a string representation of the current value of the document
            property. Only considered if **t** is a Literal. Defaults to False.
        :type process_val_as_serialized_list: bool
        """

        t, t_col, t_key, t_label = t_meta

        if t_key in adb_docs.get(t_col, {}):
            return

        if t in self.__reified_subject_map:
            _from, _, _to = self.__reified_subject_map[t]

            if self.__predicate_collection is not None:
                t_col = self.__predicate_collection.name

            adb_docs[t_col][t_key] = {
                "_key": t_key,
                "_from": _from,
                "_to": _to,
            }

        elif isinstance(t, URIRef):
            adb_docs[t_col][t_key] = {
                "_key": t_key,
                self.__rdf_uri_attr: str(t),
                self.__rdf_label_attr: t_label,
                self.__rdf_type_attr: "URIRef",
            }

            if (
                self.__uri_map_collection is not None
                and t_col != self.__UNKNOWN_RESOURCE
            ):
                uri_col = self.__uri_map_collection.name
                adb_docs[uri_col][t_key] = {
                    "_key": t_key,
                    "collection": t_col,
                    self.__rdf_uri_attr: str(t),
                }

        elif isinstance(t, BNode):
            adb_docs[t_col][t_key] = {
                "_key": t_key,
                self.__rdf_label_attr: "",
                self.__rdf_type_attr: "BNode",
            }

        elif isinstance(t, Literal) and all([s_col, s_key, p_label]):
            self.__pgt_process_rdf_literal(
                adb_docs,
                t,
                s_col,
                s_key,
                p_label,
                sg_str,
                process_val_as_serialized_list,
            )

        else:
            raise ValueError(f"Invalid type for RDF Term: {t}")  # pragma: no cover

    def __pgt_process_rdf_literal(
        self,
        adb_docs: ADBDocs,
        literal: Literal,
        s_col: str,
        s_key: str,
        p_label: str,
        sg_str: str,
        process_val_as_serialized_list: bool = False,
    ) -> None:
        """RDF -> ArangoDB (PGT): Process an RDF Literal as an ArangoDB
        document property.

        :param adb_docs: The ArangoDB documents buffer to populate.
        :type adb_docs: ADBDocs
        :param literal: The RDF Literal to process
        :type literal: Literal
        :param s_col: The ArangoDB document collection of the Subject associated
            to the RDF Literal **literal**.
        :type s_col: str
        :param s_key: The ArangoDB document key of the Subject associated
            to the RDF Literal **literal**.
        :type s_key: str
        :param p_label: The RDF Predicate Label key of the Predicate associated
            to the RDF Literal **literal**.
        :type p_label: str
        :param sg_str: The string representation of the sub-graph URIRef associated
            to the RDF Literal **literal**.
        :type sg_str: str
        :param process_val_as_serialized_list: If enabled, the value of **literal** is
            appended to a string representation of the current value of the document
            property. Defaults to False.
        :type process_val_as_serialized_list: bool
        """
        doc = adb_docs[s_col][s_key]
        val = self.__get_literal_val(literal, str(literal))
        self.__pgt_rdf_val_to_adb_val(doc, p_label, val, process_val_as_serialized_list)

        if sg_str:
            doc[self.__rdf_sub_graph_uri_attr] = sg_str

    def __pgt_process_object(
        self,
        adb_docs: ADBDocs,
        s_meta: RDFTermMeta,
        p_meta: RDFTermMeta,
        o_meta: RDFTermMeta,
        sg_str: str,
    ) -> None:
        """RDF -> ArangoDB (PGT): Processes the RDF Object into ArangoDB.
        Given the possibily of the RDF Object being used as the "root" of
        an RDF Collection or an RDF Container (i.e an RDF List), this wrapper
        function is used to prevent calling `__pgt_process_rdf_term` if it is not
        required.

        :param adb_docs: The ArangoDB documents buffer to populate.
        :type adb_docs: ADBDocs
        :param s_meta: The RDF Term Metadata associated to the
            RDF Subject of the statement containing the RDF Object.
        :type s_meta: arango_rdf.typings.RDFTermMeta
        :param p_meta: The RDF Term Metadata associated to the
            RDF Predicate of the statement containing the RDF Object.
        :type p_meta: arango_rdf.typings.RDFTermMeta
        :param o_meta: The RDF Term Metadata associated to the RDF Object.
        :type o_meta: arango_rdf.typings.RDFTermMeta
        :param sg_str: The string representation of the sub-graph URIRef associated
            to this statement (if any).
        :type sg_str: str
        """

        s, s_col, s_key, _ = s_meta
        p, _, _, p_label = p_meta
        o, _, _, _ = o_meta

        if self.__pgt_object_is_head_of_rdf_list(o):
            head = {"root": o, "sub_graph": sg_str}
            self.__rdf_list_heads[s][p] = head

        else:
            self.__pgt_process_rdf_term(
                adb_docs, o_meta, s_col, s_key, p_label, sg_str=sg_str
            )

    def __pgt_process_statement(
        self,
        adb_docs: ADBDocs,
        s_meta: RDFTermMeta,
        p_meta: RDFTermMeta,
        o_meta: RDFTermMeta,
        sg_str: str,
        reified_subject: Optional[Union[URIRef, BNode]] = None,
    ) -> None:
        """RDF -> ArangoDB (PGT): Processes the RDF Statement (s, p, o) as an
        ArangoDB Edge for PGT.

        An edge is only created if:
            1) The RDF Object within the RDF Statement is not a Literal
            2) The RDF Object is not the "root" node of an RDF List structure

        :param adb_docs: The ArangoDB documents buffer to populate.
        :type adb_docs: ADBDocs
        :param s_meta: The RDF Term Metadata associated to the
            RDF Subject of the statement containing the RDF Object.
        :type s_meta: arango_rdf.typings.RDFTermMeta
        :param p_meta: The RDF Term Metadata associated to the
            RDF Predicate of the statement containing the RDF Object.
        :type p_meta: arango_rdf.typings.RDFTermMeta
        :param o_meta: The RDF Term Metadata associated to the RDF Object.
        :type o_meta: arango_rdf.typings.RDFTermMeta
        :param sg_str: The string representation of the sub-graph URIRef associated
            to this statement (if any).
        :type sg_str: str
        :param reified_subject: The RDF Subject of the RDF Statement
            (s, p, o) that was originally in Reified form. Only used
            during `ArangoRDF.__flatten_reified_triples()`.
        :type reified_subject: URIRef | BNode | None
        """
        o, o_col, o_key, _ = o_meta

        if isinstance(o, Literal) or self.__pgt_object_is_head_of_rdf_list(o):
            return

        _, s_col, s_key, _ = s_meta
        p, _, p_key, p_label = p_meta

        _from = f"{s_col}/{s_key}"
        _to = f"{o_col}/{o_key}"

        # local name of predicate URI is used as the collection name
        # if **predicate_collection_name** is not specified
        e_col = (
            self.__predicate_collection.name
            if self.__predicate_collection is not None
            else p_label
        )

        if reified_subject:
            e_key = self.rdf_id_to_adb_key(str(reified_subject), reified_subject)
            self.__reified_subject_map[reified_subject] = (_from, e_col, _to)
        else:
            e_key = self.hash(f"{s_key}-{p_key}-{o_key}")

        self.__add_adb_edge(
            adb_docs,
            e_col,
            e_key,
            _from,
            _to,
            str(p),
            p_label,
            sg_str,
        )

        self.__e_col_map[e_col]["from"].add(s_col)
        self.__e_col_map[e_col]["to"].add(o_col)

    def __pgt_object_is_head_of_rdf_list(self, o: RDFTerm) -> bool:
        """RDF -> ArangoDB (PGT): Return True if the RDF Object *o*
        is either the "root" node of some RDF Collection or RDF Container
        within the RDF Graph. Essential for unpacking the complicated data
        structure of RDF Lists and re-building them as a JSON List for ArangoDB
        insertion.

        :param o: The RDF Object.
        :type o: URIRef | BNode | Literal
        :return: Whether the object points to an RDF List or not.
        :rtype: bool
        """
        # Quick check: if not a BNode, it can't be an RDF list head
        if not isinstance(o, BNode):
            return False

        # Use pre-computed RDF list subjects for O(1) lookup
        return o in self.__rdf_list_subjects

    def __pgt_process_rdf_lists(
        self, adb_docs: ADBDocs, bar_progress: Progress
    ) -> None:
        """RDF -> ArangoDB (PGT): Process all RDF Collections & Containers
        within the RDF Graph prior to inserting the documents into ArangoDB.

        Given the "linked-list" nature of these RDF Lists, we rely on
        recursion via the `__pgt_process_rdf_list_object`,
        `__pgt_unpack_rdf_collection`, and `__pgt_unpack_rdf_container` functions.

        NOTE: A form of string manipulation is used if Literals are
        present within the RDF List.

        For example:

        `ex:Doc ex:numbers (1 (2 3)) .`

        would be constructed via a string-based solution:

        "[" → "[1" → "[1, [" → "[1, [2," → "[1, [2, 3" → "[1, [2, 3]" → "[1, [2, 3]]"

        I know, it's hacky.
        """
        list_heads = self.__rdf_list_heads.items()
        bar_progress_task = bar_progress.add_task("", total=len(list_heads))

        for s, s_dict in list_heads:
            bar_progress.advance(bar_progress_task)

            s_meta = self.__pgt_get_term_metadata(s)
            _, s_col, s_key, _ = s_meta

            doc = adb_docs[s_col][s_key]
            doc["_key"] = s_key

            for p, p_dict in s_dict.items():
                p_meta = self.__pgt_get_term_metadata(p)
                p_label = p_meta[-1]

                root: RDFTerm = p_dict["root"]
                sg: str = p_dict["sub_graph"]

                doc[p_label] = ""
                self.__pgt_process_rdf_list_object(
                    adb_docs, doc, s_meta, p_meta, root, sg
                )
                doc[p_label] = doc[p_label].rstrip(",")

                # Delete doc[p_key] if there are no Literals within the RDF List
                # TODO: Revisit the possibility of empty collections or containers...
                if set(doc[p_label]) == {"[", "]"}:
                    del doc[p_label]
                else:
                    doc[p_label] = literal_eval(doc[p_label])

    def __pgt_process_rdf_list_object(
        self,
        adb_docs: ADBDocs,
        doc: Json,
        s_meta: RDFTermMeta,
        p_meta: RDFTermMeta,
        o: RDFTerm,
        sg: str,
    ) -> None:
        """RDF -> ArangoDB (PGT): Given an ArangoDB Document, and the
        RDF List Statement represented by **s_meta**, **p_meta**, and **o**,
        process the value of the object **o** into the ArangoDB Document.

        1. If **o** is part of an RDF Collection Data Structure,
        rely on the recursive `__pgt_unpack_rdf_collection` function.

        2. If **o** is part of an RDF Container Data Structure,
        rely on the recursive `__pgt_unpack_rdf_container` function.

        3. If **o** is none of the above, then it is considered
        as a processable entity.

        :param doc: The ArangoDB Document associated to the RDF List.
        :type doc: Dict[str, Any]
        :param s_meta: The RDF Term Metadata associated to the RDF Subject.
        :type s_meta: arango_rdf.typings.RDFTermMeta
        :param p_meta: The RDF Term Metadata associated to the RDF Predicate.
        :type p_meta: arango_rdf.typings.RDFTermMeta
        :param o: The RDF List Object to process into ArangoDB.
        :type o: URIRef | BNode | Literal
        :param sg: The string representation of the sub-graph URIRef associated
            to the RDF List Statement (if any).s
        :type sg: str
        """
        p_label = p_meta[-1]

        if o in self.__rdf_list_data["_COLLECTION_BNODE"]:
            doc[p_label] += "["

            next_bnode_dict = self.__rdf_list_data["_COLLECTION_BNODE"][o]
            self.__pgt_unpack_rdf_collection(
                adb_docs, doc, s_meta, p_meta, next_bnode_dict, sg
            )

            doc[p_label] = doc[p_label].rstrip(",") + "],"

        elif o in self.__rdf_list_data["_CONTAINER_BNODE"]:
            doc[p_label] += "["

            next_bnode_dict = self.__rdf_list_data["_CONTAINER_BNODE"][o]
            self.__pgt_unpack_rdf_container(
                adb_docs, doc, s_meta, p_meta, next_bnode_dict, sg
            )

            doc[p_label] = doc[p_label].rstrip(",") + "],"

        else:
            _, s_col, s_key, _ = s_meta
            o_meta = self.__pgt_get_term_metadata(o)

            # Process the RDF Object as an ArangoDB Document
            self.__pgt_process_rdf_term(
                adb_docs,
                o_meta,
                s_col,
                s_key,
                p_label,
                process_val_as_serialized_list=True,
            )
            # Process the RDF Statement as an ArangoDB Edge
            self.__pgt_process_statement(adb_docs, s_meta, p_meta, o_meta, sg)

    def __pgt_unpack_rdf_collection(
        self,
        adb_docs: ADBDocs,
        doc: Json,
        s_meta: RDFTermMeta,
        p_meta: RDFTermMeta,
        bnode_dict: Dict[str, RDFTerm],
        sg: str,
    ) -> None:
        """RDF -> ArangoDB (PGT): A recursive function that disassembles
        the structure of the RDF Collection, most notably known by its
        `rdf:first` & `rdf:rest` structure.

        :param doc: The ArangoDB Document associated to the RDF Collection.
        :type doc: Dict[str, Any]
        :param s_meta: The RDF Term Metadata associated to the RDF Subject.
        :type s_meta: arango_rdf.typings.RDFTermMeta
        :param p_meta: The RDF Term Metadata associated to the RDF Predicate.
        :type p_meta: arango_rdf.typings.RDFTermMeta
        :param bnode_dict: A dictionary mapping the RDF.First and RDF.Rest
            values associated to the current BNode of the RDF Collection.
        :type bnode_dict: Dict[str, URIRef | BNode | Literal]
        :param sg: The string representation of the sub-graph URIRef associated
            to the RDF List Statement (if any).
        :type sg: str
        """

        first: RDFTerm = bnode_dict["first"]
        self.__pgt_process_rdf_list_object(adb_docs, doc, s_meta, p_meta, first, sg)

        if "rest" in bnode_dict and bnode_dict["rest"] != RDF.nil:
            rest = bnode_dict["rest"]

            next_bnode_dict = self.__rdf_list_data["_COLLECTION_BNODE"][rest]
            self.__pgt_unpack_rdf_collection(
                adb_docs, doc, s_meta, p_meta, next_bnode_dict, sg
            )

    def __pgt_unpack_rdf_container(
        self,
        adb_docs: ADBDocs,
        doc: Json,
        s_meta: RDFTermMeta,
        p_meta: RDFTermMeta,
        bnode_dict: Dict[str, Union[RDFTerm, List[RDFTerm]]],
        sg: str,
    ) -> None:
        """RDF -> ArangoDB (PGT): A recursive function that disassembles
        the structure of the RDF Container, most notably known for its
        `rdf:li` or `rdf:_n` structure.

        :param doc: The ArangoDB Document associated to the RDF Collection.
        :type doc: Dict[str, Any]
        :param s_meta: The RDF Term Metadata associated to the RDF Subject.
        :type s_meta: arango_rdf.typings.RDFTermMeta
        :param p_meta: The RDF Term Metadata associated to the RDF Predicate.
        :type p_meta: arango_rdf.typings.RDFTermMeta
        :param bnode_dict: A dictionary mapping the values associated
            associated to the current BNode of the RDF Container.
        :type bnode_dict: Dict[str, URIRef | BNode | Literal]
        :param sg: The string representation of the sub-graph URIRef associated
            to the RDF List Statement (if any).
        :type sg: str
        """
        # Sort based on the keys within bnode_dict
        for data in sorted(bnode_dict.items()):
            _, value = data  # Fetch the value associated to the current key

            # It is possible for the Container Membership Property
            # to be re-used in multiple statements (e.g rdf:li),
            # hence the reason why `value` can be a list or a single element.
            value_as_list = value if isinstance(value, list) else [value]
            for o in value_as_list:
                self.__pgt_process_rdf_list_object(adb_docs, doc, s_meta, p_meta, o, sg)

    def __pgt_contextualize_statement(
        self,
        adb_docs: ADBDocs,
        s_meta: RDFTermMeta,
        p_meta: RDFTermMeta,
        o_meta: RDFTermMeta,
        sg_str: str,
    ) -> None:
        """RDF -> ArangoDB (PGT): Contextualizes the RDF Statement (s, p, o).

        :param adb_docs: The ArangoDB documents buffer to populate.
        :type adb_docs: ADBDocs
        :param s_meta: The RDF Term Metadata associated to **s**.
        :type s_meta: arango_rdf.typings.RDFTermMeta
        :param p_meta: The RDF Term Metadata associated to **p**.
        :type p_meta: arango_rdf.typings.RDFTermMeta
        :param o_meta: The RDF Term Metadata associated to **o**.
        :type o_meta: arango_rdf.typings.RDFTermMeta
        :param sg_str: The string representation of the sub-graph URIRef associated
            to this statement (if any).
        :type sg_str: str
        """
        self.__contextualize_statement(
            adb_docs, s_meta, p_meta, o_meta, sg_str, is_pgt=True
        )

    def __pgt_create_adb_graph(self, name: str) -> ADBGraph:
        """RDF -> ArangoDB (PGT): Create an ArangoDB graph based
        on a PGT Transformation.

        :param name: The ArangoDB Graph name
        :type name: str
        :return: The ArangoDB Graph API wrapper.
        :rtype: arango.graph.Graph
        """
        edge_definitions: List[Dict[str, Union[str, List[str]]]] = []
        all_v_cols: Set[str] = set()
        non_orphan_v_cols: Set[str] = set()

        if self.__resource_collection is not None:
            all_v_cols.add(self.__resource_collection.name)
            all_v_cols.add("Class")
            all_v_cols.add("Property")
        else:
            for col in self.__adb_col_statements.objects(
                subject=None, predicate=self.adb_col_uri, unique=True
            ):
                all_v_cols.add(str(col))

            # TODO: Revisit the following
            # This discard prevents these collections
            # from appearing as empty collections in the graph
            # (they don't actually hold any documents)
            all_v_cols.discard("Statement")
            all_v_cols.discard("List")

        for e_col, v_cols in self.__e_col_map.items():
            edge_definitions.append(
                {
                    "from_vertex_collections": list(v_cols["from"]),
                    "edge_collection": e_col,
                    "to_vertex_collections": list(v_cols["to"]),
                }
            )

            non_orphan_v_cols |= {
                c for c in v_cols["from"] | v_cols["to"] if c not in self.__e_col_map
            }

        orphan_v_cols = all_v_cols ^ non_orphan_v_cols
        if self.__resource_collection is None:
            orphan_v_cols = orphan_v_cols ^ {self.__UNKNOWN_RESOURCE}

        if not self.db.has_graph(name):
            self.__create_graph(
                name,
                edge_definitions=edge_definitions,
                orphan_collections=list(orphan_v_cols),
            )

            return self.db.graph(name)

        old_edge_definitions = {
            edge_def["edge_collection"]: edge_def
            for edge_def in self.db.graph(name).edge_definitions()
        }

        for e_d in edge_definitions:
            if e_d["edge_collection"] in old_edge_definitions:
                old_e_d = old_edge_definitions[e_d["edge_collection"]]

                from_v_cols = set(e_d["from_vertex_collections"])
                from_v_cols |= set(old_e_d["from_vertex_collections"])

                to_v_cols = set(e_d["to_vertex_collections"])
                to_v_cols |= set(old_e_d["to_vertex_collections"])

                # Update Edge Definition
                self.db.graph(name).replace_edge_definition(
                    e_d["edge_collection"],
                    list(from_v_cols),
                    list(to_v_cols),
                )

            else:
                # Create new Edge Definiton
                self.db.graph(name).create_edge_definition(
                    e_d["edge_collection"],
                    e_d["from_vertex_collections"],
                    e_d["to_vertex_collections"],
                )

        return self.db.graph(name)

    ############################################
    # Private: RDF -> ArangoDB (RPT, PGT, LPG) #
    ############################################

    def __create_collection(self, col: str, edge: bool = False) -> None:
        """RDF -> ArangoDB: Create an ArangoDB Collection."""
        try:
            self.db.create_collection(col, edge=edge)
        except Exception:
            # Collection may have been created by another thread
            if not self.db.has_collection(col):
                raise

    def __create_graph(
        self,
        name: str,
        edge_definitions: List[Dict[str, Any]],
        orphan_collections: List[str] = [],
    ) -> None:
        """RDF -> ArangoDB: Create an ArangoDB Graph."""
        try:
            self.db.create_graph(
                name,
                edge_definitions=edge_definitions,
                orphan_collections=orphan_collections,
            )
        except Exception:
            # Graph may have been created by another thread
            if not self.db.has_graph(name):
                raise

    def __load_meta_ontology(self, rdf_graph: RDFGraph) -> RDFConjunctiveGraph:
        """RDF -> ArangoDB: Load the RDF, RDFS, and OWL
        Ontologies into **rdf_graph** as 3 sub-graphs. This method returns
        an RDF Graph of type rdflib.graph.ConjunctiveGraph in order to support
        sub-graph functionality.

        Essential for Graph Contextualization.

        NOTE: If **rdf_graph** is already of type rdflib.graph.ConjunctiveGraph,
        then the **same** graph is returned (pass by reference).

        :param rdf_graph: The RDF Graph, soon to be converted into an ArangoDB Graph.
        :type rdf_graph: rdflib.graph.Graph
        :return: A ConjunctiveGraph equivalent of **rdf_graph** containing 3
            additional subgraphs (RDF, RDFS, OWL)
        :rtype: rdflib.graph.ConjunctiveGraph
        """

        graph: RDFConjunctiveGraph = (
            rdf_graph
            if isinstance(rdf_graph, RDFConjunctiveGraph)
            else RDFConjunctiveGraph() + rdf_graph
        )

        for ns in os.listdir(f"{PROJECT_DIR}/meta"):
            graph.parse(f"{PROJECT_DIR}/meta/{ns}", format="trig")

        return graph

    def __flatten_reified_triples(
        self,
        adb_docs: ADBDocs,
        process_subject_predicate_object: Callable[..., None],
        contextualize_statement_func: Callable[..., None],
        batch_size: Optional[int],
        adb_import_kwargs: Dict[str, Any],
    ) -> None:
        """RDF -> ArangoDB: Parse all reified triples within the RDF Graph
        if Reified Triple Simplification is enabled.

        NOTE: This modifies the RDF Graph in-place. TODO: Revisit

        NOTE: This function is NOT thread-safe due to thread-safety issues with
        rdflib's SPARQL parser. Therefore it should ONLY be called from a single thread.

        :param process_subject_predicate_object: A function that processes
            the RDF Statement (s, p, o) as an ArangoDB document. Either
            `__rpt_process_subject_predicate_object` or
            `__pgt_process_subject_predicate_object`.
        :type process_subject_predicate_object: Callable[..., None]
        :param contextualize_statement_func: A function that contextualizes
            an RDF Statement. A no-op function is used if Graph Contextualization
            is disabled.
        :type contextualize_statement_func: Callable[..., None]
        """
        graph_supports_quads = isinstance(self.__rdf_graph, RDFConjunctiveGraph)

        # Recursion is used to process nested reified triples
        # Things can get really wild here...
        def process_reified_subject(
            reified_subject: RDFTerm, sg: Optional[List[Any]]
        ) -> None:
            s = self.__rdf_graph.value(reified_subject, RDF.subject)
            p = self.__rdf_graph.value(reified_subject, RDF.predicate)
            o = self.__rdf_graph.value(reified_subject, RDF.object)

            for t in [(s, RDF.type, RDF.Statement), (o, RDF.type, RDF.Statement)]:
                if t in self.__rdf_graph:
                    new_reified_subject = t[0]
                    if graph_supports_quads:
                        new_sg = list(self.__rdf_graph.contexts(t))
                        process_reified_subject(new_reified_subject, new_sg)
                    else:
                        process_reified_subject(new_reified_subject, sg)

            process_subject_predicate_object(
                adb_docs, s, p, o, sg, reified_subject, contextualize_statement_func
            )

            # Remove the reified triple from the RDF Graph
            # once it has been processed
            self.__rdf_graph.remove((reified_subject, RDF.type, RDF.Statement))
            self.__rdf_graph.remove((reified_subject, RDF.subject, s))
            self.__rdf_graph.remove((reified_subject, RDF.predicate, p))
            self.__rdf_graph.remove((reified_subject, RDF.object, o))

        graph_return = ""
        graph_clause = ""
        if graph_supports_quads:
            graph_return = "?g"
            graph_clause = """
                OPTIONAL { GRAPH ?h { ?reified_subject a rdf:Statement . } }
                BIND(IF(BOUND(?h), ?h, iri("")) AS ?g)
                # TODO: Figure out why UNDEF is not working
            """

        query = f"""
            SELECT ?reified_subject {graph_return}
            WHERE {{
                ?reified_subject a rdf:Statement .
                {graph_clause}
            }}
        """

        text = "(RDF → ADB): PGT [Flatten Reified Triples (Query)]"
        with self._get_spinner_progress(text) as sp:
            sp.add_task("")

            data = self.__rdf_graph.query(query)

        total = len(data)
        batch_size = batch_size or total
        m = "(RDF → ADB): Flatten Reified Triples"
        bar_progress = self._get_bar_progress(m, "#FFFFFF")
        bar_progress_task = bar_progress.add_task("", total=total)
        spinner_progress = self._get_import_spinner_progress("    ")

        with self._live_context(bar_progress, spinner_progress):
            for i, (reified_subject, *sg) in enumerate(data, 1):
                # Only process the reified triple if it has not been processed yet
                # i.e recursion
                if reified_subject not in self.__reified_subject_map:
                    process_reified_subject(reified_subject, sg)

                if i % batch_size == 0:
                    bar_progress.update(bar_progress_task, advance=batch_size)
                    self.__insert_adb_docs(
                        adb_docs, spinner_progress, **adb_import_kwargs
                    )

            last_advance = total % batch_size if batch_size > 0 else 0
            bar_progress.update(bar_progress_task, advance=last_advance)
            self.__insert_adb_docs(adb_docs, spinner_progress, **adb_import_kwargs)

    def __get_subgraph_str(self, possible_sg: Optional[List[Any]]) -> str:
        """RDF -> ArangoDB: Extract the sub-graph URIRef string of a quad (if any).

        :param data: The Sub Graph object of a quad (if any).
        :type data: List[URIRef]
        :return: The string representation of the sub-graph URIRef.
        :rtype: str
        """
        if not possible_sg:
            return ""

        sg = possible_sg[0]
        sg_identifier = sg.identifier if isinstance(sg, RDFGraph) else sg

        if isinstance(sg_identifier, URIRef):
            return str(sg_identifier)

        if isinstance(sg_identifier, BNode):
            return ""  # TODO: Revisit

        raise ValueError(f"Sub Graph Identifier is not a URIRef or BNode: {sg}")

    def __add_adb_edge(
        self,
        adb_docs: ADBDocs,
        col: str,
        key: str,
        _from: str,
        _to: str,
        _uri: str,
        _label: str,
        _sg: str,
    ) -> None:
        """RDF -> ArangoDB: Insert the JSON-equivalent of an ArangoDB Edge
        into `adb_docs` for temporary storage, until it gets
        ingested into the **col** ArangoDB Collection.

        :param adb_docs: The ArangoDB documents buffer to populate.
        :type adb_docs: ADBDocs
        :param col: The name of the ArangoDB Edge Collection.
        :type col: str
        :param key: The ArangoDB Key of the Edge.
        :type key: str
        :param _from: The _id of the ArangoDB _from document.
        :type _from: str.
        :param _to: The _id of the ArangoDB _to document.
        :type _to: str.
        :param _uri: The URI string of the RDF Predicate (i.e this edge).
        :type _uri: str
        :param _label: The "local name" of the RDF Predicate.
        :type _label: str
        :param _sg: The URI string of the Sub Graph associated to this edge (if any).
        :type _sg: str
        """

        if self.__predicate_collection is not None:
            col = self.__predicate_collection.name

        adb_docs[col][key] = {
            **adb_docs[col][key],
            "_key": key,
            "_from": _from,
            "_to": _to,
            self.__rdf_uri_attr: _uri,
            self.__rdf_label_attr: _label,
            self.__rdf_type_attr: "URIRef",
        }

        if _sg:
            adb_docs[col][key][self.__rdf_sub_graph_uri_attr] = _sg

    def __build_explicit_type_map(
        self, adb_adb_col_statement: Callable[..., None] = empty_func
    ) -> TypeMap:
        """RDF -> ArangoDB: Build a dictionary mapping the
        (subject rdf:type object) relationships within the RDF Graph.

        Essential for providing Domain & Range Introspection, and essential for
        completing the ArangoDB Collection Mapping Process.

        For example, given the following snippet:
        -----------------------------
        @prefix ex: <http://example.com/> .

        ex:bob rdf:type ex:Person .
        ex:bob rdf:type ex:Parent .
        ex:bob ex:son ex:alex .
        -----------------------------
        The `explicit_type_map` would look like:
        ```
        {
            URIRef("ex:bob"): {"ex:Person", "ex:Parent"},
            URIRef("ex:son"): {"rdf:Property"},
            URIRef("ex:alex"): {}
        }
        ```

        :return: The explicit_type_map dictionary mapping all RDF Statements of
            the form (subject rdf:type object).
        :rtype: DefaultDict[RDFTerm, Set[str]]
        """
        explicit_type_map: TypeMap = defaultdict(set)

        s: URIRef
        p: URIRef
        o: URIRef

        # RDF Type Statements
        for s, o, *_ in self.__rdf_graph[: RDF.type :]:
            explicit_type_map[s].add(str(o))
            adb_adb_col_statement(o, "Class", True)

        # RDF Predicates
        for p in self.__rdf_graph.predicates(unique=True):
            explicit_type_map[p].add(self.__rdf_property_str)
            adb_adb_col_statement(p, "Property", True)

        # RDF Type Statements (Reified)
        for s in self.__rdf_graph[: RDF.predicate : RDF.type]:
            reified_s: URIRef = self.__rdf_graph.value(s, RDF.subject)
            reified_o: URIRef = self.__rdf_graph.value(s, RDF.object)

            explicit_type_map[reified_s].add(str(reified_o))
            adb_adb_col_statement(
                reified_o,
                "Class",
                True,
            )

        # RDF Predicates (Reified)
        for s, o, *_ in self.__rdf_graph[: RDF.predicate :]:
            explicit_type_map[o].add(self.__rdf_property_str)
            adb_adb_col_statement(
                o,
                "Property",
                True,
            )

        return explicit_type_map

    def __build_subclass_tree(
        self, adb_adb_col_statement: Callable[..., None] = empty_func
    ) -> Tree:
        """RDF -> ArangoDB: Build a Tree Data Structure
        representing the `rdfs:subClassOf` Taxonomy of the RDF Graph.

        Essential for providing Domain & Range Introspection, and essential for
        completing the ArangoDB Collection Mapping Process.

        For example, given the following snippet:
        -----------------------------
        @prefix ex: <http://example.com/> .

        ex:Zenkey rdfs:subClassOf :Zebra .
        ex:Zenkey rdfs:subClassOf :Donkey .
        ex:Donkey rdfs:subClassOf :Animal .
        ex:Zebra rdfs:subClassOf :Animal .
        ex:Human rdfs:subClassOf :Animal .
        ex:Animal rdfs:subClassOf :LivingThing .
        ex:LivingThing rdfs:subClassOf :Thing .
        -----------------------------
        The `subclass_tree` would look like:
        ```
        ==================
        |http://www.w3.org/2000/01/rdf-schema#Resource
        |-...
        |-http://www.w3.org/2000/01/rdf-schema#Class
        |-...
        |--...
        |--http://example.com/Thing
        |---http://example.com/LivingThing
        |----http://example.com/Animal
        |-----http://example.com/Donkey
        |------http://example.com/Zenkey
        |-----http://example.com/Human
        |-----http://example.com/Zebra
        |------http://example.com/Zenkey
        ==================
        ```

        :return: The subclass_tree containing the RDFS SubClassOf Taxonomy.
        :rtype: arango_rdf.utils.Tree
        """
        subclass_map: DefaultDict[str, Set[str]] = defaultdict(set)
        if self.__contextualize_graph:
            root_node = Node(self.__rdfs_resource_str)
            subclass_graph = self.__meta_graph + self.__rdf_graph
        else:
            root_node = Node(self.__rdfs_class_str)
            subclass_graph = self.__rdf_graph

        # RDFS SubClassOf Statements
        for s, o, *_ in subclass_graph[: RDFS.subClassOf :]:
            subclass_map[str(o)].add(str(s))

            adb_adb_col_statement(s, "Class", True)
            adb_adb_col_statement(o, "Class", True)

        # RDF SubClassOf Statements (Reified)
        for s in subclass_graph[: RDF.predicate : RDFS.subClassOf]:
            reified_s: URIRef = self.__rdf_graph.value(s, RDF.subject)
            reified_o: URIRef = self.__rdf_graph.value(s, RDF.object)

            subclass_map[str(reified_o)].add(str(reified_s))
            adb_adb_col_statement(reified_s, "Class", True)
            adb_adb_col_statement(reified_o, "Class", True)

        # Connect any 'parent' URIs (i.e URIs that aren't a subclass of another URI)
        # to the RDFS Class URI (prevents having multiple subClassOf taxonomies)
        # Excludes the RDFS Resource URI
        for key in set(subclass_map) - {self.__rdfs_resource_str}:
            if (URIRef(key), RDFS.subClassOf, None) not in subclass_graph:
                # TODO: Consider using OWL:Thing instead of RDFS:Class
                subclass_map[self.__rdfs_class_str].add(key)

        # if root_node not in subclass_map:
        #     subclass_map[self.__rdfs_resource_str].add(self.__rdfs_class_str)

        return Tree(root=root_node, submap=subclass_map)

    def __build_predicate_scope(
        self, adb_adb_col_statement: Callable[..., None] = empty_func
    ) -> PredicateScope:
        """RDF -> ArangoDB: Build a dictionary mapping
        the Domain & Range values of RDF Predicates within `self.__rdf_graph`.

        Essential for providing Domain & Range Inference, and essential for
        completing the ArangoDB Collection Mapping Process.

        For example, given the following snippet:
        --------------------------------
        @prefix ex: <http://example.com/> .

        ex:name rdfs:domain ex:Person .
        ex:son rdfs:domain ex:Parent .
        ex:son rdfs:range ex:Person .
        --------------------------------
        The `predicate_scope` would look like:
        ```
        {
            URIRef("ex:name"): {
                "domain": {("ex:Person", hash("ex:Person")),}
                "range": {}
            },
            URIRef("ex:son"): {
                "domain": {("ex:Parent", hash("ex:Parent)),}
                "range": {("ex:Person", hash("ex:Person")),}
            }
        }
        ```

        :return: The predicate_scope dictionary mapping all predicates within the
            RDF Graph to their respective Domain & Range values..
        :rtype: arango_rdf.typings.PredicateScope
        """
        class_blacklist = [self.__rdfs_literal_str, self.__rdfs_resource_str]

        predicate_scope: PredicateScope = defaultdict(lambda: defaultdict(set))
        predicate_scope_graph = (
            self.__meta_graph + self.__rdf_graph
            if self.__contextualize_graph
            else self.__rdf_graph
        )

        # RDFS Domain & Range
        for label in ["domain", "range"]:
            for p, c in predicate_scope_graph[: RDFS[label] :]:
                class_str = str(c)

                if class_str not in class_blacklist:
                    class_key = self.rdf_id_to_adb_key(class_str)
                    predicate_scope[p][label].add((class_str, class_key))

                adb_adb_col_statement(p, "Property", True)
                adb_adb_col_statement(c, "Class", True)

        # RDFS Domain & Range (Reified)
        for label in ["domain", "range"]:
            t = predicate_scope_graph[: RDF.predicate : RDFS[label]]
            for s in t:
                reified_s: URIRef = self.__rdf_graph.value(s, RDF.subject)
                reified_o: URIRef = self.__rdf_graph.value(s, RDF.object)

                class_str = str(reified_o)

                if class_str not in class_blacklist:
                    class_key = self.rdf_id_to_adb_key(class_str)
                    predicate_scope[reified_s][label].add((class_str, class_key))

                adb_adb_col_statement(reified_s, "Property", True)
                adb_adb_col_statement(reified_o, "Class", True)

        return predicate_scope

    def __build_domain_range_map(self) -> TypeMap:
        """RDF -> ArangoDB: Build a dictionary mapping
        the Domain/Range inference results of all RDF Subjects/Objects
        that are found in an RDF Statement containing a Predicate with a
        defined Domain or Range.

        Essential for completing the ArangoDB Collection Mapping Process.

        For example, given the following snippet:
        ----------------------------------
        @prefix ex: <http://example.com/> .

        ex:bob ex:address "123 Main st" .
        ex:bob ex:son ex:alex .

        ex:address rdfs:domain ex:Entity .
        ex:son rdfs:domain ex:Parent .
        ex:son rdfs:range ex:Person .
        ----------------------------------
        The `domain_range_map` would look like:
        ```
        {
            URIRef("ex:bob"): {"ex:Entity", "ex:Parent"},
            URIRef("ex:alex"): {"ex:Person"}
        }
        ```

        :return: The Domain and Range Mapping
        :rtype: arango_rdf.typings.TypeMap
        """
        domain_range_map: TypeMap = defaultdict(set)

        s: URIRef
        o: URIRef
        for p, scope in self.__predicate_scope.items():
            # RDF Triples
            for s, o, *_ in self.__rdf_graph[:p:]:
                for class_str, _ in scope["domain"]:
                    domain_range_map[s].add(class_str)

                for class_str, _ in scope["range"]:
                    domain_range_map[o].add(class_str)

            # RDF Triples (Reified)
            for s in self.__rdf_graph[: RDF.predicate : p]:
                reified_s: URIRef = self.__rdf_graph.value(s, RDF.subject)
                reified_o: URIRef = self.__rdf_graph.value(s, RDF.object)

                for class_str, _ in scope["domain"]:
                    domain_range_map[reified_s].add(class_str)

                for class_str, _ in scope["range"]:
                    domain_range_map[reified_o].add(class_str)

        return domain_range_map

    def __combine_type_map_and_dr_map(self) -> TypeMap:
        """RDF -> ArangoDB: Combine the results of the
        `__build_explicit_type_map()` & `__build_domain_range_map()` methods.

        Essential for providing Domain & Range Introspection.

        :return: The combined mapping (union) of the two dictionaries provided.
        :rtype: arango_rdf.typings.TypeMap
        """
        type_map: TypeMap = defaultdict(set)

        for key in self.__explicit_type_map.keys() | self.__domain_range_map.keys():
            type_map[key] = self.__explicit_type_map[key] | self.__domain_range_map[key]

        return type_map

    def __get_literal_val(self, t: Literal, t_str: str) -> Any:
        """RDF -> ArangoDB: Extracts a JSON-serializable representation
        of a Literal's value  based on its datatype.

        :param t: The RDF Literal object.
        :type t: Literal
        :param t_str: The string representation of the RDF Literal
        :type t_str: str
        :return: A JSON-serializable value representing the Literal
        :rtype: Any
        """
        if isinstance(t.value, (date, time, Duration)):
            return t_str

        if t.datatype == XSD.decimal:
            return float(t.value)

        return t.value if t.value is not None else t_str

    def __insert_adb_docs(
        self, adb_docs: ADBDocs, spinner_progress: Progress, **adb_import_kwargs: Any
    ) -> None:
        """RDF -> ArangoDB: Insert ArangoDB documents into their ArangoDB collection.

        :param adb_docs: The ArangoDB documents buffer to insert.
        :type adb_docs: ADBDocs
        :param spinner_progress: The spinner progress bar.
        :type spinner_progress: rich.progress.Progress
        :param adb_import_kwargs: Keyword arguments to specify additional
            parameters for ArangoDB document insertion. Full parameter list:
            https://docs.python-arango.com/en/main/specs.html#arango.collection.Collection.insert_many
        :param adb_import_kwargs: Any
        """
        if len(adb_docs) == 0:
            return

        db = self.async_db if self.insert_async else self.db

        if "overwrite_mode" not in adb_import_kwargs:
            adb_import_kwargs["overwrite_mode"] = "update"
        if "merge" not in adb_import_kwargs:
            adb_import_kwargs["merge"] = True
        if "raise_on_document_error" not in adb_import_kwargs:
            adb_import_kwargs["raise_on_document_error"] = True

        # Avoiding "RuntimeError: dictionary changed size during iteration"
        adb_cols = list(adb_docs.keys())

        for col in adb_cols:
            doc_list = adb_docs[col].values()

            action = f"(RDF → ADB): Import '{col}' ({len(doc_list)})"
            spinner_progress_task = spinner_progress.add_task("", action=action)

            if not self.db.has_collection(col):
                is_edge = col in self.__e_col_map
                self.__create_collection(col, edge=is_edge)

            logger.debug(f"Inserting Documents: {doc_list}")

            try:
                result = db.collection(col).insert_many(doc_list, **adb_import_kwargs)
            except Exception as e:
                e_str = str(e)

                logger.error(f"Error inserting documents: {e_str}")
                raise ArangoRDFImportException(e_str, col, list(doc_list))

            logger.debug(f"Insert Result: {result}")

            del adb_docs[col]

            spinner_progress.stop_task(spinner_progress_task)
            spinner_progress.update(spinner_progress_task, visible=False)

    def __contextualize_statement(
        self,
        adb_docs: ADBDocs,
        s_meta: RDFTermMeta,
        p_meta: RDFTermMeta,
        o_meta: RDFTermMeta,
        sg_str: str,
        is_pgt: bool,
    ) -> None:
        """RDF -> ArangoDB: Contextualizes the RDF Statement (s, p, o).

        :param s_meta: The RDF Term Metadata associated to **s**.
        :type s_meta: arango_rdf.typings.RDFTermMeta
        :param p_meta: The RDF Term Metadata associated to **p**.
        :type p_meta: arango_rdf.typings.RDFTermMeta
        :param o_meta: The RDF Term Metadata associated to **o**.
        :type o_meta: arango_rdf.typings.RDFTermMeta
        :param sg_str: The string representation of the sub-graph URIRef associated
            to this statement (if any).
        :type sg_str: str
        :param is_pgt: A flag to identify if this method call originates
            from an PGT process or not.
        :type is_pgt: bool
        """

        p, _, p_key, _ = p_meta

        # Create the <Predicate> <RDF.type> <RDF.Property> ArangoDB Edge
        # p_has_no_type_statement = len(type_map[p]) == 0
        if (p, RDF.type, None) not in self.__rdf_graph:
            edge_col = "type" if is_pgt else self.__STATEMENT_COL
            edge_key = f"{p_key}-{self.__rdf_type_key}-{self.__rdf_property_key}"
            _from_col = "Property" if is_pgt else self.__URIREF_COL
            _to_col = "Class" if is_pgt else self.__URIREF_COL

            self.__add_adb_edge(
                adb_docs,
                col=edge_col,
                key=self.hash(edge_key),
                _from=f"{_from_col}/{p_key}",
                _to=f"{_to_col}/{self.__rdf_property_key}",
                _uri=self.__rdf_type_str,
                _label="type",
                _sg=sg_str,
            )

        # Run RDFS Domain/Range Inference & Introspection
        dr_meta = [(*s_meta, "domain"), (*o_meta, "range")]
        self.__infer_and_introspect_dr(adb_docs, p, p_key, dr_meta, sg_str, is_pgt)

    def __infer_and_introspect_dr(
        self,
        adb_docs: ADBDocs,
        p: URIRef,
        p_key: str,
        dr_meta: List[Tuple[RDFTerm, str, str, str, str]],
        sg_str: str,
        is_pgt: bool,
    ) -> None:
        """RDF -> ArangoDB: Provide Domain/Range (DR) Inference & Introspection
        for the current statement represented by **p** and **dr_meta**

        1. DR Inference: Generate `RDF:type` statements for RDF Resources via the
            `RDFS:Domain` & `RDFS:Range` statements of RDF Predicates.

        2. DR Introspection: Generate `RDFS:Domain` & `RDFS:Range` statements for
            RDF Predicates via the `RDF:type` statements of RDF Resources.

        Uses the following instance variables:
        - self.__type_map: A dictionary mapping the "natural" & "synthetic"
            `RDF.Type` statements of every RDF Resource.
            See `ArangoRDF.__combine_type_map_and_dr_map()` for more info.

        - self.__predicate_scope: A dictionary mapping the Domain & Range
            values of RDF Predicates. See `ArangoRDF.__build_predicate_scope()`
            for more info.

        :param p: The RDF Predicate Object.
        :type p: URIRef
        :param p_key: The ArangoDB Key of the RDF Predicate Object.
        :type p_key: str
        :param dr_meta: The Domain & Range Metadata associated to the
            current (s,p,o) statement.
        :type dr_meta: List[Tuple[URIRef | BNode | Literal, str, str, str]]
        :param sg_str: The string representation of the Sub Graph URI
            of the statement associated to the current predicate **p**.
        :type sg_str: str
        :param is_pgt: A flag to identify if this method call originates
            from an PGT process or not.
        :type is_pgt: bool
        """
        TYPE_COL = "type" if is_pgt else self.__STATEMENT_COL
        CLASS_COL = "Class" if is_pgt else self.__URIREF_COL
        P_COL = "Property" if is_pgt else self.__URIREF_COL

        dr_map = {
            "domain": (self.__rdfs_domain_str, self.__rdfs_domain_key),
            "range": (self.__rdfs_range_str, self.__rdfs_range_key),
        }

        e_col_type = (
            self.__predicate_collection.name
            if self.__predicate_collection is not None and is_pgt
            else "type"
        )

        if is_pgt:
            self.__e_col_map[e_col_type]["to"].add("Class")

        for t, t_col, t_key, t_label, dr_label in dr_meta:
            if isinstance(t, Literal):
                continue

            DR_COL = dr_label if is_pgt else self.__STATEMENT_COL

            # Domain/Range Inference
            # TODO: REVISIT CONDITIONS FOR INFERENCE
            # t_has_no_type_statement = len(type_map[t]) == 0
            t_has_no_type_statement = (t, RDF.type, None) not in self.__rdf_graph
            if t_has_no_type_statement:
                for _, class_key in self.__predicate_scope[p][dr_label]:
                    key = self.hash(f"{t_key}-{self.__rdf_type_key}-{class_key}")
                    self.__add_adb_edge(
                        adb_docs,
                        col=TYPE_COL,
                        key=key,
                        _from=f"{t_col}/{t_key}",
                        _to=f"{CLASS_COL}/{class_key}",
                        _uri=self.__rdf_type_str,
                        _label="type",
                        _sg=sg_str,
                    )

                if is_pgt:
                    self.__e_col_map[e_col_type]["from"].add(t_col)

            # Domain/Range Introspection
            # TODO: REVISIT CONDITIONS FOR INTROSPECTION
            # p_dr_not_in_graph = (p, RDFS[dr_label], None) not in self.__rdf_graph
            # p_dr_not_in_meta_graph = (p, RDFS[dr_label], None) not in self.meta_graph
            p_already_has_dr = dr_label in self.__predicate_scope[p]
            p_used_in_meta_graph = (None, p, None) in self.__meta_graph
            if self.__type_map[t] and not p_already_has_dr and not p_used_in_meta_graph:
                dr_str, dr_key = dr_map[dr_label]

                for class_str in self.__type_map[t]:
                    # TODO: optimize class_key
                    class_key = self.rdf_id_to_adb_key(class_str)
                    key = self.hash(f"{p_key}-{dr_key}-{class_key}")
                    self.__add_adb_edge(
                        adb_docs,
                        col=DR_COL,
                        key=key,
                        _from=f"{P_COL}/{p_key}",
                        _to=f"{CLASS_COL}/{class_key}",
                        _uri=dr_str,
                        _label=dr_label,
                        _sg=sg_str,
                    )

    def __add_adb_col_statement(
        self,
        subject: RDFTerm,
        adb_col: str,
        overwrite: bool = False,
    ) -> None:
        """RDF -> ArangoDB: Add a statement to **self.__adb_col_statements**

        :param subject: The RDF Subject.
        :type subject: URIRef | BNode
        :param adb_col: The ArangoDB Collection name.
        :type adb_col: str
        :param overwrite: If True, delete any existing statements of
            the form (s, URIRef("http://www.arangodb.com/collection"), None).
            Defaults to False.
        :type overwrite: bool
        """
        if overwrite:
            self.__adb_col_statements.remove((subject, self.adb_col_uri, None))

        elif (subject, self.adb_col_uri, None) in self.__adb_col_statements:
            return

        self.__adb_col_statements.add((subject, self.adb_col_uri, Literal(adb_col)))

    #############################
    # Private: ArangoDB <-> RDF #
    #############################

    def __extract_statements(
        self,
        triple: Tuple[RDFTerm, RDFTerm, RDFTerm],
        rdf_graph: RDFGraph,
        keep_triples_in_rdf_graph: bool,
    ) -> RDFGraph:
        """ArangoDB <-> RDF: Extracts statements from an RDF Graph.

        :param triple: The triple to extract from the RDF Graph.
        :type triple: Tuple[RDFTerm, RDFTerm, RDFTerm]
        :param rdf_graph: The RDF Graph to extract the triple from.
        :type rdf_graph: rdflib.graph.Graph
        :param keep_triples_in_rdf_graph: Keep the statements of the form **triple**
            in the original graph once extracted. Defaults to False.
        :type keep_triples_in_rdf_graph: bool
        :return: The ArangoDB Collection Mapping graph.
        :rtype: rdflib.graph.Graph
        """
        extract_graph = RDFGraph()
        extract_graph.bind("adb", self.__adb_ns)

        _, p, _ = triple

        with self._get_spinner_progress(
            f"(RDF ↔ ADB): Extract Statements '{str(p)}'"
        ) as sp:
            sp.add_task("")

            for t in rdf_graph.triples(triple):
                extract_graph.add(t)

        if not keep_triples_in_rdf_graph:
            rdf_graph.remove(triple)

        return extract_graph
