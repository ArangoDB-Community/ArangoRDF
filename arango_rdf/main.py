#!/usr/bin/env python3
import gc
import logging
import os
import re
from ast import literal_eval
from collections import defaultdict
from datetime import date, time
from pathlib import Path
from typing import Any, DefaultDict, Dict, List, Optional, Set, Tuple, Union

from arango.cursor import Cursor
from arango.database import Database
from arango.graph import Graph as ADBGraph
from arango.result import Result
from farmhash import Fingerprint64
from isodate import Duration
from rdflib import RDF, RDFS, XSD, BNode
from rdflib import ConjunctiveGraph as RDFConjunctiveGraph
from rdflib import Dataset as RDFDataset
from rdflib import Graph as RDFGraph
from rdflib import Literal, URIRef
from rich.console import Group
from rich.live import Live

from .abc import Abstract_ArangoRDF
from .typings import (
    ADBDocs,
    ADBMetagraph,
    Json,
    PredicateScope,
    RDFLists,
    RDFObject,
    RDFSubject,
    TermMetadata,
    TypeMap,
)
from .utils import Node, Tree, adb_track, logger, rdf_track

PROJECT_DIR = Path(__file__).parent


class ArangoRDF(Abstract_ArangoRDF):
    """ArangoRDF: Transform RDF Graphs into
    ArangoDB Graphs & vice-versa.

    Implemented using concepts referred in
    https://arxiv.org/abs/2210.05781.

    :param db: A python-arango database instance
    :type db: arango.database.Database
    :param logging_lvl: Defaults to logging.INFO. Other useful options are
        logging.DEBUG (more verbose), and logging.WARNING (less verbose).
    :type logging_lvl: str | int
    :raise TypeError: On invalid parameter types
    """

    def __init__(
        self,
        db: Database,
        logging_lvl: Union[str, int] = logging.INFO,
    ):
        self.set_logging(logging_lvl)

        if not isinstance(db, Database):
            msg = "**db** parameter must inherit from arango.database.Database"
            raise TypeError(msg)

        self.db = db

        # `adb_docs`: An RDF to ArangoDB variable used as a buffer
        # to store the to-be-inserted ArangoDB documents.
        self.adb_docs: ADBDocs

        # `adb_col_uri`: An RDF predicate used to identify
        # the ArangoDB Collection of an arbitrary RDF Resource.
        # e.g (<http://example.com/Bob> <http://www.arangodb.com/collection> "Person")
        self.adb_col_uri = URIRef("http://www.arangodb.com/collection")

        # `adb_key_uri`: An RDF predicate used to identify
        # the ArangoDB Key of an arbitrary RDF Resource.
        # e.g (<http://example.com/Bob> <http://www.arangodb.com/key> "4502")
        self.adb_key_uri = URIRef("http://www.arangodb.com/key")

        # `meta_graph`: An RDF Conjunctive Graph representing the
        # RDF, RDFS, and OWL Ontologies. Each ontology can be found under the
        # arango_rdf/meta directory as a .trig file.
        # Essential for fully contextualizing an RDF Graph in ArangoDB.
        self.meta_graph = RDFConjunctiveGraph()
        for ns in os.listdir(f"{PROJECT_DIR}/meta"):
            self.meta_graph.parse(f"{PROJECT_DIR}/meta/{ns}", format="trig")

        # `rdf_graph`: An instance variable that serves as a shortcut of
        # the current RDF Graph. Updated in ArangoDB-RDF & RDF-ArangoDB methods.
        self.rdf_graph = RDFGraph()

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

        logger.info(f"Instantiated ArangoRDF with database '{db.name}'")

    def set_logging(self, level: Union[int, str]) -> None:
        logger.setLevel(level)

    def __set_iterators(
        self, rdf_iter_text: str, rdf_iter_color: str, adb_iter_text: str
    ) -> None:
        self.__rdf_iterator = rdf_track(rdf_iter_text, rdf_iter_color)
        self.__adb_iterator = adb_track(adb_iter_text)

    ###################################################################################
    # RDF to ArangoDB: RPT Methods
    # 1) rdf_to_arangodb_by_rpt:
    # 2) __rpt_process_term:
    # 3) __rpt_create_adb_graph
    ###################################################################################

    def rdf_to_arangodb_by_rpt(
        self,
        name: str,
        rdf_graph: RDFGraph,
        contextualize_graph: bool = False,
        overwrite_graph: bool = False,
        batch_size: Optional[int] = None,
        **import_options: Any,
    ) -> ADBGraph:
        """Create an ArangoDB Graph from an RDF Graph using
        the RDF-topology-preserving transformation (RPT) Algorithm.

        RPT tries is to preserve the RDF Graph structure by transforming
        each RDF statement into an edge in the Property Graph. More info on
        RPT can be foundin the package's README file, or in the following
        paper: https://arxiv.org/pdf/2210.05781.pdf.

        The `rdf_to_arangodb_by_rpt` method will store the RDF Resources of
        **rdf_graph** under the following ArangoDB Collections:
        - {name}_URIRef: The Document collection for `rdflib.term.URIRef` resources.
        - {name}_BNode: The Document collection for`rdflib.term.BNode` resources.
        - {name}_Literal: The Document collection for `rdflib.term.Literal` resources.
        - {name}_Statement: The Edge collection for all triples/quads.

        :param name: The name of the RDF Graph
        :type name: str
        :param rdf_graph: The RDF Graph object. NOTE: This method does not
            currently support RDF graphs of type `rdflib.graph.Dataset`.
            Apologies for the inconvenience.
        :type: rdf_graph: rdflib.graph.Graph
        :param contextualize_graph: A work-in-progress flag that seeks
            to enhance the Terminology Box of **rdf_graph** by providing
            the following features:
                1) Process RDF Predicates within **rdf_graph** as their own ArangoDB
                    Document, and cast a (predicate RDF.type RDF.Property) edge
                    relationship into the ArangoDB graph for every RDF predicate
                    used in the form (subject predicate object) within **rdf_graph**.
                2) Provide RDFS.Domain & RDFS.Range Inference on all
                    RDF Resources within the **rdf_graph**, so long that no
                    RDF.Type statement already exists in **rdf_graph**
                    for the given resource.
                3) Provide RDFS.Domain & RDFS.Range Introspection on all
                    RDF Predicates with the **rdf_graph**, so long that
                    no RDFS.Domain or RDFS.Range statement already exists
                    for the given predicate.
                4) TODO - What's next?
        :type contextualize_graph: bool
        :param overwrite_graph: Overwrites the ArangoDB graph identified
            by **name** if it already exists, and drops its associated collections.
            Defaults to False.
        :type overwrite_graph: bool
        :param batch_size: If specified, runs the ArangoDB Data Ingestion
            process for every **batch_size** RDF triples/quads within **rdf_graph**.
            Defaults to `len(rdf_graph)`.
        :type batch_size: int | None
        :param import_options: Keyword arguments to specify additional
            parameters for the ArangoDB Data Ingestion process.
            The full parameter list is here:
            https://docs.python-arango.com/en/main/specs.html#arango.collection.Collection.import_bulk
        :type import_options: Any
        :return: The ArangoDB Graph API wrapper.
        :rtype: arango.graph.Graph
        """
        # TODO: REVISIT
        if isinstance(rdf_graph, RDFDataset):
            raise TypeError(  # pragma: no cover
                """
                Invalid type for **rdf_graph**: ArangoRDF does not yet
                support RDF Graphs of type rdflib.graph.Dataset
            """
            )

        self.rdf_graph = rdf_graph

        # Reset the ArangoDB Config
        self.adb_docs = defaultdict(lambda: defaultdict(dict))
        self.__import_options = import_options
        self.__import_options["on_duplicate"] = "update"

        # Set the RPT ArangoDB Collection names
        self.__URIREF_COL = f"{name}_URIRef"
        self.__BNODE_COL = f"{name}_BNode"
        self.__LITERAL_COL = f"{name}_Literal"
        self.__STATEMENT_COL = f"{name}_Statement"

        if overwrite_graph:
            self.db.delete_graph(name, ignore_missing=True, drop_collections=True)

        if contextualize_graph:
            self.rdf_graph = self.load_base_ontology(rdf_graph)
            explicit_type_map = self.__build_explicit_type_map()
            predicate_scope = self.__build_predicate_scope()
            domain_range_map = self.__build_domain_range_map(predicate_scope)
            type_map = self.__combine_type_map_and_dr_map(
                explicit_type_map, domain_range_map
            )

        size = len(self.rdf_graph)
        if batch_size is None:
            batch_size = size

        s: RDFSubject  # Subject
        p: URIRef  # Predicate
        o: RDFObject  # Object
        sg: Optional[RDFGraph]  # Sub Graph

        statements = (
            self.rdf_graph.quads
            if isinstance(self.rdf_graph, RDFConjunctiveGraph)
            else self.rdf_graph.triples
        )

        self.__set_iterators("RDF → ADB (RPT)", "#08479E", "    ")
        with Live(Group(self.__rdf_iterator, self.__adb_iterator)):
            self.__rdf_task = self.__rdf_iterator.add_task("", total=size)

            triple = (None, None, None)  # Iterate through all statements
            enum_statements = enumerate(statements(triple), 1)  # type: ignore[operator]

            for count, (s, p, o, *rest) in enum_statements:
                self.__rdf_iterator.update(self.__rdf_task, advance=1)

                # Skip any ArangoDB Annotation Properties:
                if p == self.adb_key_uri:
                    continue

                # Get the Sub Graph URI (if it exists)
                sg = rest[0] if rest else None
                sg_str = str(sg.identifier) if sg else ""

                # Load the RDF Subject & Object as ArangoDB Documents
                s_col, s_key = self.__rpt_process_term(s)
                o_col, o_key = self.__rpt_process_term(o)

                # Fetch the RDF Predicate metadata
                p_str = str(p)
                p_key = self.rdf_id_to_adb_key(p_str)
                p_label = self.rdf_id_to_adb_label(p_str)

                # Load the RDF triple/quad statement as an ArangoDB Edge
                self.__add_adb_edge(
                    self.__STATEMENT_COL,
                    f"{s_key}-{p_key}-{o_key}",
                    f"{s_col}/{s_key}",
                    f"{o_col}/{o_key}",
                    p_str,
                    p_label,
                    sg_str,
                )

                if contextualize_graph:
                    # Load the RDF Predicate as an ArangoDB Document
                    self.__rpt_process_term(p)

                    # Create the <Predicate> <RDF.type> <RDF.Property> ArangoDB Edge
                    # p_has_no_type_statement = len(type_map[p]) == 0
                    p_has_no_type_statement = (p, RDF.type, None) not in self.rdf_graph
                    if p_has_no_type_statement:
                        key = f"{p_key}-{self.__rdf_type_key}-{self.__rdf_property_key}"
                        self.__add_adb_edge(
                            self.__STATEMENT_COL,
                            key,
                            f"{self.__URIREF_COL}/{p_key}",
                            f"{self.__URIREF_COL}/{self.__rdf_property_key}",
                            self.__rdf_type_str,
                            "type",
                            sg_str,
                        )

                    # Run RDFS Domain/Range Inference & Introspection
                    dr_meta = [
                        (s, s_key, s_col, "domain"),
                        (o, o_key, o_col, "range"),
                    ]

                    self.__infer_and_introspect_dr(
                        p,
                        p_key,
                        dr_meta,
                        type_map,
                        predicate_scope,
                        sg_str,
                        is_rpt=True,
                    )

                # Empty `self.adb_docs` into ArangoDB once `batch_size` has been reached
                if count % batch_size == 0:
                    self.__insert_adb_docs()

            # Insert the remaining `self.adb_docs` into ArangoDB
            self.__insert_adb_docs()

        return self.__rpt_create_adb_graph(name)

    def __rpt_process_term(self, t: Union[RDFSubject, RDFObject]) -> Tuple[str, str]:
        """Process an RDF Term as an ArangoDB document via RPT Standards. Returns the
        ArangoDB Collection & Document Key associated to the RDF term.

        :param t: The RDF Term to process
        :type t: URIRef | BNode | Literal
        :return: The ArangoDB Collection name & Document Key of the RDF Term
        :rtype: Tuple[str, str]
        """

        t_col = ""
        t_str = str(t)
        t_key = self.rdf_id_to_adb_key(t_str, t)

        if type(t) is URIRef:
            t_col = self.__URIREF_COL
            t_label = self.rdf_id_to_adb_label(t_str)

            self.adb_docs[t_col][t_key] = {
                "_key": t_key,
                "_uri": t_str,
                "_label": t_label,
                "_rdftype": "URIRef",
            }

        elif type(t) is BNode:
            t_col = self.__BNODE_COL

            self.adb_docs[t_col][t_key] = {
                "_key": t_key,
                "_label": t_key,  # TODO: REVISIT
                "_rdftype": "BNode",
            }

        elif type(t) is Literal:
            t_col = self.__LITERAL_COL
            t_value = self.__get_literal_val(t, t_str)

            self.adb_docs[t_col][t_key] = {
                "_key": t_key,
                "_value": t_value,
                "_label": t_value,  # TODO: REVISIT
                "_rdftype": "Literal",
            }

            if t.language:
                self.adb_docs[t_col][t_key]["_lang"] = t.language
            elif t.datatype:
                self.adb_docs[t_col][t_key]["_datatype"] = str(t.datatype)

        else:
            raise ValueError()  # pragma: no cover

        return t_col, t_key

    def __rpt_create_adb_graph(self, name: str) -> ADBGraph:
        """Create an ArangoDB graph based on an RPT Transformation.

        :param name: The ArangoDB Graph name
        :type name: str
        :return: The ArangoDB Graph API wrapper.
        :rtype: arango.graph.Graph
        """

        if self.db.has_graph(name):  # pragma: no cover
            return self.db.graph(name)

        return self.db.create_graph(  # type: ignore[return-value]
            name,
            edge_definitions=[
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
            ],
        )

    ###################################################################################
    # RDF to ArangoDB: PGT Methods
    # 1) rdf_to_arangodb_by_pgt:
    # 2) build_adb_mapping_for_pgt:
    # 3) __pgt_get_term_metadata:
    # 4) __pgt_get_term_metadata:
    # 5) __pgt_process_rdf_term:
    # 6) __pgt_rdf_val_to_adb_property:
    # 7) __pgt_process_subject:
    # 8) __pgt_process_object:
    # 9) __pgt_process_predicate:
    # 10) __pgt_object_is_head_of_rdf_list:
    # 11) __pgt_statement_is_part_of_rdf_list:
    # 12) __pgt_process_rdf_lists:
    # 13) __pgt_process_rdf_list_object:
    # 14) __pgt_unpack_rdf_collection:
    # 15) __pgt_unpack_rdf_container:
    # 16) __pgt_create_adb_graph:
    ###################################################################################

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
        """Create an ArangoDB Graph from an RDF Graph using
        the Property Graph Transformation (PGT) Algorithm.

        In contrast to RPT, PGT ensures that datatype property statements are
        mapped to node properties in the PG. More info on PGT can be found
        in the package's README file, or in the following
        paper: https://arxiv.org/pdf/2210.05781.pdf.

        In contrast to RPT, the `rdf_to_arangodb_by_pgt` method will rely on
        the nature of the RDF Resource/Statement to determine which ArangoDB
        Collection it belongs to. The ArangoDB Collection mapping process relies
        on two fundamental URIs:

            1) <http://www.arangodb.com/collection> (adb:collection)
                - Any RDF Statement of the form
                    <http://example.com/Bob> <adb:collection> "Person"
                    will map the Subject to the ArangoDB
                    "Person" document collection.

            2) <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> (rdf:type)
                - This strategy is divided into 3 cases:
                    2.1) If an RDF Resource only has one `rdf:type` statement,
                        then the local name of the RDF Object is used as the ArangoDB
                        Document Collection name. For example,
                        <http://example.com/Bob> <rdf:type> <http://example.com/Person>
                        would create an JSON Document for <http://example.com/Bob>,
                        and place it under the "Person" Document Collection.
                        NOTE: The RDF Object will also have its own JSON Document
                        created, and will be placed under the "Class"
                        Document Collection.

                    2.2) If an RDF Resource has multiple `rdf:type` statements,
                        with some (or all) of the RDF Objects of those statements
                        belonging in an `rdfs:subClassOf` Taxonomy, then the
                        local name of the "most specific" Class within the Taxonomy is
                        used (i.e the Class with the biggest depth). If there is a
                        tie between 2+ Classes, then the URIs are alphabetically
                        sorted & the first one is picked.

                    2.3) If an RDF Resource has multiple `rdf:type` statements, with
                        none of the RDF Objects of those statements belonging in an
                        `rdfs:subClassOf` Taxonomy, then the URIs are
                        alphabetically sorted & the first one is picked. The local
                        name of the selected URI will be designated as the Document
                        collection for that Resource.

            NOTE 1: If `contextualize_graph` is set to True, then additional `rdf:type`
                statements may be generated via ArangoRDF's Domain & Range Inference
                feature. These "synthetic" statements will be considered when mapping
                RDF Resources to the correct ArangoDB Collections, but ONLY if there
                were no "original" rdf:type statements to consider for
                the given RDF Resource.

            NOTE 2: The ArangoDB Collection Mapping algorithm is a Work in Progress,
                and will most likely be subject to change for the time being.

        In contrast to RPT, regardless of whether `contextualize_graph` is set to
        True or not, all RDF Predicates within every RDF Statement in **rdf_graph**
        will be processed as their own ArangoDB Document, and will be stored under
        the "Property" Document Collection.

        ===============================================================================
        To demo the ArangoDB Collection Mapping process,
        let us consider the following RDF Graph
        --------------------------------------------------------------------
        @prefix ex: <http://example.com/> .
        @prefix adb: <http://www.arangodb.com/> .

        ex:B rdfs:subClassOf ex:A .
        ex:C rdfs:subClassOf ex:A .
        ex:D rdfs:subClassOf ex:C .

        ex:alex rdf:type ex:A .

        ex:john rdf:type ex:B .
        ex:john rdf:type ex:D .

        ex:mike rdf:type ex:G
        ex:mike rdf:type ex:F
        ex:mike rdf:type ex:E

        ex:frank adb:collection "Z" .
        ex:frank rdf:type D .

        ex:bob ex:name "Bob" .
        --------------------------------------------------------------------
        Given the RDF TTL Snippet above, we can derive the following
        ArangoDB Collection mappings:

        ex:alex --> "A"
            - This RDF Resource only has one associated `rdf:type` statement.

        ex:john --> "D"
            - This RDF Resource has 2 `rdf:type` statements, but `ex:D` is "deeper"
            than `ex:B` when considering the `rdfs:subClassOf` Taxonomy.

        ex:mike --> "E"
            - This RDF Resource has multiple `rdf:type` statements, with
            none belonging to the `rdfs:subClassOf` Taxonomy.
            Therefore, Alphabetical Sorting is used.

        ex:frank --> "Z"
            - This RDF Resource has an `adb:collection` statement associated
            to it, which is prioritized over any other `rdf:type`
            statement it may have.

        ex:bob --> "UnknownResource"
            - This RDF Resource has neither an `rdf:type` statement
            nor an `adb:collection` statement associated to it. It
            is therefore placed under the "UnknownResource"
            Document Collection.
        ===============================================================================

        :param name: The name of the RDF Graph
        :type name: str
        :param rdf_graph: The RDF Graph object. NOTE: This method does not
            currently support RDF graphs of type `rdflib.graph.Dataset`.
            Apologies for the inconvenience.
        :type: rdf_graph: rdflib.graph.Graph
        :param contextualize_graph: A work-in-progress flag that seeks
            to enhance the Terminology Box of **rdf_graph** by providing
            the following features:
                1) Cast a (predicate RDF.type RDF.Property) edge
                    relationship into the ArangoDB graph for every RDF predicate
                    used in the form (subject predicate object) within **rdf_graph**.
                2) Provide RDFS.Domain & RDFS.Range Inference on all
                    RDF Resources within the **rdf_graph**, so long that no
                    RDF.Type statement already exists in **rdf_graph**
                    for the given resource.
                3) Provide RDFS.Domain & RDFS.Range Introspection on all
                    RDF Predicates with the **rdf_graph**, so long that
                    no RDFS.Domain or RDFS.Range statement already exists
                    for the given predicate.
                4) TODO - What's next?
        :type contextualize_graph: bool
        :param overwrite_graph: Overwrites the ArangoDB graph identified
            by **name** if it already exists, and drops its associated collections.
            Defaults to False.
        :type overwrite_graph: bool
        :param batch_size: If specified, runs the ArangoDB Data Ingestion
            process for every **batch_size** RDF triples/quads within **rdf_graph**.
            Defaults to `len(rdf_graph)`.
        :type batch_size: int | None
        :param adb_mapping: An (optional) RDF Graph containing the ArangoDB
            Collection Mapping statements of all identifiable Resources.
            See `ArangoRDF.build_adb_mapping_for_pgt()` for more info.
        :type adb_mapping: rdflib.graph.Graph | None
        :param import_options: Keyword arguments to specify additional
            parameters for the ArangoDB Data Ingestion process.
            The full parameter list is here:
            https://docs.python-arango.com/en/main/specs.html#arango.collection.Collection.import_bulk
        :return: The ArangoDB Graph API wrapper.
        :rtype: arango.graph.Graph
        """
        # TODO: REVISIT
        if isinstance(rdf_graph, RDFDataset):
            raise TypeError(  # pragma: no cover
                """
                Invalid type for **rdf_graph**: ArangoRDF does not yet
                support RDF Graphs of type rdflib.graph.Dataset
            """
            )

        self.rdf_graph = rdf_graph

        # Reset the ArangoDB Config
        self.adb_docs = defaultdict(lambda: defaultdict(dict))
        self.__import_options = import_options
        self.__import_options["on_duplicate"] = "update"

        # A unique set of instance variable for the PGT Process to
        # convert RDF Lists into JSON Lists
        self.__rdf_lists: RDFLists = defaultdict(lambda: defaultdict(dict))

        # A set of ArangoDB Collections that will NOT imported via
        # batch processing, as they contain documents whose properties
        # are subject to change. For example, an RDF Resource may have
        # multiple Literal statements associated to it.
        self.__adb_col_blacklist: Set[str] = set()

        # The ArangoDB Collection name of all unidentified RDF Resources
        self.__UNKNOWN_RESOURCE = f"{name}_UnknownResource"

        # Builds the ArangoDB Edge Definitions of the (soon to be) ArangoDB Graph
        self.__e_col_map: DefaultDict[str, DefaultDict[str, Set[str]]]
        self.__e_col_map = defaultdict(lambda: defaultdict(set))

        if not contextualize_graph:
            self.adb_mapping = self.build_adb_mapping_for_pgt(
                self.rdf_graph, adb_mapping
            )
        else:
            adb_mapping = adb_mapping or RDFGraph()
            self.rdf_graph = self.load_base_ontology(rdf_graph)
            explicit_type_map = self.__build_explicit_type_map(adb_mapping)
            subclass_tree = self.__build_subclass_tree(adb_mapping)
            predicate_scope = self.__build_predicate_scope(adb_mapping)
            domain_range_map = self.__build_domain_range_map(predicate_scope)
            type_map = self.__combine_type_map_and_dr_map(
                explicit_type_map, domain_range_map
            )

            self.adb_mapping = self.build_adb_mapping_for_pgt(
                self.rdf_graph,
                adb_mapping,
                explicit_type_map,
                subclass_tree,
                predicate_scope,
                domain_range_map,
            )

            self.__e_col_map["type"]["from"].add("Property")
            self.__e_col_map["type"]["from"].add("Class")
            self.__e_col_map["type"]["to"].add("Class")
            for label in ["domain", "range"]:
                self.__e_col_map[label]["from"].add("Property")
                self.__e_col_map[label]["to"].add("Class")

        if overwrite_graph:
            self.db.delete_graph(name, ignore_missing=True, drop_collections=True)

        size = len(self.rdf_graph)
        if batch_size is None:
            batch_size = size

        s: RDFSubject  # Subject
        p: URIRef  # Predicate
        o: RDFObject  # Object
        sg: Optional[RDFGraph]  # Sub Graph

        statements = (
            self.rdf_graph.quads
            if isinstance(self.rdf_graph, RDFConjunctiveGraph)
            else self.rdf_graph.triples
        )

        ############## PGT Processing ##############
        self.__set_iterators("RDF → ADB (PGT)", "#08479E", "    ")
        with Live(Group(self.__rdf_iterator, self.__adb_iterator)):
            self.__rdf_task = self.__rdf_iterator.add_task("", total=size)

            triple = (None, None, None)  # Iterate through all statements
            enum_statements = enumerate(statements(triple), 1)  # type: ignore[operator]

            for count, (s, p, o, *rest) in enum_statements:
                self.__rdf_iterator.update(self.__rdf_task, advance=1)

                # Skip any ArangoDB Annotation Properties:
                if p in [self.adb_col_uri, self.adb_key_uri]:
                    continue

                # Address the possibility of (s, p, o) being a part of the
                # structure of an RDF Collection or an RDF Container.
                rdf_list_col = self.__pgt_statement_is_part_of_rdf_list(s, p)
                if rdf_list_col:
                    key = self.rdf_id_to_adb_label(str(p))
                    doc = self.__rdf_lists[rdf_list_col][s]
                    self.__pgt_rdf_val_to_adb_property(doc, key, val=o)
                    continue

                # Get the Sub Graph URI (if it exists)
                sg = rest[0] if rest else None
                sg_str = str(sg.identifier) if sg else ""

                # Get the PGT metadata associated to each RDF Term
                s_meta = self.__pgt_get_term_metadata(s)
                p_meta = self.__pgt_get_term_metadata(p)
                o_meta = self.__pgt_get_term_metadata(o)

                # Load the RDF Subject & Object as ArangoDB Documents
                self.__pgt_process_subject(s, s_meta)
                self.__pgt_process_object(s, s_meta, p_meta, o, o_meta, sg_str)

                # Load the RDF triple/quad as an ArangoDB Edge
                self.__pgt_process_predicate(s_meta, p_meta, o, o_meta, sg_str)

                # Load the RDF Predicate as an ArangoDB Document
                self.__pgt_process_rdf_term(p, p_meta)

                if contextualize_graph:
                    p_key = p_meta[1]

                    # Create the <Predicate> <RDF.type> <RDF.Property> ArangoDB Edge
                    # p_has_no_type_statement = len(type_map[p]) == 0
                    p_has_no_type_statement = (p, RDF.type, None) not in self.rdf_graph
                    if p_has_no_type_statement:
                        self.__add_adb_edge(
                            "type",
                            f"{p_key}-{self.__rdf_type_key}-{self.__rdf_property_key}",
                            f"Property/{p_key}",
                            f"Class/{self.__rdf_property_key}",
                            self.__rdf_type_str,
                            "type",
                            sg_str,
                        )

                    # Run RDFS Domain/Range Inference & Introspection
                    dr_meta = [
                        (s, s_meta[1], s_meta[2], "domain"),
                        (o, o_meta[1], o_meta[2], "range"),
                    ]

                    self.__infer_and_introspect_dr(
                        p,
                        p_key,
                        dr_meta,
                        type_map,
                        predicate_scope,
                        sg_str,
                        is_rpt=False,
                    )

                # Empty 'self.adb_docs' into ArangoDB once 'batch_size' has been reached
                if count % batch_size == 0:
                    self.__insert_adb_docs(self.__adb_col_blacklist)

        gc.collect()
        ############## ############## ##############

        ############## Post Processing ##############
        self.__set_iterators("RDF → ADB (PGT Post-Process)", "#EF7D00", "    ")
        with Live(Group(self.__rdf_iterator, self.__adb_iterator)):
            # Process `self.__rdf_lists` data into `self.adb_docs`
            self.__pgt_process_rdf_lists()
            # Insert the remaining `self.adb_docs` into ArangoDB
            self.__insert_adb_docs()

        gc.collect()
        ############## ############### ##############

        # Notify user of any RDF Resources that have been
        # unable to be identified during the ArangoDB Collection Mapping process.
        if self.db.has_collection(self.__UNKNOWN_RESOURCE):
            un_count = self.db.collection(self.__UNKNOWN_RESOURCE).count()
            logger.info(
                f"""\n
                ----------------
                Unknown Resource(s) found in graph '{name}'.
                Unable to identify the nature of {un_count} URIRefs & BNodes.
                ----------------
                """
            )

        return self.__pgt_create_adb_graph(name)

    def build_adb_mapping_for_pgt(
        self,
        rdf_graph: RDFGraph,
        adb_mapping: Optional[RDFGraph] = None,
        explicit_type_map: Optional[TypeMap] = None,
        subclass_tree: Optional[Tree] = None,
        predicate_scope: Optional[PredicateScope] = None,
        domain_range_map: Optional[TypeMap] = None,
    ) -> RDFGraph:
        """The PGT Algorithm relies on the ArangoDB Collection Mapping Process to
        identify the ArangoDB Collection of every RDF Resource. Using this method prior
        to running `ArangoRDF.rdf_to_arangodb_by_pgt()` allows users to see the
        (RDF Resource)-to-(ArangoDB Collection) mapping of all of their (identifiable)
        RDF Resources. See the `ArangoRDF.rdf_to_arangodb_by_pgt()` docstring
        for an explanation on the ArangoDB Collection Mapping Process.

        Should a user be interested in making changes to this mapping,
        they are free to do so by modifying the returned RDF Graph.

        Users can then pass the (modified) ADB Mapping back into the
        `ArangoRDF.rdf_to_arangodb_by_pgt()` method to make sure the RDF Resources
        of the RDF Graph are placed in the desired ArangoDB Collections.

        A common use case would look like this:
        ```
        from arango_rdf import ArangoRDF
        from arango import ArangoClient
        from rdflib import Graph

        db = ArangoClient(...)
        adbrdf = ArangoRDF(db)

        g = Graph()
        g.parse('...')

        adb_mapping = adbrdf.build_adb_mapping_for_pgt(g)
        adb_mapping.remove(...)
        adb_mapping.add(...)

        adbrdf.rdf_to_arangodb_by_pgt(
            'PGTGraph', g, contextualize_graph=True, adb_mapping=adb_mapping
        )
        ```

        NOTE: Running this method prior to `ArangoRDF.rdf_to_arangodb_by_pgt()`
        is unnecessary if the user is not interested in
        viewing/modifying the ADB Mapping.

        For example, the `adb_mapping` may look like this:
        -----------------------------------------
        @prefix adb: <http://www.arangodb.com/> .

        <http://example.com/bob> adb:collection "Person" .
        <http://example.com/alex> adb:collection "Person" .
        <http://example.com/name> adb:collection "Property" .
        <http://example.com/Person> adb:collection "Class" .
        <http://example.com/charlie> adb:collection "Dog" .
        -----------------------------------------

        NOTE: There can only be 1 `adb:collection` statement
        associated to each RDF Resource.

        :param rdf_graph: The RDF Graph object.
        :type rdf_graph: rdflib.graph.Graph
        :param adb_mapping: An existing adb_mapping should a user not want to
            see any previous `adb:collection` statements being overwritten by
            the standard ArangoDB Collection Mapping Process.
        :type adb_mapping: rdflib.graph.Graph
        :param explicit_type_map: A dictionary mapping the "natural"
            `RDF.Type` statements of every RDF Resource.
            See `ArangoRDF.__build_explicit_type_map()` for more info.
            NOTE: Users should not use this parameter (internal use only).
        :type explicit_type_map: arango_rdf.typings.TypeMap
        :param subclass_tree: The RDFS SubClassOf Taxonomy represented
            as a Tree Data Structure. See
            `ArangoRDF.__build_subclass_tree()` for more info.
            NOTE: Users should not use this parameter (internal use only).
        :type subclass_tree: arango_rdf.utils.Tree
        :param predicate_scope: A dictionary mapping the Domain & Range values
            of RDF Predicates. See `ArangoRDF.__build_predicate_scope()` for more info.
            NOTE: Users should not use this parameter (internal use only).
        :type predicate_scope: arango_rdf.typings.PredicateScope
        :param domain_range_map: The Domain and Range Map produced by the
            `ArangoRDF.__build_domain_range_map()` method.
            NOTE: Users should not use this parameter (internal use only).
        :type domain_range_map: arango_rdf.typings.TypeMap
        :return: An RDF Graph containing the ArangoDB Collection Mapping
            statements of all identifiable Resources. See the
            `ArangoRDF.rdf_to_arangodb_by_pgt()` docstring for an explanation
            on the ArangoDB Collection Mapping Process.
        :rtype: rdflib.graph.Graph
        """
        self.rdf_graph = rdf_graph

        if adb_mapping is None:
            adb_mapping = RDFGraph()

        adb_mapping.bind("adb", "http://www.arangodb.com/")

        ############################################################
        # 1) RDF.type statements
        ############################################################
        if explicit_type_map is None:
            explicit_type_map = self.__build_explicit_type_map(adb_mapping)

        ############################################################
        # 2) RDF.subClassOf Statements
        ############################################################
        if subclass_tree is None:
            subclass_tree = self.__build_subclass_tree(adb_mapping)

        ############################################################
        # 3) RDFS.subPropertyOf statements
        ############################################################
        for s, o, *_ in self.rdf_graph[: RDFS.subPropertyOf :]:  # type: ignore[misc]
            self.__add_to_adb_mapping(adb_mapping, s, "Property", True)
            self.__add_to_adb_mapping(adb_mapping, o, "Property", True)

        ############################################################
        # 4) Domain & Range Statements
        ############################################################
        if predicate_scope is None:
            predicate_scope = self.__build_predicate_scope(adb_mapping)

        if domain_range_map is None:
            domain_range_map = self.__build_domain_range_map(predicate_scope)

        ############################################################
        # 5) ADB.Collection Statements
        ############################################################
        for s, o, *_ in self.rdf_graph[: self.adb_col_uri :]:  # type: ignore[misc]
            if type(o) is not Literal:
                raise ValueError(f"Object {o} must be Literal")  # pragma: no cover

            if (s, None, None) in adb_mapping and (s, None, o) not in adb_mapping:
                # TODO: Create custom error
                raise ValueError(  # pragma: no cover
                    f"""
                    Subject '{s}' can only have 1 ArangoDB Collection association.
                    Found '{adb_mapping.value(s, self.adb_col_uri)}' and '{str(o)}'.
                    """
                )

            self.__add_to_adb_mapping(adb_mapping, s, str(o))

        ############################################################
        # 6) Finalize **adb_mapping**
        ############################################################
        for rdf_map in [explicit_type_map, domain_range_map]:
            for rdf_resource, class_set in rdf_map.items():
                if (rdf_resource, None, None) in adb_mapping or len(class_set) == 0:
                    continue

                class_str = self.__identify_best_class(class_set, subclass_tree)
                adb_col = self.rdf_id_to_adb_label(class_str)

                self.__add_to_adb_mapping(adb_mapping, rdf_resource, adb_col)

        return adb_mapping

    def __pgt_get_term_metadata(
        self, term: Union[URIRef, BNode, Literal]
    ) -> TermMetadata:
        """Return the following PGT-relevant metadata associated to the RDF Term:
            1. The string representation of the term
            2. The Arangodb Key of the term
            3. The Arangodb Collection of the term
            4. The ArangoDB "label" value of the term (i.e its localname)

        :param term: The RDF Term
        :type term: URIRef | BNode | Literal
        :return: Metadata associated to the RDF Term.
        :rtype: Tuple[str, str, str, str]
        """
        term_str = str(term)

        if type(term) is Literal:
            return term_str, "", "", ""  # No other metadata needed

        term_key = self.rdf_id_to_adb_key(term_str, term)

        term_col = str(
            self.adb_mapping.value(term, self.adb_col_uri) or self.__UNKNOWN_RESOURCE
        )

        term_label = self.rdf_id_to_adb_label(term_str)

        return term_str, term_key, term_col, term_label

    def __pgt_process_rdf_term(
        self,
        t: Union[URIRef, BNode, Literal],
        t_meta: TermMetadata,
        s_key: str = "",
        s_col: str = "",
        p_label: str = "",
        process_val_as_string: bool = False,
    ) -> None:
        """Process an RDF Term as an ArangoDB document by PGT.

        :param t: The RDF Term
        :type t: URIRef | BNode | Literal
        :param t_meta: The PGT Metadata associated to the RDF Term.
        :type t_meta: arango_rdf.typings.TermMetadata
        :param s_key: The ArangoDB document key of the Subject associated
            to the RDF Term **t**. Only required if the RDF Term is of type Literal.
        :type s_key: str
        :param s_col: The ArangoDB document key of the Subject associated
            to the RDF Term **t**. Only required if the RDF Term is of type Literal.
        :type s_col: str
        :param p_label: The RDF Predicate Label key of the Predicate associated
            to the RDF Term **t**. Only required if the RDF Term is of type Literal.
        :type p_label: str
        :param process_val_as_string: If enabled, the value of **t** is appended to
            a string representation of the current value of the document
            property. Only considered if **t** is a Literal. Defaults to False.
        :type process_val_as_string: bool
        """

        t_str, t_key, t_col, t_label = t_meta

        if type(t) is URIRef:
            self.adb_docs[t_col][t_key] = {
                **self.adb_docs[t_col][t_key],
                "_key": t_key,
                "_uri": t_str,
                "_label": t_label,
                "_rdftype": "URIRef",
            }

        elif type(t) is BNode:
            self.adb_docs[t_col][t_key] = {
                **self.adb_docs[t_col][t_key],
                "_key": t_key,
                "_label": t_key,  # TODO: REVISIT
                "_rdftype": "BNode",
            }

        elif type(t) is Literal and all([s_col, s_key, p_label]):
            doc = self.adb_docs[s_col][s_key]
            t_value = self.__get_literal_val(t, t_str)
            self.__pgt_rdf_val_to_adb_property(
                doc, p_label, t_value, process_val_as_string
            )

            self.__adb_col_blacklist.add(s_col)  # TODO: REVISIT

        else:
            raise ValueError()  # pragma: no cover

    def __pgt_rdf_val_to_adb_property(
        self, doc: Json, key: str, val: Any, process_val_as_string: bool = False
    ) -> None:
        """A helper function used to insert an arbitrary value
        into an arbitrary document.

        :param doc: An arbitrary document
        :type doc: Dict[str, Any]
        :param key: An arbitrary document property key.
        :type key: str
        :param val: The value associated to the document property **key**.
        :type val: Any
        :param process_val_as_string: If enabled, **val** is appended to
            a string representation of the current value of the document
            property. Defaults to False.
        :type process_val_as_string: bool
        """

        # This flag is only active in ArangoRDF.__pgt_process_rdf_lists()
        if process_val_as_string:
            doc[key] += f"'{val}'," if type(val) is str else f"{val},"
            return

        try:
            # Assume (1) **key** exists and (2) points to a list
            doc[key].append(val)
        except KeyError:
            # Catch assumption #1
            doc[key] = val
        except AttributeError:
            # Catch assumption #2
            doc[key] = [doc[key], val]

    def __pgt_process_subject(self, s: RDFSubject, s_meta: TermMetadata) -> None:
        """A wrapper over the `__pgt_process_rdf_term` method for easier
        code readability. Processes the RDF Subject into ArangoDB.

        :param s: The RDF Subject to process into ArangoDB
        :type s: URIRef | BNode
        :param s_meta: The PGT Metadata associated to the RDF Subject.
        :type s_meta: arango_rdf.typings.TermMetadata
        """
        self.__pgt_process_rdf_term(s, s_meta)

    def __pgt_process_object(
        self,
        s: RDFSubject,
        s_meta: TermMetadata,
        p_meta: TermMetadata,
        o: RDFObject,
        o_meta: TermMetadata,
        sg_str: str,
    ) -> None:
        """Processes the RDF Object into ArangoDB. Given the possibily of
        the RDF Object being used as the "root" of an RDF Collection or
        an RDF Container (i.e an RDF List), this wrapper function is used
        to prevent calling `__pgt_process_rdf_term` if it is not required.

        :param s: The RDF Subject of the statement (s,p,o).
        :type s: URIRef | BNode
        :param s_meta: The PGT Metadata associated to the
            RDF Subject of the statement containing the RDF Object.
        :type s_meta: arango_rdf.typings.TermMetadata
        :param p_meta: The PGT Metadata associated to the
            RDF Predicate of the statement containing the RDF Object.
        :type p_meta: arango_rdf.typings.TermMetadata
        :param o: The RDF Object of the statement (s,p,o).
        :type o: URIRef | BNode | Literal
        :param o_meta: The PGT Metadata associated to the RDF Object.
        :type o_meta: arango_rdf.typings.TermMetadata
        :param sg_str: The string representation of the sub-graph URIRef associated
            to this statement (if any).
        :type sg_str: str
        """

        p_str, _, _, p_label = p_meta

        if self.__pgt_object_is_head_of_rdf_list(o):
            head = {"root": o, "sub_graph": sg_str}
            self.__rdf_lists["_LIST_HEAD"][s][p_str] = head

        else:
            _, s_key, s_col, _ = s_meta
            self.__pgt_process_rdf_term(o, o_meta, s_key, s_col, p_label)

    def __pgt_process_predicate(
        self,
        s_meta: TermMetadata,
        p_meta: TermMetadata,
        o: RDFObject,
        o_meta: TermMetadata,
        sg: str,
    ) -> None:
        """Processes the RDF Statement as an edge into ArangoDB.

        An edge is only created if:
            1) The RDF Object within the RDF Statement is not a Literal
            2) The RDF Object is not the "root" node of an RDF List structure

        :param s_meta: The PGT Metadata associated to the
            RDF Subject of the statement containing the RDF Object.
        :type s_meta: arango_rdf.typings.TermMetadata
        :param p_meta: The PGT Metadata associated to the
            RDF Predicate of the statement containing the RDF Object.
        :type p_meta: arango_rdf.typings.TermMetadata
        :param o: The RDF Object to process into ArangoDB.
        :type o: URIRef | BNode | Literal
        :param o_meta: The PGT Metadata associated to the RDF Object.
        :type o_meta: arango_rdf.typings.TermMetadata
        :param sg: The string representation of the sub-graph URIRef associated
            to this statement (if any).
        :type sg: str
        """
        if type(o) is Literal or self.__pgt_object_is_head_of_rdf_list(o):
            return

        _, s_key, s_col, _ = s_meta
        p_str, p_key, _, p_label = p_meta
        _, o_key, o_col, _ = o_meta

        self.__add_adb_edge(
            p_label,
            f"{s_key}-{p_key}-{o_key}",
            f"{s_col}/{s_key}",
            f"{o_col}/{o_key}",
            p_str,
            p_label,
            sg,
        )

        self.__e_col_map[p_label]["from"].add(s_col)
        self.__e_col_map[p_label]["to"].add(o_col)

    def __pgt_object_is_head_of_rdf_list(self, o: RDFObject) -> bool:
        """Return True if the RDF Object *o* is either the "root" node
        of some RDF Collection or RDF Container within the RDF Graph.
        Essential for unpacking the complicated data structure of
        RDF Lists and re-building them as a JSON List for ArangoDB insertion.

        :param o: The RDF Object.
        :type o: URIRef | BNode | Literal
        :return: Whether the object points to an RDF List or not.
        :rtype: bool
        """

        first = (o, RDF.first, None)
        rest = (o, RDF.rest, None)

        _n = (o, URIRef(f"{RDF}_1"), None)
        li = (o, URIRef(f"{RDF}li"), None)

        is_head_of_collection = first in self.rdf_graph or rest in self.rdf_graph
        is_head_of_container = _n in self.rdf_graph or li in self.rdf_graph

        return is_head_of_collection or is_head_of_container

    def __pgt_statement_is_part_of_rdf_list(self, s: RDFSubject, p: URIRef) -> str:
        """Return the associated "Document Buffer" key if the RDF Statement
        (s, p, _) is part of an RDF Collection or RDF Container within the RDF Graph.
        Essential for unpacking the complicated data structure of
        RDF Lists and re-building them as an ArangoDB Document Property.

        :param s: The RDF Subject.
        :type s: URIRef | BNode
        :param p: The RDF Predicate.
        :type p: URIRef
        :return: The **self.adb_docs** "Document Buffer" key associated
            to the RDF Statement. If the statement is not part of an RDF
            List, return an empty string.
        :rtype: str
        """
        # TODO: Discuss repurcussions of this assumption
        if type(s) is not BNode:
            return ""

        if p in [RDF.first, RDF.rest]:
            return "_COLLECTION_BNODE"

        p_str = str(p)
        _n = r"^http://www.w3.org/1999/02/22-rdf-syntax-ns#_[0-9]{1,}$"
        li = r"^http://www.w3.org/1999/02/22-rdf-syntax-ns#li$"
        if re.match(_n, p_str) or re.match(li, p_str):
            return "_CONTAINER_BNODE"

        return ""

    def __pgt_process_rdf_lists(self) -> None:
        """A helper function to help process all RDF Collections & Containers
        within the RDF Graph prior to inserting the documents into ArangoDB.

        This function relies on a Dictionary/Linked-List representation of the
        RDF Lists. This representation is stored via the "_LIST_HEAD",
        "_CONTAINER_BNODE", and "_COLLECTION_BNODE" keys within `self.adb_docs`.

        Given the recursive nature of these RDF Lists, we rely on
        recursion via the `__pgt_process_rdf_list_object`,
        `__pgt_unpack_rdf_collection`, and `__pgt_unpack_rdf_container` functions.

        NOTE: A form of string manipulation is used if Literals are
        present within the RDF List. For example, given the RDF Statement
        ```ex:Doc ex:numbers (1 (2 3)) .```, the equivalent ArangoDB List is
        constructed via a string-based solution:
        "[" → "[1" → "[1, [" → "[1, [2," → "[1, [2, 3" → "[1, [2, 3]" → "[1, [2, 3]]"
        """
        list_heads = self.__rdf_lists["_LIST_HEAD"].items()

        self.__rdf_task = self.__rdf_iterator.add_task("", total=len(list_heads))
        for s, s_dict in list_heads:
            self.__rdf_iterator.update(self.__rdf_task, advance=1)

            s_meta = self.__pgt_get_term_metadata(s)
            _, s_key, s_col, _ = s_meta

            for p_str, p_dict in s_dict.items():
                p_meta = self.__pgt_get_term_metadata(URIRef(p_str))
                p_label = p_meta[-1]

                doc = self.adb_docs[s_col][s_key]
                doc["_key"] = s_key  # NOTE: Is this really necessary?

                root: RDFObject = p_dict["root"]
                sg: str = p_dict["sub_graph"]

                doc[p_label] = ""
                self.__pgt_process_rdf_list_object(doc, s_meta, p_meta, root, sg)
                doc[p_label] = doc[p_label].rstrip(",")

                # Delete doc[p_key] if there are no Literals within the RDF List
                if set(doc[p_label]) == {"[", "]"}:
                    del doc[p_label]
                else:
                    doc[p_label] = literal_eval(doc[p_label])

    def __pgt_process_rdf_list_object(
        self,
        doc: Json,
        s_meta: TermMetadata,
        p_meta: TermMetadata,
        o: RDFObject,
        sg: str,
    ) -> None:
        """Given an ArangoDB Document, and the RDF List Statement represented
        by `s_meta, p_meta, o`, process the value of the object **o**
        into the ArangoDB Document.

        If the Object is part of an RDF Collection Data Structure,
        rely on the recursive `__pgt_unpack_rdf_collection` function.

        If the Object is part of an RDF Container Data Structure,
        rely on the recursive `__pgt_unpack_rdf_container` function.

        If the Object is none of the above, then it is considered
        as a processable entity.

        :param doc: The ArangoDB Document associated to the RDF List.
        :type doc: Dict[str, Any]
        :param s_meta: The PGT Metadata associated to the RDF Subject.
        :type s_meta: arango_rdf.typings.TermMetadata
        :param p_meta: The PGT Metadata associated to the RDF Predicate.
        :type p_meta: arango_rdf.typings.TermMetadata
        :param o: The RDF List Object to process into ArangoDB.
        :type o: URIRef | BNode | Literal
        :param sg: The string representation of the sub-graph URIRef associated
            to the RDF List Statement (if any).
        :type sg: str
        """
        p_label = p_meta[-1]

        if o in self.__rdf_lists["_COLLECTION_BNODE"]:
            assert type(o) is BNode

            doc[p_label] += "["

            next_bnode_dict = self.__rdf_lists["_COLLECTION_BNODE"][o]
            self.__pgt_unpack_rdf_collection(doc, s_meta, p_meta, next_bnode_dict, sg)

            doc[p_label] = str(doc[p_label]).rstrip(",") + "],"

        elif o in self.__rdf_lists["_CONTAINER_BNODE"]:
            assert type(o) is BNode

            doc[p_label] += "["

            next_bnode_dict = self.__rdf_lists["_CONTAINER_BNODE"][o]
            self.__pgt_unpack_rdf_container(doc, s_meta, p_meta, next_bnode_dict, sg)

            doc[p_label] = str(doc[p_label]).rstrip(",") + "],"

        else:
            _, s_key, s_col, _ = s_meta
            o_meta = self.__pgt_get_term_metadata(o)

            self.__pgt_process_rdf_term(o, o_meta, s_key, s_col, p_label, True)
            self.__pgt_process_predicate(s_meta, p_meta, o, o_meta, sg)

    def __pgt_unpack_rdf_collection(
        self,
        doc: Json,
        s_meta: TermMetadata,
        p_meta: TermMetadata,
        bnode_dict: Dict[str, RDFObject],
        sg: str,
    ) -> None:
        """A recursive function that disassembles the structure of the
        RDF Collection, most notably known for its "first" & "rest" structure.

        :param doc: The ArangoDB Document associated to the RDF Collection.
        :type doc: Dict[str, Any]
        :param s_meta: The PGT Metadata associated to the RDF Subject.
        :type s_meta: arango_rdf.typings.TermMetadata
        :param p_meta: The PGT Metadata associated to the RDF Predicate.
        :type p_meta: arango_rdf.typings.TermMetadata
        :param bnode_dict: A dictionary mapping the RDF.First and RDF.Rest
            values associated to the current BNode of the RDF Collection.
        :type bnode_dict: Dict[str, URIRef | BNode | Literal]
        :param sg: The string representation of the sub-graph URIRef associated
            to the RDF List Statement (if any).
        :type sg: str
        """

        first: RDFObject = bnode_dict["first"]
        self.__pgt_process_rdf_list_object(doc, s_meta, p_meta, first, sg)

        if "rest" in bnode_dict and bnode_dict["rest"] != RDF.nil:
            rest = bnode_dict["rest"]
            assert type(rest) is BNode

            next_bnode_dict = self.__rdf_lists["_COLLECTION_BNODE"][rest]
            self.__pgt_unpack_rdf_collection(doc, s_meta, p_meta, next_bnode_dict, sg)

    def __pgt_unpack_rdf_container(
        self,
        doc: Json,
        s_meta: TermMetadata,
        p_meta: TermMetadata,
        bnode_dict: Dict[str, Union[RDFObject, List[RDFObject]]],
        sg: str,
    ) -> None:
        """A recursive function that disassembles the structure of the
        RDF Container, most notably known for its linear structure
        (i.e rdf:li & rdf:_n properties)

        :param doc: The ArangoDB Document associated to the RDF Collection.
        :type doc: Dict[str, Any]
        :param s_meta: The PGT Metadata associated to the RDF Subject.
        :type s_meta: arango_rdf.typings.TermMetadata
        :param p_meta: The PGT Metadata associated to the RDF Predicate.
        :type p_meta: arango_rdf.typings.TermMetadata
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
                self.__pgt_process_rdf_list_object(doc, s_meta, p_meta, o, sg)

    def __pgt_create_adb_graph(self, name: str) -> ADBGraph:
        """Create an ArangoDB graph based on a PGT Transformation.

        :param name: The ArangoDB Graph name
        :type name: str
        :return: The ArangoDB Graph API wrapper.
        :rtype: arango.graph.Graph
        """
        if self.db.has_graph(name):  # pragma: no cover
            return self.db.graph(name)

        edge_definitions: List[Dict[str, Union[str, List[str]]]] = []

        all_collections: Set[str] = set()
        orphan_collections: List[str] = []
        non_orphan_collections: Set[str] = set()

        for col in self.adb_mapping.objects(None, self.adb_col_uri, True):
            all_collections.add(str(col))

        for e_col, v_cols in self.__e_col_map.items():
            edge_definitions.append(
                {
                    "from_vertex_collections": list(v_cols["from"]),
                    "edge_collection": e_col,
                    "to_vertex_collections": list(v_cols["to"]),
                }
            )

            non_orphan_collections |= v_cols["from"] | v_cols["to"]

        orphan_collections = list(
            all_collections ^ non_orphan_collections ^ {self.__UNKNOWN_RESOURCE}
        )

        return self.db.create_graph(  # type: ignore[return-value]
            name, edge_definitions, orphan_collections
        )

    ###################################################################################
    # RDF to ArangoDB: RPT & PGT Shared Methods
    # 1) load_meta_ontology
    # 2) load_base_ontology
    # 3) rdf_id_to_adb_key
    # 4) rdf_id_to_adb_label
    # 5) __add_adb_edge:
    # 6) __identify_best_class:
    # 7) __infer_and_introspect_dr:
    # 8) __build_explicit_type_map:
    # 9) __build_subclass_tree:
    # 10) __build_predicate_scope
    # 11) __build_domain_range_map:
    # 12) __combine_type_map_and_dr_map:
    # 13) __get_literal_val:
    # 14) __insert_adb_docs:
    ###################################################################################

    def load_meta_ontology(self, rdf_graph: RDFGraph) -> RDFConjunctiveGraph:
        """An RDF-to-ArangoDB helper method that loads the RDF, RDFS, and OWL
        Ontologies into **rdf_graph** as 3 sub-graphs. This method returns
        an RDF Graph of type rdflib.graph.ConjunctiveGraph in order to support
        sub-graph functionality.

        This method is useful for users who seek to help contextualize their
        RDF Graph within ArangoDB. A common use case would look like this:

        ```
        from arango_rdf import ArangoRDF
        from arango import ArangoClient
        from rdflib import Graph

        db = ArangoClient(...)
        adbrdf = ArangoRDF(db)

        g = Graph()
        g.parse('...')

        cg = adbrdf.load_meta_ontology(g) # Returns a `ConjunctiveGraph`
        adbrdf.rdf_to_arangodb_by_rpt('RPTGraph', cg, contextualize_graph=True)
        adbrdf.rdf_to_arangodb_by_pgt('PGTGraph', cg, contextualize_graph=True)
        ```

        NOTE: If **rdf_graph** is already of type rdflib.graph.ConjunctiveGraph,
        then the **same** graph is returned (pass by reference).

        :param rdf_graph: The RDF Graph, soon to be converted into an ArangoDB Graph.
        :type rdf_graph: rdflib.graph.Graph
        :return: A ConjunctiveGraph equivalent of **rdf_graph** containing 3
            additional subgraphs (RDF, RDFS, OWL)
        :rtype: rdflib.graph.ConjunctiveGraph
        """

        graph: RDFConjunctiveGraph = (
            rdf_graph  # type: ignore[assignment]
            if isinstance(rdf_graph, RDFConjunctiveGraph)
            else RDFConjunctiveGraph() + rdf_graph
        )

        for ns in os.listdir(f"{PROJECT_DIR}/meta"):
            graph.parse(f"{PROJECT_DIR}/meta/{ns}", format="trig")

        return graph

    def load_base_ontology(self, rdf_graph: RDFGraph) -> RDFGraph:
        """An RDF-to-ArangoDB helper method that loads a minimialistic
        t-box into **rdf_graph**.

        This method is called when users choose to set the
        `contextualize_graph` flag to True via any of the two
        `rdf_to_arangodb` methods.

        The "base" t-box triples are:
        1)  <RDFS.Class> <RDF.type> <RDFS.Class>
        2)  <RDF.Property> <RDF.type> <RDFS.Class>
        3)  <RDF.type> <RDF.type> <RDF.Property>
        4)  <RDFS.domain> <RDF.type> <RDF.Property>
        5)  <RDFS.range> <RDF.type> <RDF.Property>

        :param rdf_graph: The RDF Graph, soon to be converted into an ArangoDB Graph.
        :type rdf_graph: rdflib.graph.Graph
        :return: The same **rdf_graph** with an addition of 5 statements
            (at maximum) that make up the "base" t-box required for contextualizing
            an RDF graph into ArangoDB.
        :rtype: rdflib.graph.Graph
        """

        base_ontology = [
            (RDFS.Class, RDF.type, RDFS.Class),
            (RDF.Property, RDF.type, RDFS.Class),
            (RDF.type, RDF.type, RDF.Property),
            (RDFS.domain, RDF.type, RDF.Property),
            (RDFS.range, RDF.type, RDF.Property),
            (self.adb_col_uri, RDF.type, RDF.Property),
            (self.adb_key_uri, RDF.type, RDF.Property),
        ]

        for t in base_ontology:
            # We must make sure that we are not overwriting any quad statements
            if t not in rdf_graph:
                rdf_graph.add(t)

        return rdf_graph

    def rdf_id_to_adb_key(
        self, rdf_id: str, rdf_term: Optional[RDFObject] = None
    ) -> str:
        """Convert an RDF Resource ID string into an ArangoDB Key via
        some hashing function. If **rdf_term** is provided, then the value of
        the statement (rdf_term adb:key "<ArangoDB Document Key>") will be used
        as the ArangoDB Key (assuming that said statement exists).

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
        :param rdf_term: The optional RDF Term to check if it has an
            adb:key statement associated to it.
        :type rdf_term: Optional[URIRef | BNode | Literal]
        :return: The ArangoDB _key equivalent of **rdf_id**
        :rtype: str
        """
        # hash(rdf_id) # NOTE: not platform/session independent!
        # hashlib.md5(rdf_id.encode()).hexdigest()
        # xxhash.xxh64(rdf_id.encode()).hexdigest()
        # mmh3.hash64(rdf_id, signed=False)[0]
        # str(cityhash.CityHash64(item))
        # str(Fingerprint64(rdf_id))

        adb_key = self.rdf_graph.value(rdf_term, self.adb_key_uri)
        return str(adb_key) if adb_key else str(Fingerprint64(rdf_id))

    def rdf_id_to_adb_label(self, rdf_id: str) -> str:
        """Return the suffix of an RDF URI. The suffix can (1)
        be used as an ArangoDB Collection name, or (2) be used as
        the `_label` property value for an ArangoDB Document.
        For example, rdf_id_to_adb_label("http://example.com/Person")
        returns "Person".

        :param rdf_id: The string representation of a URIRef
        :type rdf_id: str
        :return: The suffix of the RDF URI string
        :rtype: str
        """
        return re.split("/|#", rdf_id)[-1] or rdf_id

    def __add_adb_edge(
        self,
        col: str,
        key: str,
        _from: str,
        _to: str,
        _uri: str,
        _label: str,
        _sg: str,
    ) -> None:
        """Insert the JSON-equivalent of an ArangoDB Edge
        into `self.adb_docs` for temporary storage, until it gets
        ingested into the **col** ArangoDB Collection.

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

        if key not in self.adb_docs[col]:
            self.adb_docs[col][key] = {
                "_key": key,
                "_from": _from,
                "_to": _to,
                "_uri": _uri,
                "_label": _label,
            }

        if _sg:
            self.adb_docs[col][key]["_sub_graph_uri"] = _sg

    def __identify_best_class(self, class_set: Set[str], subclass_tree: Tree) -> str:
        """Find the ideal RDFS Class among a selection of RDFS Classes.
        This system is a Work in Progress.

        :param class_set: The set of RDFS Class URIs to choose from.
        :type class_set: Set[str]
        :param subclass_tree: The Tree data structure representing
            the RDFS Taxonomy. See `ArangoRDF.__build_subclass_tree()`
            for more info.
        :type subclass_tree: arango_rdf.utils.Tree
        :return: The most suitable RDFS Class URI among the set of classes.
        :rtype: str
        """
        if len(class_set) == 1:
            return list(class_set)[0]

        elif any([c in subclass_tree for c in class_set]):
            best_class = ""
            best_depth = -1

            for c in sorted(class_set):
                depth = subclass_tree.get_node_depth(c)

                if depth > best_depth:
                    best_depth = depth
                    best_class = c

            return best_class

        else:
            return sorted(class_set)[0]

    def __infer_and_introspect_dr(
        self,
        p: URIRef,
        p_key: str,
        dr_meta: List[Tuple[RDFObject, str, str, str]],
        type_map: TypeMap,
        predicate_scope: PredicateScope,
        sg_str: str,
        is_rpt: bool,
    ) -> None:
        """A helper method shared accross RDF to ArangoDB RPT & PGT to provide
        Domain/Range (DR) Inference & Introspection.

        DR Inference: Generate `RDF:type` statements for RDF Resources via the
            `RDFS:Domain` & `RDFS:Range` statements of RDF Predicates.

        DR Introspection: Generate `RDFS:Domain` & `RDFS:Range` statements for
            RDF Predicates via the `RDF:type` statements of RDF Resources.

        :param p: The RDF Predicate Object.
        :type p: URIRef
        :param p_key: The ArangoDB Key of the RDF Predicate Object.
        :type p_key: str
        :param dr_meta: The Domain & Range Metadata associated to the
            current (s,p,o) statement.
            i.e [(s, s_key, s_col, "domain"), (o, o_key, o_col, "range")].
        :type dr_meta: List[Tuple[URIRef | BNode | Literal, str, str, str]]
        :param type_map: A dictionary mapping the "natural" & "synthetic"
            `RDF.Type` statements of every RDF Resource.
            See `ArangoRDF.__combine_type_map_and_dr_map()` for more info.
        :type type_map: arango_rdf.typings.TypeMap
        :param predicate_scope: A dictionary mapping the Domain & Range
            values of RDF Predicates. See `ArangoRDF.__build_predicate_scope()`
            for more info.
        :type predicate_scope: arango_rdf.typings.PredicateScope
        :param sg_str: The string representation of the Sub Graph URI
            of the statement associated to the current predicate **p**.
        :type sg_str: str
        :param is_rpt: A flag to identify if this method call originates
            from an RPT process or not.
        :type is_rpt: bool
        """
        if is_rpt:
            TYPE_COL = self.__STATEMENT_COL
            CLASS_COL = P_COL = self.__URIREF_COL
        else:
            TYPE_COL = "type"
            CLASS_COL = "Class"
            P_COL = "Property"

        dr_map = {
            "domain": (self.__rdfs_domain_str, self.__rdfs_domain_key),
            "range": (self.__rdfs_range_str, self.__rdfs_range_key),
        }

        for t, t_key, t_col, dr_label in dr_meta:
            if isinstance(t, Literal):
                continue

            DR_COL = self.__STATEMENT_COL if is_rpt else dr_label

            # Domain/Range Inference
            # TODO: REVISIT CONDITIONS FOR INFERENCE
            # t_has_no_type_statement = len(type_map[t]) == 0
            t_has_no_type_statement = (t, RDF.type, None) not in self.rdf_graph
            if t_has_no_type_statement:
                for _, class_key in predicate_scope[p][dr_label]:
                    self.__add_adb_edge(
                        TYPE_COL,
                        f"{t_key}-{self.__rdf_type_key}-{class_key}",
                        f"{t_col}/{t_key}",
                        f"{CLASS_COL}/{class_key}",
                        self.__rdf_type_str,
                        "type",
                        sg_str,
                    )

                if not is_rpt:
                    self.__e_col_map["type"]["from"].add(t_col)
                    self.__e_col_map["type"]["to"].add("Class")

            # Domain/Range Introspection
            # TODO: REVISIT CONDITIONS FOR INTROSPECTION
            # p_dr_not_in_graph = (p, RDFS[dr_label], None) not in self.rdf_graph
            # p_dr_not_in_meta_graph = (p, RDFS[dr_label], None) not in self.meta_graph
            p_already_has_dr = dr_label in predicate_scope[p]
            p_used_in_meta_graph = (None, p, None) in self.meta_graph
            if type_map[t] and not p_already_has_dr and not p_used_in_meta_graph:
                dr_str, dr_key = dr_map[dr_label]

                for class_str in type_map[t]:
                    # TODO: optimize class_key
                    class_key = self.rdf_id_to_adb_key(class_str)
                    self.__add_adb_edge(
                        DR_COL,
                        f"{p_key}-{dr_key}-{class_key}",
                        f"{P_COL}/{p_key}",
                        f"{CLASS_COL}/{class_key}",
                        dr_str,
                        dr_label,
                        sg_str,
                    )

    def __build_explicit_type_map(
        self, adb_mapping: Optional[RDFGraph] = None
    ) -> TypeMap:
        """An RPT/PGT helper method used to build a dictionary mapping
        the (subject rdf:type object) relationships within the RDF Graph.

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

        :param adb_mapping: The ADB Mapping of the current (RDF to
            ArangoDB) PGT Process. If not specified, then it is implied that
            this method was called from an RPT context.
        :type adb_mapping: rdflib.graph.Graph | None
        :return: The explicit_type_map dictionary mapping all RDF Statements of
            the form (subject rdf:type object).
        :rtype: DefaultDict[RDFSubject, Set[str]]
        """
        explicit_type_map: TypeMap = defaultdict(set)

        for s, o, *_ in self.rdf_graph[: RDF.type :]:  # type: ignore[misc]
            explicit_type_map[s].add(str(o))

            if adb_mapping is not None:
                self.__add_to_adb_mapping(adb_mapping, o, "Class", True)

        for p in self.rdf_graph.predicates(subject=None, object=None, unique=True):
            assert type(p) is URIRef
            explicit_type_map[p].add(self.__rdf_property_str)

            if adb_mapping is not None:
                self.__add_to_adb_mapping(adb_mapping, p, "Property", True)

        return explicit_type_map

    def __build_subclass_tree(self, adb_mapping: Optional[RDFGraph] = None) -> Tree:
        """An RPT/PGT helper method used to build a Tree Data Structure
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

        :param adb_mapping: The ADB Mapping of the current (RDF to
            ArangoDB) PGT Process. If not specified, then it is implied that
            this method was called from an RPT context.
        :type adb_mapping: rdflib.graph.Graph | None
        :return: The subclass_tree containing the RDFS SubClassOf Taxonomy.
        :rtype: arango_rdf.utils.Tree
        """
        subclass_map: DefaultDict[str, Set[str]] = defaultdict(set)

        subclass_graph = self.meta_graph + self.rdf_graph
        for s, o, *_ in subclass_graph[: RDFS.subClassOf :]:  # type: ignore[misc]
            subclass_map[str(o)].add(str(s))

            if adb_mapping is not None:
                self.__add_to_adb_mapping(adb_mapping, s, "Class", True)
                self.__add_to_adb_mapping(adb_mapping, o, "Class", True)

        for key in set(subclass_map) ^ {self.__rdfs_resource_str}:
            if (URIRef(key), RDFS.subClassOf, None) not in subclass_graph:
                subclass_map[self.__rdfs_class_str].add(key)

        root = Node(self.__rdfs_resource_str)

        return Tree(root, subclass_map)

    def __build_predicate_scope(
        self, adb_mapping: Optional[RDFGraph] = None
    ) -> PredicateScope:
        """An RPT/PGT helper method used to build a dictionary mapping
        the Domain & Range values of RDF Predicates within `self.rdf_graph`.

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

        :param adb_mapping: The ADB Mapping of the current (RDF to
            ArangoDB) PGT Process. If not specified, then it is implied that
            this method was called from an RPT context.
        :type adb_mapping: rdflib.graph.Graph | None
        :return: The predicate_scope dictionary mapping all predicates within the
            RDF Graph to their respective Domain & Range values..
        :rtype: arango_rdf.typings.PredicateScope
        """
        class_blacklist = [self.__rdfs_literal_str, self.__rdfs_resource_str]

        predicate_scope: PredicateScope = defaultdict(lambda: defaultdict(set))
        predicate_scope_graph = self.meta_graph + self.rdf_graph
        for label in ["domain", "range"]:
            for p, c in predicate_scope_graph[: RDFS[label] :]:  # type:ignore[misc]
                class_str = str(c)

                if class_str not in class_blacklist:
                    class_key = self.rdf_id_to_adb_key(class_str)
                    predicate_scope[p][label].add((class_str, class_key))

                if adb_mapping is not None:
                    self.__add_to_adb_mapping(adb_mapping, p, "Property", True)
                    self.__add_to_adb_mapping(adb_mapping, c, "Class", True)

        return predicate_scope

    def __build_domain_range_map(self, predicate_scope: PredicateScope) -> TypeMap:
        """An RPT/PGT helper method used to build a dictionary mapping
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

        :param predicate_scope: The mapping of RDF Predicates to their
            respective domain/range values.
        :type predicate_scope: arango_rdf.typings.PredicateScope
        :return: The Domain and Range Mapping
        :rtype: arango_rdf.typings.TypeMap
        """
        domain_range_map: TypeMap = defaultdict(set)

        s: URIRef
        o: URIRef
        for p, scope in predicate_scope.items():
            t = (None, p, None)
            for s, _, o, *_ in self.rdf_graph.triples(t):  # type: ignore[assignment]
                for class_str, _ in scope["domain"]:
                    domain_range_map[s].add(class_str)

                for class_str, _ in scope["range"]:
                    domain_range_map[o].add(class_str)

        return domain_range_map

    def __combine_type_map_and_dr_map(
        self,
        explicit_type_map: TypeMap,
        domain_range_map: TypeMap,
    ) -> TypeMap:
        """An RPT/PGT helper method used to combine the results of the
        `__build_explicit_type_map()` & `__build_domain_range_map()` methods.

        Essential for providing Domain & Range Introspection.

        :param explicit_type_map: The Explicit Type Map produced by the
            `ArangoRDF.__build_explicit_type_map()` method.
        :type explicit_type_map: arango_rdf.typings.TypeMap
        :param domain_range_map: The Domain and Range Map produced by the
            `ArangoRDF.__build_domain_range_map()` method.
        :type domain_range_map: arango_rdf.typings.TypeMap
        :return: The combined mapping (union) of the two dictionaries provided.
        :rtype: arango_rdf.typings.TypeMap
        """
        type_map: TypeMap = defaultdict(set)

        for key in explicit_type_map.keys() | domain_range_map.keys():
            type_map[key] = explicit_type_map[key] | domain_range_map[key]

        return type_map

    def __get_literal_val(self, t: Literal, t_str: str) -> Any:
        """Extracts a JSON-serializable representation
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

        return t.value if t.value else t_str

    def __insert_adb_docs(self, adb_col_blacklist: Set[str] = set()) -> None:
        """Insert ArangoDB documents into their ArangoDB collection.

        :param adb_col_blacklist: A list of ArangoDB Collections that will not be
            populated on this call of `__insert_adb_docs()`. Essential for allowing List
            construction of RDF Literals (PGT Only).
        :type adb_col_blacklist: Set[str]
        """

        # Little hack to avoid "RuntimeError: dictionary changed size during iteration"
        for adb_col in list(self.adb_docs.keys()):
            if adb_col in adb_col_blacklist:
                continue

            action = f"ArangoDB Import: {adb_col}"
            adb_task = self.__adb_iterator.add_task("", action=action)

            docs = list(self.adb_docs[adb_col].values())
            if len(docs) == 0:
                continue  # pragma: no cover

            if not self.db.has_collection(adb_col):
                is_edge = {"_from", "_to"} <= docs[0].keys()
                self.db.create_collection(adb_col, edge=is_edge)

            self.db.collection(adb_col).import_bulk(docs, **self.__import_options)
            del self.adb_docs[adb_col]  # Clear buffer

            self.__adb_iterator.stop_task(adb_task)
            self.__adb_iterator.update(adb_task, visible=False)

        gc.collect()

    ###################################################################################
    # ArangoDB to RDF Methods
    # 1) arangodb_to_rdf:
    # 2) arangodb_collections_to_rdf:
    # 3) arangodb_graph_to_rdf:
    # 4) __process_adb_doc:
    # 5) __add_to_rdf_graph:
    # 6) __adb_property_to_rdf_val:
    # 7) __fetch_adb_docs:
    ###################################################################################

    def arangodb_to_rdf(
        self,
        name: str,
        rdf_graph: RDFGraph,
        metagraph: ADBMetagraph,
        list_conversion_mode: str = "static",
        infer_type_from_adb_col: bool = False,
        include_adb_key_statements: bool = False,
        **export_options: Any,
    ) -> Tuple[RDFGraph, RDFGraph]:
        """Create an RDF Graph from an ArangoDB Graph via its Metagraph.

        :param name: The name of the ArangoDB Graph
        :type name: str
        :param rdf_graph: The target RDF Graph to insert into.
        :type rdf_graph: rdflib.graph.Graph
        :param metagraph: An dictionary of dictionaries defining the ArangoDB Vertex
            & Edge Collections whose entries will be inserted into the RDF Graph.
        :type metagraph: arango_rdf.typings.ADBMetagraph
        :param list_conversion_mode: Specify how ArangoDB JSON lists
            are handled andprocessed into the RDF Graph. If "collection", ArangoDB
            lists will be processed using the RDF Collection structure. If "container",
            lists found within the ArangoDB Graph will be processed using the
            RDF Container structure. If "static", elements within lists will be
            processed as individual statements. Defaults to "static".
        :type list_conversion_mode: str
        :param infer_type_from_adb_col: Specify whether `rdf:type` relationships
            of the form (resource rdf:type adb_col) should be inferred upon
            transferring ArangoDB Documents into RDF. NOTE: Enabling this flag
            is only recommended if your ArangoDB graph is "native" to ArangoDB.
            That is, the ArangoDB graph does not originate from an RDF context.
        :type infer_type_from_adb_col: bool
        :param include_adb_key_statements: Specify whether `adb:key` relationships
            of the form (resource adb:key adb_doc_key) should be generated upon
            transferring ArangoDB Documents into RDF. This can be used to
            maintain document keys when a user is interested in round-tripping.
            NOTE: Currently only applies to ArangoDB Vertices (not edges).
            NOTE: Enabling this flag is only recommended if your ArangoDB graph
            is "native" to ArangoDB. That is, the ArangoDB graph does not
            originate from an RDF context.
        :type include_adb_key_statements: bool
        :param export_options: Keyword arguments to specify AQL query options when
            fetching documents from the ArangoDB instance. Full parameter list:
            https://docs.python-arango.com/en/main/specs.html#arango.aql.AQL.execute
        :type export_options: Any
        :return: The RDF representation of the ArangoDB Graph, along with a second
            RDF Graph mapping the RDF Resources to their designated ArangoDB Collection.
            The second graph, **adb_mapping**, can then be re-used in the RDF to
            ArangoDB (PGT) process to maintain the Document-to-Collection mappings.
        :rtype: Tuple[rdflib.graph.Graph, rdflib.graph.Graph]
        """

        self.rdf_graph = rdf_graph
        graph_supports_quads = isinstance(self.rdf_graph, RDFConjunctiveGraph)

        self.__export_options = export_options
        self.__list_conversion = list_conversion_mode
        self.__adb_graph_ns = f"{self.db._conn._url_prefixes[0]}/{name}#"

        # Maps the (soon-to-be) RDF Resources to their ArangoDB Collection
        adb_mapping: RDFGraph = RDFGraph()
        adb_mapping.bind("adb", "http://www.arangodb.com/")

        # Maps RDF Resources to the last Sub Graph that they been seen in (if any)
        subgraph_map: Dict[str, URIRef] = {}

        # Maps ArangoDB Document IDs to RDFLib Terms (i.e URIRef, Literal, BNode)
        term_map: Dict[str, Union[RDFSubject, RDFObject]] = {}

        # Maps ArangoDB Document IDs to URI strings
        self.__uri_map: Dict[str, str] = {}

        # TODO: REVISIT
        self.rdf_graph.bind(name, self.__adb_graph_ns)
        self.rdf_graph.bind("adb", "http://www.arangodb.com/")
        self.rdf_graph.bind("owl", "http://www.w3.org/2002/07/owl#")
        self.rdf_graph.bind("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
        self.rdf_graph.bind("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
        self.rdf_graph.bind("xml", "http://www.w3.org/XML/1998/namespace")
        self.rdf_graph.bind("xsd", "http://www.w3.org/2001/XMLSchema#")
        self.rdf_graph.bind("dc", "http://purl.org/dc/elements/1.1/")
        self.rdf_graph.bind("grddl", "http://www.w3.org/2003/g/data-view#")

        adb_key_blacklist = {
            "_id",
            "_key",
            "_rev",
            "_rdftype",
            "_uri",
            "_value",
            "_label",
            "_from",
            "_to",
            "_sub_graph_uri",
        }

        adb_v_col_blacklist = {
            f"{name}_URIRef",
            f"{name}_BNode",
            f"{name}_Literal",
            f"{name}_UnknownResource",
        }

        doc: Json
        if self.db.has_collection("Property"):
            for doc in self.db.collection("Property"):
                if doc.keys() >= {"_uri", "_label"}:
                    self.__uri_map[doc["_label"]] = doc["_uri"]

        term: Union[URIRef, BNode, Literal]
        for v_col, _ in metagraph["vertexCollections"].items():
            v_col_uri = URIRef(f"{self.__adb_graph_ns}{v_col}")

            self.__set_iterators(f"     ADB → RDF ({v_col})", "#97C423", "")
            with Live(Group(self.__adb_iterator, self.__rdf_iterator)):
                cursor = self.__fetch_adb_docs(v_col)
                self.__rdf_task = self.__rdf_iterator.add_task("", total=cursor.count())

                for doc in cursor:
                    self.__rdf_iterator.update(self.__rdf_task, advance=1)

                    term = self.__process_adb_doc(doc)
                    term_map[doc["_id"]] = term

                    if type(term) is URIRef or type(term) is BNode:
                        if include_adb_key_statements:
                            key = Literal(doc["_key"])
                            self.__add_to_rdf_graph(term, self.adb_key_uri, key)

                        if v_col not in adb_v_col_blacklist:
                            self.__add_to_adb_mapping(adb_mapping, term, v_col)

                            if infer_type_from_adb_col:
                                self.__add_to_rdf_graph(term, RDF.type, v_col_uri)

        for e_col, _ in metagraph["edgeCollections"].items():
            e_col_uri = URIRef(f"{self.__adb_graph_ns}{e_col}")

            self.__set_iterators(f"     ADB → RDF ({e_col})", "#5E3108", "")
            with Live(Group(self.__adb_iterator, self.__rdf_iterator)):
                cursor = self.__fetch_adb_docs(e_col)
                self.__rdf_task = self.__rdf_iterator.add_task("", total=cursor.count())

                for doc in cursor:
                    self.__rdf_iterator.update(self.__rdf_task, advance=1)

                    subject = term_map[doc["_from"]]
                    predicate = URIRef(doc.get("_uri", "")) or e_col_uri
                    object = term_map[doc["_to"]]

                    sg_uri = None
                    if graph_supports_quads and "_sub_graph_uri" in doc:
                        sg_uri = URIRef(doc["_sub_graph_uri"])
                        subgraph_map[doc["_from"]] = sg_uri
                        # subgraph_map[doc["_to"]] = sg_uri # TODO: REVISIT

                    assert type(subject) is URIRef or type(subject) is BNode
                    self.__add_to_rdf_graph(subject, predicate, object, sg_uri)

                    # TODO: Revisit when rdflib introduces RDF-star support
                    # edge = (subject, predicate, object)
                    edge = URIRef(f"{self.__adb_graph_ns}{doc['_key']}")

                    edge_has_metadata = False
                    for k, v in doc.items():
                        if k not in adb_key_blacklist:
                            edge_has_metadata = True
                            p = self.__uri_map.get(k, f"{self.__adb_graph_ns}{k}")
                            self.__adb_property_to_rdf_val(edge, URIRef(p), v, sg_uri)

                    if edge_has_metadata:
                        self.__add_to_rdf_graph(edge, RDF.type, RDF.Statement, sg_uri)
                        self.__add_to_rdf_graph(edge, RDF.subject, subject, sg_uri)
                        self.__add_to_rdf_graph(edge, RDF.predicate, predicate, sg_uri)
                        self.__add_to_rdf_graph(edge, RDF.object, object, sg_uri)

                        if e_col not in adb_v_col_blacklist:
                            e_metadata_col = f"{e_col}_edges"  # TODO - REVISIT
                            self.__add_to_adb_mapping(adb_mapping, edge, e_metadata_col)

                            if infer_type_from_adb_col:
                                self.__add_to_rdf_graph(edge, RDF.type, e_col_uri)

        # TODO: REVISIT
        # Not a fan of this at all...
        for v_col, _ in metagraph["vertexCollections"].items():
            for doc in self.__fetch_adb_docs(v_col):
                rdf_term = term_map[doc["_id"]]
                if type(rdf_term) is URIRef or type(rdf_term) is BNode:
                    sg_uri = subgraph_map.get(doc["_id"])
                    # TODO: Iterate through metagraph values instead?
                    for k, v in doc.items():
                        if k not in adb_key_blacklist:
                            p = self.__uri_map.get(k, f"{self.__adb_graph_ns}{k}")
                            self.__adb_property_to_rdf_val(
                                rdf_term, URIRef(p), v, sg_uri
                            )

        return self.rdf_graph, adb_mapping

    def arangodb_collections_to_rdf(
        self,
        name: str,
        rdf_graph: RDFGraph,
        v_cols: Set[str],
        e_cols: Set[str],
        list_conversion_mode: str = "static",
        infer_type_from_adb_col: bool = False,
        include_adb_key_statements: bool = False,
        **export_options: Any,
    ) -> Tuple[RDFGraph, RDFGraph]:
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
            are handled andprocessed into the RDF Graph. If "collection", ArangoDB
            lists will be processed using the RDF Collection structure. If "container",
            lists found within the ArangoDB Graph will be processed using the
            RDF Container structure. If "static", elements within lists will be
            processed as individual statements. Defaults to "static".
        :type list_conversion_mode: str
        :param infer_type_from_adb_col: Specify whether `rdf:type` relationships
            of the form (adb_doc rdf:type adb_col) should be inferred upon
            transferring ArangoDB Documents into RDF. NOTE: Enabling this flag
            is only recommended if your ArangoDB graph is "native" to ArangoDB.
            That is, the ArangoDB graph does not originate from an RDF context.
        :type infer_type_from_adb_col: bool
        :param include_adb_key_statements: Specify whether `adb:key` relationships
            of the form (resource adb:key adb_doc_key) should be generated upon
            transferring ArangoDB Documents into RDF. This can be used to
            maintain document keys when a user is interested in round-tripping.
            NOTE: Currently only applies to ArangoDB Vertices (not edges).
            NOTE: Enabling this flag is only recommended if your ArangoDB graph
            is "native" to ArangoDB. That is, the ArangoDB graph does not
            originate from an RDF context.
        :type include_adb_key_statements: bool
        :param export_options: Keyword arguments to specify AQL query options when
            fetching documents from the ArangoDB instance. Full parameter list:
            https://docs.python-arango.com/en/main/specs.html#arango.aql.AQL.execute
        :type export_options: Any
        :return: The RDF representation of the ArangoDB Graph, along with a second
            RDF Graph mapping the RDF Resources to their designated ArangoDB Collection.
            The second graph, **adb_mapping**, can then be re-used in the RDF to
            ArangoDB (PGT) process to maintain the Document-to-Collection mappings.
        :rtype: Tuple[rdflib.graph.Graph, rdflib.graph.Graph]
        """
        metagraph: ADBMetagraph = {
            "vertexCollections": {col: set() for col in v_cols},
            "edgeCollections": {col: set() for col in e_cols},
        }

        return self.arangodb_to_rdf(
            name,
            rdf_graph,
            metagraph,
            list_conversion_mode,
            infer_type_from_adb_col,
            include_adb_key_statements,
            **export_options,
        )

    def arangodb_graph_to_rdf(
        self,
        name: str,
        rdf_graph: RDFGraph,
        list_conversion_mode: str = "static",
        infer_type_from_adb_col: bool = False,
        include_adb_key_statements: bool = False,
        **export_options: Any,
    ) -> Tuple[RDFGraph, RDFGraph]:
        """Create an RDF Graph from an ArangoDB Graph via its Graph Name.

        :param name: The name of the ArangoDB Graph
        :type name: str
        :param rdf_graph: The target RDF Graph to insert into.
        :type rdf_graph: rdflib.graph.Graph
        :param list_conversion_mode: Specify how ArangoDB JSON lists
            are handled andprocessed into the RDF Graph. If "collection", ArangoDB
            lists will be processed using the RDF Collection structure. If "container",
            lists found within the ArangoDB Graph will be processed using the
            RDF Container structure. If "static", elements within lists will be
            processed as individual statements. Defaults to "static".
        :type list_conversion_mode: str
        :param infer_type_from_adb_col: Specify whether `rdf:type` relationships
            of the form (adb_doc rdf:type adb_col) should be inferred upon
            transferring ArangoDB Documents into RDF. NOTE: Enabling this flag
            is only recommended if your ArangoDB graph is "native" to ArangoDB.
            That is, the ArangoDB graph does not originate from an RDF context.
        :type infer_type_from_adb_col: bool
        :param include_adb_key_statements: Specify whether `adb:key` relationships
            of the form (resource adb:key adb_doc_key) should be generated upon
            transferring ArangoDB Documents into RDF. This can be used to
            maintain document keys when a user is interested in round-tripping.
            NOTE: Currently only applies to ArangoDB Vertices (not edges).
            NOTE: Enabling this flag is only recommended if your ArangoDB graph
            is "native" to ArangoDB. That is, the ArangoDB graph does not
            originate from an RDF context.
        :type include_adb_key_statements: bool
        :param export_options: Keyword arguments to specify AQL query options when
            fetching documents from the ArangoDB instance. Full parameter list:
            https://docs.python-arango.com/en/main/specs.html#arango.aql.AQL.execute
        :type export_options: Any
        :return: The RDF representation of the ArangoDB Graph, along with a second
            RDF Graph mapping the RDF Resources to their designated ArangoDB Collection.
            The second graph, **adb_mapping**, can then be re-used in the RDF to
            ArangoDB (PGT) process to maintain the Document-to-Collection mappings.
        :rtype: Tuple[rdflib.graph.Graph, rdflib.graph.Graph]
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
            infer_type_from_adb_col,
            include_adb_key_statements,
            **export_options,
        )

    def __process_adb_doc(self, doc: Json) -> RDFObject:
        """An ArangoDB to RDF helper method used to process ArangoDB
        JSON documents as an RDF Term. Returns the URIRef, BNode, or
        Literal equivalent of **doc**. If **doc** does not have
        "_rdftype" as a property, then the URIRef type is used.

        :param doc: An arbitrary ArangoDB document.
        :type doc: Dict[str, Any]
        :return: The RDF Term representing the ArangoDB document
        :rtype: URIRef | BNode | Literal
        """
        key_map = {
            "URIRef": "_uri",
            "Literal": "_value",
            "BNode": "_key",
        }

        rdf_type = doc.get("_rdftype", "URIRef")
        val = doc.get(key_map[rdf_type], f"{self.__adb_graph_ns}{doc['_key']}")

        if rdf_type == "URIRef":
            return URIRef(val)

        elif rdf_type == "BNode":
            return BNode(val)

        elif rdf_type == "Literal":
            if "_lang" in doc:
                return Literal(val, lang=doc["_lang"])

            elif "_datatype" in doc:
                return Literal(val, datatype=doc["_datatype"])

            else:
                return Literal(val)

        else:  # pragma: no cover
            raise ValueError(f"Unrecognized type {rdf_type} ({doc})")

    def __add_to_rdf_graph(
        self, s: RDFSubject, p: URIRef, o: RDFObject, sg_uri: Optional[URIRef] = None
    ) -> None:
        """Another ArangoDB-to-RDF helper method used to insert the statement
        (s,p,o) into the RDF Graph as a Triple or Quad, depending on if a
        Sub Graph URI is specified.

        :param s: The RDF Subject object of the (s,p,o) statement.
        :type s: URIRef | BNode
        :param p: The RDF Predicate object of the (s,p,o) statement.
        :type p: URIRef
        :param o: The RDF Object object of the (s,p,o) statement.
        :type o: URIRef | BNode | Literal
        :param sg_uri: The Sub Graph URI of the (s,p,o) statement, if any.
        :type sg_uri: URIRef | None
        """
        statement = (s, p, o, sg_uri) if sg_uri else (s, p, o)
        self.rdf_graph.add(statement)  # type: ignore[arg-type]

    def __adb_property_to_rdf_val(
        self, s: RDFSubject, p: URIRef, val: Any, sg_uri: Optional[URIRef] = None
    ) -> None:
        """A helper function used to insert an arbitrary ArangoDB
        document property as an RDF Object in some RDF Statement.

        If the ArangoDB document property **val** is of type list
        or dict, then a recursive process is introduced to unpack
        the ArangoDB document property into multiple RDF Statements.

        Otherwise, the ArangoDB Document Property is treated as
        a Literal in the context of RDF.

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
        :param sg_uri: The Sub Graph URI of the (s,p,o) statement, if any.
        :type sg_uri: URIRef | None
        """

        if type(val) is list:
            if self.__list_conversion == "collection":
                node: RDFSubject = BNode()
                self.__add_to_rdf_graph(s, p, node, sg_uri)

                rest: RDFSubject
                for i, v in enumerate(val):
                    self.__adb_property_to_rdf_val(node, RDF.first, v)

                    rest = RDF.nil if i == len(val) - 1 else BNode()
                    self.__add_to_rdf_graph(node, RDF.rest, rest, sg_uri)
                    node = rest

            elif self.__list_conversion == "container":
                bnode = BNode()
                self.__add_to_rdf_graph(s, p, bnode, sg_uri)

                for i, v in enumerate(val, 1):
                    _n = URIRef(f"{RDF}_{i}")
                    self.__adb_property_to_rdf_val(bnode, _n, v)

            elif self.__list_conversion == "static":
                for v in val:
                    self.__adb_property_to_rdf_val(s, p, v)

            else:
                raise ValueError("Invalid **list_conversion_mode value")

        elif type(val) is dict:
            bnode = BNode()
            self.__add_to_rdf_graph(s, p, bnode, sg_uri)

            for k, v in val.items():
                p_str = self.__uri_map.get(k, f"{self.__adb_graph_ns}{k}")
                self.__adb_property_to_rdf_val(bnode, URIRef(p_str), v)

        else:
            # TODO: Datatype? Lang?
            self.__add_to_rdf_graph(s, p, Literal(val), sg_uri)

    def __fetch_adb_docs(self, adb_col: str) -> Result[Cursor]:
        """Fetches ArangoDB documents within a collection.

        :param adb_col: The ArangoDB collection.
        :type adb_col: str
        :return: Result cursor.
        :rtype: arango.cursor.Cursor
        """
        action = f"ArangoDB Export: {adb_col}"
        adb_task = self.__adb_iterator.add_task("", action=action)

        aql = f"FOR doc IN {adb_col} RETURN doc"
        cursor = self.db.aql.execute(aql, count=True, **self.__export_options)

        self.__adb_iterator.stop_task(adb_task)
        self.__adb_iterator.update(adb_task, visible=True)

        return cursor

    ###################################################################################
    # RDF to ArangoDB & ArangoDB to RDF Shared Methods
    # 1) __add_to_adb_mapping:
    ###################################################################################

    def __add_to_adb_mapping(
        self,
        adb_mapping: RDFGraph,
        s: RDFSubject,
        adb_col: str,
        overwrite: bool = False,
    ) -> None:
        """Add to **adb_mapping** a statement of the form
        (s, URIRef("http://www.arangodb.com/collection"), Literal(adb_col)) .

        :param adb_mapping: The RDF Graph representing the
            ArangoDB Collection Mapping statements.
        :type adb_mapping: rdflib.graph.Graph
        :param s: The RDF Subject.
        :type s: URIRef | BNode
        :param adb_col: The ArangoDB Collection name.
        :type adb_col: str
        :param overwrite: If True, delete any existing statements of
            the form (s, URIRef("http://www.arangodb.com/collection"), None).
            Defaults to False.
        :type overwrite: bool
        """
        if overwrite:
            adb_mapping.remove((s, self.adb_col_uri, None))

        adb_mapping.add((s, self.adb_col_uri, Literal(adb_col)))
