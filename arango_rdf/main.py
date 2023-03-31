#!/usr/bin/env python3
import gc
import hashlib
import logging
import os
import re
import sys
from ast import literal_eval
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any, DefaultDict, Dict, List, Optional, Set, Tuple, Union

from arango.cursor import Cursor
from arango.database import Database
from arango.graph import Graph as ADBGraph
from arango.result import Result
from rdflib import RDF, RDFS, BNode
from rdflib import ConjunctiveGraph as RDFConjunctiveGraph
from rdflib import Dataset as RDFDataset
from rdflib import Graph as RDFGraph
from rdflib import Literal, URIRef
from rdflib.term import Node
from rich.console import Group
from rich.live import Live

from .abc import Abstract_ArangoRDF
from .typings import (
    ADBDocs,
    ADBMetagraph,
    DomainRangeMap,
    Json,
    RDFLists,
    RDFObject,
    RDFSubject,
    TermMetadata,
)
from .utils import adb_track, logger, rdf_track

PROJECT_DIR = Path(__file__).parent


class ArangoRDF(Abstract_ArangoRDF):
    """ArangoRDF. Transform an RDF Graph into an
    ArangoDB Graph & vice-versa.

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

        # A dictionary mapping all of the to-be-inserted ArangoDB
        # documents to their ArangoDB collection.
        self.adb_docs: ADBDocs
        self.__domain_range_map: DomainRangeMap
        self.__rdf_lists: RDFLists

        self.__rdf_type_str = str(RDF.type)
        self.__rdf_type_key = self.rdf_id_to_adb_key(self.__rdf_type_str)
        self.__rdf_property_str = str(RDF.Property)
        self.__rdf_property_key = self.rdf_id_to_adb_key(self.__rdf_property_str)

        logger.info(f"Instantiated ArangoRDF with database '{db.name}'")

    def set_logging(self, level: Union[int, str]) -> None:
        logger.setLevel(level)

    def rdf_to_arangodb_by_rpt(
        self,
        name: str,
        rdf_graph: RDFGraph,
        overwrite_graph: bool = False,
        contextualize_graph: bool = False,
        batch_size: Optional[int] = None,
        **import_options: Any,
    ) -> ADBGraph:
        """Create an ArangoDB Graph from an RDF Graph using
        the RDF-topology-preserving transformation (RPT) Algorithm.
        See this package's README.md file for more information,
        or visit https://arxiv.org/pdf/2210.05781.pdf.

        :param name: The name of the RDF Graph
        :type name: str
        :param rdf_graph: The RDF Graph object
        :type: rdf_graph: rdflib.graph.Graph
        :param overwrite_graph: Overwrites the graph if it already exists, and drops its
            associated collections. Defaults to False.
        :type overwrite_graph: bool
        :param contextualize_graph: Imports the arango_rdf/ontologies/rdfowl.ttl
            to the ArangoDB graph to serve as a base ontology. Defaults to False.
        :type contextualize_graph: bool
        :param batch_size: If specified, runs the ArangoDB Data Import process for every
            **batch_size** triples within the **rdf_graph**.
            If unspecified, **batch_size** will be set to `len(rdf_graph)`.
        :type batch_size: int
        :param import_options: Keyword arguments to specify additional
            parameters for ArangoDB document insertion. Full parameter list:
            https://docs.python-arango.com/en/main/specs.html#arango.collection.Collection.import_bulk
        :type import_options: Any

        :return: The ArangoDB Graph API wrapper.
        :rtype: arango.graph.Graph
        """

        self.adb_docs = defaultdict(lambda: defaultdict(dict))
        self.__domain_range_map = defaultdict(lambda: defaultdict(dict))
        self.__rdf_statements = (
            rdf_graph.quads
            if isinstance(rdf_graph, (RDFDataset, RDFConjunctiveGraph))
            else rdf_graph.triples
        )

        self.__rdf_graph = rdf_graph
        self.__contextualize_graph = contextualize_graph
        self.__import_options = import_options
        self.__import_options["on_duplicate"] = "update"

        # Set ArangoDB Collection (i.e 'col') names
        self.__URIREF_COL = f"{name}_URIRef"
        self.__BNODE_COL = f"{name}_BNode"
        self.__LITERAL_COL = f"{name}_Literal"
        self.__STATEMENT_COL = f"{name}_Statement"

        if self.__contextualize_graph:
            for ns in os.listdir(f"{PROJECT_DIR}/meta"):
                rdf_graph.parse(f"{PROJECT_DIR}/meta/{ns}")

            self.__build_domain_and_range_map()

        if overwrite_graph:
            self.db.delete_graph(name, ignore_missing=True, drop_collections=True)

        size = len(rdf_graph)
        if batch_size is None:
            batch_size = size

        self.__setup_iterators("RDF → ADB (RPT)", "#08479E", "    ")
        with Live(Group(self.__rdf_iterator, self.__adb_iterator)):
            self.__rdf_task = self.__rdf_iterator.add_task("", total=size)

            s: RDFSubject  # Subject
            p: URIRef  # Predicate
            o: RDFObject  # Object

            for count, (s, p, o, *sub_graph) in enumerate(rdf_graph, 1):  # type: ignore
                self.__rdf_iterator.update(self.__rdf_task, advance=1)

                s_col, s_key = self.__rpt_process_term(s)
                o_col, o_key = self.__rpt_process_term(o)

                p_str = str(p)
                p_key = self.rdf_id_to_adb_key(p_str)
                p_label = self.rdf_id_to_adb_label(p_str)

                self.__add_adb_edge(
                    self.__STATEMENT_COL,
                    f"{s_key}-{p_key}-{o_key}",
                    f"{s_col}/{s_key}",
                    f"{o_col}/{o_key}",
                    p_str,
                    p_label,
                    str(sub_graph[0]) if sub_graph else "",
                )

                if self.__contextualize_graph:
                    p_col, p_key = self.__rpt_process_term(p)
                    self.__add_adb_edge(
                        self.__STATEMENT_COL,
                        f"{p_key}-{self.__rdf_type_key}-{self.__rdf_property_key}",
                        f"{p_col}/{p_key}",
                        f"{self.__URIREF_COL}/{self.__rdf_property_key}",
                        self.__rdf_type_str,
                        "type",
                        "",
                    )

                    dr_list = [(s_col, s_key, "domain"), (o_col, o_key, "range")]
                    for term_col, term_key, dr in dr_list:
                        if self.__domain_range_map[p][dr]:
                            dr_key = self.__domain_range_map[p][dr]["key"]

                            self.__add_adb_edge(
                                self.__STATEMENT_COL,
                                f"{term_key}-{self.__rdf_type_key}-{dr_key}",
                                f"{term_col}/{term_key}",
                                f"{self.__URIREF_COL}/{dr_key}",
                                self.__rdf_type_str,
                                "type",
                                "",
                            )

                if count % batch_size == 0:
                    self.__insert_adb_docs()

            self.__insert_adb_docs()

        return self.__rpt_create_adb_graph(name)

    def __rpt_process_term(self, t: Union[RDFSubject, RDFObject]) -> Tuple[str, str]:
        """Process an RDF Term as an ArangoDB document by RPT. Returns the
        ArangoDB Collection & Document Key associated to the RDF term.

        :param t: The RDF Term to process
        :type t: URIRef | BNode | Literal
        :return: The ArangoDB Collection name & Document Key of the RDF Term
        :rtype: Tuple[str, str]
        """

        t_col = ""
        t_str = str(t)

        if isinstance(t, URIRef):
            t_col = self.__URIREF_COL
            t_key = self.rdf_id_to_adb_key(t_str)
            t_label = self.rdf_id_to_adb_label(t_str)

            self.adb_docs[t_col][t_key] = {
                "_key": t_key,
                "_uri": t_str,
                "_label": t_label,
                "_rdftype": "URIRef",
            }

        elif isinstance(t, BNode):
            t_col = self.__BNODE_COL
            t_key = t_str

            self.adb_docs[t_col][t_key] = {"_key": t_key, "_rdftype": "BNode"}

        elif isinstance(t, Literal):
            t_col = self.__LITERAL_COL
            t_key = str(hash(t_str))

            t_value = t_str if isinstance(t.value, date) else t.value or t_str
            t_datatype = t.datatype or "http://www.w3.org/2001/XMLSchema#string"
            t_lang = t.language or "en"

            self.adb_docs[t_col][t_key] = {
                "_key": t_key,
                "_value": t_value,
                "_label": t_value,  # TODO: REVISIT
                "_lang": t_lang,
                "_datatype": t_datatype,
                "_rdftype": "Literal",
            }

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
            [
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

    def rdf_to_arangodb_by_pgt(
        self,
        name: str,
        rdf_graph: RDFGraph,
        overwrite_graph: bool = False,
        contextualize_graph: bool = False,
        adb_collection_uri: URIRef = URIRef("http://www.arangodb.com/collection"),
        batch_size: Optional[int] = None,
        **import_options: Any,
    ) -> ADBGraph:
        """Create an ArangoDB Graph from an RDF Graph using
        the Property Graph Transformation (PGT) Algorithm.
        See this package's README.md file for more information,
        or visit https://arxiv.org/pdf/2210.05781.pdf.

        :param name: The name of the RDF Graph
        :type name: str
        :param rdf_graph: The RDF Graph object
        :type: rdf_graph: rdflib.graph.Graph
        :param overwrite_graph: Overwrites the graph if it already exists, and drops its
            associated collections. Defaults to False.
        :type overwrite_graph: bool
        :param contextualize_graph: Load the OWL, RDF, & RDFS namespace ontologies
            into the graph. Defaults to False.
        :type contextualize_graph: bool
        :param adb_collection_uri: A custom URIRef used as an (optional) predicate to
            identify the ArangoDB Collection name of RDF subjects within the graph.
            Defaults to `URIRef("http://www.arangodb.com/collection")`. For example,
            `example:alice http://www.arangodb.com/collection Person` will place the
            ArangoDB "alice" document in the ArangoDB "Person" Collection.
        :type adb_collection_uri: URIRef
        :param batch_size: If specified, runs the ArangoDB Data Import process for every
            **batch_size** triples within the **rdf_graph**.
            If unspecified, **batch_size** will be set to `len(rdf_graph)`.
        :type batch_size: int
        :param import_options: Keyword arguments to specify additional
            parameters for ArangoDB document insertion. Full parameter list:
            https://docs.python-arango.com/en/main/specs.html#arango.collection.Collection.import_bulk
        :type import_options: Any

        :return: The ArangoDB Graph API wrapper.
        :rtype: arango.graph.Graph
        """

        # Reset config
        self.adb_docs = defaultdict(lambda: defaultdict(dict))
        self.__domain_range_map = defaultdict(lambda: defaultdict(dict))
        self.__rdf_lists = defaultdict(lambda: defaultdict(dict))
        self.__rdf_statements = (
            rdf_graph.quads
            if isinstance(rdf_graph, (RDFDataset, RDFConjunctiveGraph))
            else rdf_graph.triples
        )

        self.__rdf_graph = rdf_graph
        self.__contextualize_graph = contextualize_graph
        self.__adb_collection_uri = adb_collection_uri
        self.__import_options = import_options
        self.__import_options["on_duplicate"] = "update"
        self.__process_rdf_val_as_string = False

        # Map URI strings to ArangoDB Collection names
        self.__col_map: Dict[str, str] = {}
        # A set of ArangoDB Collections that will NOT imported via
        # batch processing (i.e they contain documents whose properties
        # are subject to change)
        self.__adb_col_blacklist: Set[str] = set()
        # ArangoDB Collection name for all unidentified RDF Resources
        self.__UNIDENTIFIED_NODE_COL = f"{name}_UnidentifiedNode"
        # Maintains the set of ArangoDB Edge Definitions of the RDF Graph
        self.__e_col_map: DefaultDict[str, DefaultDict[str, Set[str]]]
        self.__e_col_map = defaultdict(lambda: defaultdict(set))

        if self.__contextualize_graph:
            for ns in os.listdir(f"{PROJECT_DIR}/meta"):
                rdf_graph.parse(f"{PROJECT_DIR}/meta/{ns}")

        if overwrite_graph:
            self.db.delete_graph(name, ignore_missing=True, drop_collections=True)

        size = len(rdf_graph)
        if batch_size is None:
            batch_size = size

        self.__build_domain_and_range_map()
        self.__pgt_build_col_map()

        ############## PGT Processing ##############
        self.__setup_iterators("RDF → ADB (PGT)", "#08479E", "    ")
        with Live(Group(self.__rdf_iterator, self.__adb_iterator)):
            self.__rdf_task = self.__rdf_iterator.add_task("", total=size)

            s: RDFSubject  # Subject
            p: URIRef  # Predicate
            o: RDFObject  # Object

            # object_blacklist = [RDF.nil, RDF.Alt, RDF.Bag, RDF.List, RDF.Seq]

            for count, (s, p, o, *sub_graph) in enumerate(rdf_graph, 1):  # type: ignore
                self.__rdf_iterator.update(self.__rdf_task, advance=1)

                # TODO: Discuss repercussions
                if p == self.__adb_collection_uri:  # or o in object_blacklist:
                    continue

                s_str = str(s)
                o_str = str(o)

                p_meta = self.__pgt_get_term_metadata(str(p), is_predicate=True)
                p_str, p_key, p_col, p_label = p_meta

                rdf_list_col = self.__pgt_statement_is_part_of_rdf_list(s, p_str)
                if rdf_list_col:
                    doc = self.__rdf_lists[rdf_list_col][s_str]
                    self.__rdf_val_to_adb_property(doc, p_label, o)

                else:
                    sg = str(sub_graph[0]) if sub_graph else ""

                    s_meta = self.__pgt_get_term_metadata(s_str)
                    o_meta = self.__pgt_get_term_metadata(o_str)

                    self.__pgt_process_subject(s, s_meta)
                    self.__pgt_process_predicate(s_meta, p_meta, o, o_meta, sg)
                    self.__pgt_process_object(s_meta, p_meta, o, o_meta, sg)

                    if self.__contextualize_graph:
                        # TODO: REVISIT
                        p_col = self.__col_map.get(p_str, "Property")
                        new_p_meta = (p_meta[0], p_meta[1], p_col, p_meta[3])
                        self.__pgt_process_subject(p, new_p_meta)
                        self.__add_adb_edge(
                            "type",
                            f"{p_key}-{self.__rdf_type_key}-{self.__rdf_property_key}",
                            f"{p_col}/{p_key}",
                            f"Class/{self.__rdf_property_key}",
                            self.__rdf_type_str,
                            "type",
                            "",
                        )

                        dr_list = [(s_meta, "domain"), (o_meta, "range")]
                        for term_meta, dr in dr_list:
                            if self.__domain_range_map[p][dr]:
                                _, term_key, term_col, _ = term_meta
                                dr_key = self.__domain_range_map[p][dr]["key"]

                                self.__add_adb_edge(
                                    "type",
                                    f"{term_key}-{self.__rdf_type_key}-{dr_key}",
                                    f"{term_col}/{term_key}",
                                    f"Class/{dr_key}",
                                    self.__rdf_type_str,
                                    "type",
                                    "",
                                )

                if count % batch_size == 0:
                    self.__insert_adb_docs(self.__adb_col_blacklist)

        gc.collect()
        ############## ############## ##############

        ############## Post Processing ##############
        self.__setup_iterators("RDF → ADB (PGT Post-Process)", "#EF7D00", "    ")
        with Live(Group(self.__rdf_iterator, self.__adb_iterator)):
            self.__pgt_process_rdf_lists()
            self.__insert_adb_docs()

        gc.collect()
        ############## ############### ##############

        if self.db.has_collection(self.__UNIDENTIFIED_NODE_COL):
            un_count = self.db.collection(self.__UNIDENTIFIED_NODE_COL).count()
            logger.info(
                f"""\n
                ----------------
                UnidentifiedNodes found in graph '{name}'.
                No `rdf:type` statement found for the
                following number of URIRefs/BNodes: {un_count}.
                ----------------
                """
            )

        return self.__pgt_create_adb_graph(name)

    def __pgt_build_col_map(self) -> None:
        """A pre-processing step that iterates through specific RDF Graph
        statements to build a URI-to-ArangoDB-Collection mapping of all
        RDF Nodes within the graph.

        This step is required for the PGT Processing stage in order to
        ensure each RDF Node is properly identified and categorized under
        a specific ArangoDB collection.
        """
        type_map: DefaultDict[str, Set[str]] = defaultdict(set)
        subclass_map: DefaultDict[str, Set[str]] = defaultdict(set)

        explicit_col_map = self.__pgt_build_explicit_col_map()

        ############################################################
        # TODO - REVISIT
        for p, data in self.__domain_range_map.items():
            t = (None, p, None)
            for s, _, o, *_ in self.__rdf_statements(t):  # type: ignore
                if data["domain"]:
                    type_map[str(s)].add(data["domain"]["str"])

                if data["range"]:
                    type_map[str(o)].add(data["range"]["str"])

        ############################################################
        t = (None, RDF.type, None)
        for s, _, o, *_ in self.__rdf_statements(t):  # type: ignore[operator]
            type_map[str(s)].add(str(o))

        ############################################################
        t = (None, RDFS.subClassOf, None)
        for s, _, o, *_ in self.__rdf_statements(t):  # type: ignore[operator]
            subclass_map[str(s)].add(str(o))

        ############################################################

        ############################################################
        if self.__contextualize_graph:
            triple = (None, None, None)
            for _, p, _, *_ in self.__rdf_statements(triple):  # type: ignore[operator]
                type_map[str(p)].add(self.__rdf_property_str)
        ############################################################

        self.__pgt_finalize_col_map(type_map, subclass_map, explicit_col_map)

    def __pgt_build_explicit_col_map(self) -> Dict[str, str]:
        """TODO: DEFINE"""
        explicit_col_map: Dict[str, str] = dict()

        triple = (None, self.__adb_collection_uri, None)
        for s, _, o, *_ in self.__rdf_statements(triple):  # type: ignore
            s_str = str(s)
            o_str = str(o)

            if not isinstance(o, Literal):
                raise ValueError(f"Object {o} must be Literal")  # pragma: no cover

            if s_str in explicit_col_map:
                # TODO: Create custom error
                raise ValueError(  # pragma: no cover
                    f"""
                    Subject {s} can only have 1 ArangoDB Collection association.
                    Found '{explicit_col_map[s_str]}' and '{o_str}'.
                    """
                )

            explicit_col_map[s_str] = o_str

        return explicit_col_map

    def __pgt_finalize_col_map(
        self,
        type_map: DefaultDict[str, Set[str]],
        subclass_map: DefaultDict[str, Set[str]],
        explicit_col_map: Dict[str, str],
    ) -> None:
        """TODO: DEFINE"""

        # Helper function to recursively iterate through `subclass_map`
        def get_depth(class_str: str, depth: int) -> int:
            if class_str not in subclass_map:
                return depth

            for sub_class_str in subclass_map[class_str]:
                if sub_class_str == class_str:
                    return depth  # pragma: no cover

                return get_depth(sub_class_str, depth + 1)

            return -1  # pragma: no cover

        for s_str, class_set in type_map.items():
            # Case 1 (ArangoDB Collection Property Used)
            if s_str in explicit_col_map:
                self.__col_map[s_str] = explicit_col_map[s_str]

            # Case 2 (Only one type statement associated to s_str)
            elif len(class_set) == 1:
                self.__col_map[s_str] = self.rdf_id_to_adb_label(class_set.pop())

            # Case 3 (Taxonomy)
            elif any([c in subclass_map for c in class_set]):
                max_depth = -1
                best_class = ""
                # NOTE: Process is not deterministic if `sorted()` is removed
                for c in sorted(class_set):
                    depth = get_depth(c, 0)

                    if depth > max_depth:
                        max_depth = depth
                        best_class = c

                self.__col_map[s_str] = self.rdf_id_to_adb_label(best_class)

            # Case 4 (Multiple types without explicit_col_map or sub_class_map entry)
            else:
                # TODO - Deal with domain_range_map issue..
                # Should it even be considered for `type_map`?
                # Should it be in its own "Case" ?
                self.__col_map[s_str] = self.rdf_id_to_adb_label(sorted(class_set)[0])

    def __pgt_get_term_metadata(
        self, term_str: str, is_predicate: bool = False
    ) -> TermMetadata:
        """Return the following PGT-relevant metadata associated to the RDF Term:
            1. The string representation of the term
            2. The Arangodb Key of the term
            3. The Arangodb Collection of the term
            4. The ArangoDB "label" doc property value of the term

        :param term_str: The string representation of the RDF Term
        :type term_str: str
        :param is_predicate: Set to true if **term_str represents a predicate
        :return: The string representation, ArangoDB Document Key,
        ArangoDB Collection name, and RDF Label of the RDF Term.
        :rtype: TermMetadata
        """
        term_key = self.rdf_id_to_adb_key(term_str)

        if is_predicate:
            term_col = self.rdf_id_to_adb_label(term_str)
            return term_str, term_key, term_col, term_col

        term_col = self.__col_map.get(term_str, self.__UNIDENTIFIED_NODE_COL)
        term_label = self.rdf_id_to_adb_label(term_str)

        return term_str, term_key, term_col, term_label

    def __pgt_process_rdf_term(
        self,
        t: Union[RDFSubject, RDFObject],
        t_meta: TermMetadata,
        s_key: str = "",
        s_col: str = "",
        p_label: str = "",
    ) -> None:
        """Process an RDF Term as an ArangoDB document by PGT.

        :param t: The RDF Term
        :type t: URIRef | BNode | Literal
        :param t_meta: The PGT Metadata associated to the RDF Term.
        :type t_meta: TermMetadata
        :param s_key: The ArangoDB document key of the Subject associated
            to the RDF Term **t**. Only required if the RDF Term is of type Literal.
        :type s_key: str
        :param s_col: The ArangoDB document key of the Subject associated
            to the RDF Term **t**. Only required if the RDF Term is of type Literal.
        :type s_col: str
        :param p_label: The RDF Predicate Label key of the Predicate associated
            to the RDF Term **t**. Only required if the RDF Term is of type Literal.
        :type p_label: str
        """

        t_str, t_key, t_col, t_label = t_meta

        if isinstance(t, URIRef):
            self.adb_docs[t_col][t_key] = {
                **self.adb_docs[t_col][t_key],
                "_key": t_key,
                "_uri": t_str,
                "_label": t_label,
                "_rdftype": "URIRef",
            }

        elif isinstance(t, BNode):
            self.adb_docs[t_col][t_key] = {
                **self.adb_docs[t_col][t_key],
                "_key": t_key,
                "_rdftype": "BNode",
            }

        elif isinstance(t, Literal) and all([s_col, s_key, p_label]):
            doc = self.adb_docs[s_col][s_key]
            t_value = t_str if isinstance(t.value, date) else t.value or t_str
            self.__rdf_val_to_adb_property(doc, p_label, t_value)

            self.__adb_col_blacklist.add(s_col)  # TODO: REVISIT

        else:
            raise ValueError()  # pragma: no cover

    def __rdf_val_to_adb_property(self, doc: Json, key: str, val: Any) -> None:
        """A helper function used to insert an arbitrary RDF value
        as a document property of some arbitrary ArangoDB document.

        If `self.__process_rdf_val_as_string` is enabled, the RDF
        Literal value is appended to a string representation of the
        current value of the document property (instead of relying
        on a list structure).

        :param doc: An arbitrary document
        :type doc: Dict[str, Any]
        :param key: An arbitrary document property key.
        :type key: str
        :param val: The value associated to the document property **key**.
        :type val: Any
        """

        # This flag is set active in ArangoRDF.__pgt_process_rdf_lists()
        if self.__process_rdf_val_as_string:
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
        """A wrapper over the function `__pgt_process_rdf_term` for easier
        code readability. Processes the RDF Subject into ArangoDB.

        :param s: The RDF Subject to process into ArangoDB
        :type s: URIRef | BNode
        :param s_meta: The PGT Metadata associated to the RDF Subject.
        :type s_meta: TermMetadata
        """
        self.__pgt_process_rdf_term(s, s_meta)

    def __pgt_process_object(
        self,
        s_meta: TermMetadata,
        p_meta: TermMetadata,
        o: RDFObject,
        o_meta: TermMetadata,
        sg: str,
    ) -> None:
        """Processes the RDF Object into ArangoDB. Given the possibily of
        the RDF Object being used as the "root" of an RDF Collection or
        an RDF Container (i.e an RDF List), this wrapper function is used
        to prevent calling `__pgt_process_rdf_term` if it is not required.

        :param s_meta: The PGT Metadata associated to the
            RDF Subject of the statement containing the RDF Object.
        :type s_meta: TermMetadata
        :param p_meta: The PGT Metadata associated to the
            RDF Predicate of the statement containing the RDF Object.
        :type p_meta: TermMetadata
        :param o: The RDF Object to process into ArangoDB.
        :type o: URIRef | BNode | Literal
        :param o_meta: The PGT Metadata associated to the RDF Object.
        :type o_meta: TermMetadata
        :param sg: The string representation of the sub-graph URIRef associated
            to this statement (if any).
        :type sg: str
        """

        s_str, s_key, s_col, _ = s_meta
        p_str, _, _, p_label = p_meta

        if self.__pgt_object_is_head_of_rdf_list(o):
            foo = {"root": o, "sub_graph": sg}
            self.__rdf_lists["_LIST_HEAD"][s_str][p_str] = foo

        else:
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
        :type s_meta: TermMetadata
        :param p_meta: The PGT Metadata associated to the
            RDF Predicate of the statement containing the RDF Object.
        :type p_meta: TermMetadata
        :param o: The RDF Object to process into ArangoDB.
        :type o: URIRef | BNode | Literal
        :param o_meta: The PGT Metadata associated to the RDF Object.
        :type o_meta: TermMetadata
        :param sg: The string representation of the sub-graph URIRef associated
            to this statement (if any).
        :type sg: str
        """
        if isinstance(o, Literal) or self.__pgt_object_is_head_of_rdf_list(o):
            return

        _, s_key, s_col, _ = s_meta
        p_str, p_key, p_col, p_label = p_meta
        _, o_key, o_col, _ = o_meta

        self.__add_adb_edge(
            p_col,
            f"{s_key}-{p_key}-{o_key}",
            f"{s_col}/{s_key}",
            f"{o_col}/{o_key}",
            p_str,
            p_label,
            sg,
        )

        self.__e_col_map[p_col]["from"].add(s_col)
        self.__e_col_map[p_col]["to"].add(o_col)

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

        rdf_str = str(RDF)
        _n = (o, URIRef(f"{rdf_str}_1"), None)
        li = (o, URIRef(f"{rdf_str}li"), None)

        is_head_of_collection = first in self.__rdf_graph or rest in self.__rdf_graph
        is_head_of_container = _n in self.__rdf_graph or li in self.__rdf_graph

        return is_head_of_collection or is_head_of_container

    def __pgt_statement_is_part_of_rdf_list(self, s: RDFSubject, p_str: str) -> str:
        """Return the associated "Document Buffer" key if the RDF Statement
        (s, p, _) is part of an RDF Collection or RDF Container within the RDF Graph.
        Essential for unpacking the complicated data structure of
        RDF Lists and re-building them as an ArangoDB Document Property.

        :param s: The RDF Subject.
        :type s: URIRef | BNode
        :param p_str: The string representation of the RDF Predicate.
        :type p: str
        :return: The **self.adb_docs** "Document Buffer" key associated
            to the RDF Statement. If the statement is not part of an RDF
            List, return an empty string.
        :rtype: str
        """
        # TODO: Discuss repurcussions of this assumption
        if not isinstance(s, BNode):
            return ""

        if p_str in [str(RDF.first), str(RDF.rest)]:
            return "_COLLECTION_BNODE"

        _n = r"^http://www.w3.org/1999/02/22-rdf-syntax-ns#_[0-9]{1,}$"
        li = r"^http://www.w3.org/1999/02/22-rdf-syntax-ns#li$"
        if re.match(_n, p_str) or re.match(li, p_str):
            return "_CONTAINER_BNODE"

        return ""  # pragma: no cover

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
        self.__process_rdf_val_as_string = True
        list_heads = self.__rdf_lists["_LIST_HEAD"].items()

        self.__rdf_task = self.__rdf_iterator.add_task("", total=len(list_heads))
        for s_str, s_dict in list_heads:
            self.__rdf_iterator.update(self.__rdf_task, advance=1)

            s_meta = self.__pgt_get_term_metadata(s_str)
            _, s_key, s_col, _ = s_meta

            for p_str, p_dict in s_dict.items():
                p_meta = self.__pgt_get_term_metadata(p_str, is_predicate=True)
                p_label = p_meta[-1]

                doc = self.adb_docs[s_col][s_key]
                doc["_key"] = s_key  # NOTE: Is this really necessary?

                root: RDFObject = p_dict["root"]
                sg: str = p_dict["sub_graph"]

                doc[p_label] = ""
                self.__pgt_process_rdf_list_object(doc, s_meta, p_meta, root, sg)
                doc[p_label] = doc[p_label].rstrip(",")

                # Delete doc[p_key] if there are no Literals within the List
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
        :type s_meta: TermMetadata
        :param p_meta: The PGT Metadata associated to the RDF Predicate.
        :type p_meta: TermMetadata
        :param o: The RDF List Object to process into ArangoDB.
        :type o: URIRef | BNode | Literal
        :param sg: The string representation of the sub-graph URIRef associated
            to the RDF List Statement (if any).
        :type sg: str
        """
        o_str = str(o)

        if o_str in self.__rdf_lists["_COLLECTION_BNODE"]:
            p_label = p_meta[-1]
            doc[p_label] += "["

            next_bnode_dict = self.__rdf_lists["_COLLECTION_BNODE"][o_str]
            self.__pgt_unpack_rdf_collection(doc, s_meta, p_meta, next_bnode_dict, sg)

            doc[p_label] = str(doc[p_label]).rstrip(",") + "],"

        elif o_str in self.__rdf_lists["_CONTAINER_BNODE"]:
            p_label = p_meta[-1]
            doc[p_label] += "["

            next_bnode_dict = self.__rdf_lists["_CONTAINER_BNODE"][o_str]
            self.__pgt_unpack_rdf_container(doc, s_meta, p_meta, next_bnode_dict, sg)

            doc[p_label] = str(doc[p_label]).rstrip(",") + "],"

        elif o_str:
            o_meta = self.__pgt_get_term_metadata(o_str)
            self.__pgt_process_object(s_meta, p_meta, o, o_meta, sg)
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
        :type s_meta: TermMetadata
        :param p_meta: The PGT Metadata associated to the RDF Predicate.
        :type p_meta: TermMetadata
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
            next_bnode_dict = self.__rdf_lists["_COLLECTION_BNODE"][str(rest)]
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
        :type s_meta: TermMetadata
        :param p_meta: The PGT Metadata associated to the RDF Predicate.
        :type p_meta: TermMetadata
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

        non_orphan_collections: Set[str] = set()
        edge_definitions: List[Dict[str, Union[str, List[str]]]] = []

        for e_col, v_cols in self.__e_col_map.items():
            edge_definitions.append(
                {
                    "from_vertex_collections": list(v_cols["from"]),
                    "edge_collection": e_col,
                    "to_vertex_collections": list(v_cols["to"]),
                }
            )

            non_orphan_collections = non_orphan_collections | v_cols["from"]
            non_orphan_collections = non_orphan_collections | v_cols["to"]

        orphan_collections = list(
            non_orphan_collections
            ^ {self.__UNIDENTIFIED_NODE_COL}
            ^ set(self.__col_map.values())
        )

        return self.db.create_graph(  # type: ignore[return-value]
            name, edge_definitions, orphan_collections
        )

    def rdf_id_to_adb_key(self, rdf_id: str) -> str:
        """Convert an RDF Resource ID string into an ArangoDB Key via
        Hashlib's MD5 function.

        :param rdf_id: The string representation of an RDF Resource
        :type rdf_id: str
        :return: The ArangoDB _key equivalent of **rdf_id**
        :rtype: str
        """
        return hashlib.md5(rdf_id.encode()).hexdigest()

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

    def arangodb_to_rdf(
        self,
        name: str,
        rdf_graph: RDFGraph,
        metagraph: ADBMetagraph,
        list_conversion_mode: str = "static",
        adb_graph_namespace: str = "",
        reify_triples: bool = False,
        **export_options: Any,
    ) -> RDFGraph:
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
        :param export_options: Keyword arguments to specify AQL query options when
            fetching documents from the ArangoDB instance. Full parameter list:
            https://docs.python-arango.com/en/main/specs.html#arango.aql.AQL.execute
        :type export_options: Any
        :return: An RDF Graph equivalent to the ArangoDB Graph specified
        :rtype: rdflib.graph.Graph
        """

        graph_supports_quads = isinstance(rdf_graph, (RDFDataset, RDFConjunctiveGraph))

        self.__rdf_graph = rdf_graph
        self.__list_conversion = list_conversion_mode
        self.__export_options = export_options

        # Maps ArangoDB Document IDs to URI strings
        self.__uri_map: Dict[str, str] = {}
        # Maps ArangoDB Document IDs to RDFLib Terms (i.e URIRef, Literal, BNode)
        self.__term_map: Dict[str, Node] = {}

        # TODO - REVISIT
        adb_col_uri: URIRef = URIRef("http://www.arangodb.com/collection")
        self.__rdf_graph.bind("adb", "http://www.arangodb.com/")
        self.__adbg_ns = (
            adb_graph_namespace or f"{self.db._conn._url_prefixes[0]}/{name}#"
        )
        self.__rdf_graph.bind(name, self.__adbg_ns)
        self.__rdf_graph.bind("owl", "http://www.w3.org/2002/07/owl#")
        self.__rdf_graph.bind("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
        self.__rdf_graph.bind("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
        self.__rdf_graph.bind("xml", "http://www.w3.org/XML/1998/namespace")
        self.__rdf_graph.bind("xsd", "http://www.w3.org/2001/XMLSchema#")
        self.__rdf_graph.bind("dc", "http://purl.org/dc/elements/1.1/")
        self.__rdf_graph.bind("grddl", "http://www.w3.org/2003/g/data-view#")

        rdf_types = ["URIRef", "BNode", "Literal"]
        key_map: Dict[str, str] = {
            "URIRef": "_uri",
            "Literal": "_value",
            "BNode": "_key",
        }

        adb_key_blacklist = [
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
        ]

        adb_v_col_blacklist = [
            f"{name}_URIRef",
            f"{name}_BNode",
            f"{name}_Literal",
            f"{name}_UnidentifiedNode",
        ]

        doc: Json
        for col in ["Class", "Property"]:  # TODO - REVISIT
            if col in metagraph["vertexCollections"]:
                for doc in self.db.collection(col):
                    # NOTE: risk of ambiguity here...
                    # i.e "Class" -> owl:Class or rdf:Class?
                    self.__uri_map[doc["_label"]] = doc["_uri"]

        rdf_term: Union[RDFSubject, RDFObject]
        for v_col, _ in metagraph["vertexCollections"].items():
            # v_col_uri = self.__uri_map.get(v_col, f"{self.__adbg_ns}{v_col}")

            self.__setup_iterators(f"     ADB → RDF ({v_col})", "#97C423", "")
            with Live(Group(self.__adb_iterator, self.__rdf_iterator)):
                cursor = self.__fetch_adb_docs(v_col)
                self.__rdf_task = self.__rdf_iterator.add_task("", total=cursor.count())

                for doc in cursor:
                    self.__rdf_iterator.update(self.__rdf_task, advance=1)

                    rdf_type = doc.get("_rdftype", "URIRef")
                    if rdf_type not in rdf_types:  # pragma: no cover
                        raise ValueError(f"Unrecognized type {rdf_type} ({doc})")

                    id = doc.get(key_map[rdf_type], f"{self.__adbg_ns}{doc['_key']}")
                    rdf_term = getattr(sys.modules[__name__], rdf_type)(id)  # magic
                    self.__term_map[doc["_id"]] = rdf_term

                    if isinstance(rdf_term, Literal):  # RPT Case
                        continue

                    if v_col not in adb_v_col_blacklist:
                        rdf_graph.add((rdf_term, adb_col_uri, Literal(v_col)))
                        # rdf_graph.add((rdf_term, RDF.type, URIRef(v_col_uri)))

                    # TODO: Iterate through metagraph values instead?
                    for k, v in doc.items():
                        if k not in adb_key_blacklist:
                            # TODO: Should we add {v_col} to f"{self.__adbg_ns}{k}"?
                            p = self.__uri_map.get(k, f"{self.__adbg_ns}{k}")
                            self.__adb_property_to_rdf_val(rdf_term, URIRef(p), v)

        for e_col, _ in metagraph["edgeCollections"].items():
            e_col_uri = self.__uri_map.get(e_col, f"{self.__adbg_ns}{e_col}")

            self.__setup_iterators(f"     ADB → RDF ({e_col})", "#5E3108", "")
            with Live(Group(self.__adb_iterator, self.__rdf_iterator)):
                cursor = self.__fetch_adb_docs(e_col)
                self.__rdf_task = self.__rdf_iterator.add_task("", total=cursor.count())

                for doc in cursor:
                    self.__rdf_iterator.update(self.__rdf_task, advance=1)

                    subject = self.__term_map[doc["_from"]]
                    predicate = URIRef(doc.get("_uri", e_col_uri))
                    object = self.__term_map[doc["_to"]]

                    statement = (subject, predicate, object)
                    if graph_supports_quads and doc.get("_sub_graph_uri"):
                        rdf_graph.remove(statement)
                        statement += (URIRef(doc["_sub_graph_uri"]),)  # type: ignore

                    rdf_graph.add(statement)

                    # TODO: Revisit when rdflib supports RDF*
                    if reify_triples:
                        edge_has_meta = False
                        edge = URIRef(f"{self.__adbg_ns}{doc['_key']}")

                        for k, v in doc.items():
                            if k not in adb_key_blacklist:
                                edge_has_meta = True
                                # TODO: Should we add {e_col} to f"{self.__adbg_ns}{k}"?
                                p = self.__uri_map.get(k, f"{self.__adbg_ns}{k}")
                                self.__adb_property_to_rdf_val(edge, URIRef(p), v)

                        if edge_has_meta:
                            rdf_graph.add((edge, RDF.type, RDF.Statement))
                            rdf_graph.add((edge, RDF.subject, subject))
                            rdf_graph.add((edge, RDF.predicate, predicate))
                            rdf_graph.add((edge, RDF.object, object))

                            if e_col not in adb_v_col_blacklist:
                                rdf_graph.add((edge, adb_col_uri, Literal(e_col)))
                                # rdf_graph.add((edge, RDF.type, URIRef(e_col_uri)))

        # if graph_supports_quads:
        #     assert isinstance(rdf_graph, (RDFDataset, RDFConjunctiveGraph))
        #     for sg in self.__rdf_graph.graphs():
        #         # self.rdf_id_to_adb_label(str(sg.identifier()))
        #         name = str(sg.identifier.split('/')[-1])
        #         sg.serialize(name, format='trig')

        return self.__rdf_graph

    def arangodb_collections_to_rdf(
        self,
        name: str,
        rdf_graph: RDFGraph,
        v_cols: Set[str],
        e_cols: Set[str],
        list_conversion_mode: str = "static",
        adb_graph_namespace: str = "",
        reify_triples: bool = False,
        **export_options: Any,
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
            are handled andprocessed into the RDF Graph. If "collection", ArangoDB
            lists will be processed using the RDF Collection structure. If "container",
            lists found within the ArangoDB Graph will be processed using the
            RDF Container structure. If "static", elements within lists will be
            processed as individual statements. Defaults to "static".
        :type list_conversion_mode: str
        :param export_options: Keyword arguments to specify AQL query options when
            fetching documents from the ArangoDB instance. Full parameter list:
            https://docs.python-arango.com/en/main/specs.html#arango.aql.AQL.execute
        :type export_options: Any
        :return: An RDF Graph equivalent to the ArangoDB Graph specified
        :rtype: rdflib.graph.Graph
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
            adb_graph_namespace,
            reify_triples,
            **export_options,
        )

    def arangodb_graph_to_rdf(
        self,
        name: str,
        rdf_graph: RDFGraph,
        list_conversion_mode: str = "static",
        adb_graph_namespace: str = "",
        reify_triples: bool = False,
        **export_options: Any,
    ) -> RDFGraph:
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
        :param export_options: Keyword arguments to specify AQL query options when
            fetching documents from the ArangoDB instance. Full parameter list:
            https://docs.python-arango.com/en/main/specs.html#arango.aql.AQL.execute
        :type export_options: Any
        :return: An RDF Graph equivalent to the ArangoDB Graph specified
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
            adb_graph_namespace,
            reify_triples,
            **export_options,
        )

    def __adb_property_to_rdf_val(
        self,
        s: RDFSubject,
        p: URIRef,
        val: Any,
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
        """

        if type(val) is list:
            if self.__list_conversion == "collection":
                node: RDFSubject = BNode()
                self.__rdf_graph.add((s, p, node))

                rest: RDFSubject
                for i, v in enumerate(val):
                    self.__adb_property_to_rdf_val(node, RDF.first, v)

                    rest = RDF.nil if i == len(val) - 1 else BNode()
                    self.__rdf_graph.add((node, RDF.rest, rest))
                    node = rest

            elif self.__list_conversion == "container":
                bnode = BNode()
                self.__rdf_graph.add((s, p, bnode))

                for i, v in enumerate(val, 1):
                    _n = URIRef(f"{str(RDF)}_{i}")
                    self.__adb_property_to_rdf_val(bnode, _n, v)

            elif self.__list_conversion == "static":
                for v in val:
                    self.__adb_property_to_rdf_val(s, p, v)

            else:
                raise ValueError("Invalid **list_conversion_mode value")

        elif type(val) is dict:
            bnode = BNode()
            self.__rdf_graph.add((s, p, bnode))

            for k, v in val.items():
                p_str = self.__uri_map.get(k, f"{self.__adbg_ns}{k}")
                self.__adb_property_to_rdf_val(bnode, URIRef(p_str), v)

        else:
            # TODO: Datatype? Lang?
            self.__rdf_graph.add((s, p, Literal(val)))

    def __setup_iterators(
        self, rdf_iter_text: str, rdf_iter_color: str, adb_iter_text: str
    ) -> None:
        self.__rdf_iterator = rdf_track(rdf_iter_text, rdf_iter_color)
        self.__adb_iterator = adb_track(adb_iter_text)

    def __add_adb_edge(
        self,
        col: str,
        key: str,
        _from: str,
        _to: str,
        _uri: str,
        _label: str,
        _sg_uri: str,
    ) -> None:
        self.adb_docs[col][key] = {
            "_key": key,
            "_from": _from,
            "_to": _to,
            "_uri": _uri,
            "_label": _label,
            "_sub_graph_uri": _sg_uri,
        }

    def __build_domain_and_range_map(self) -> None:
        blacklist = [str(RDFS.Literal), str(RDFS.Resource)]

        for p in ["domain", "range"]:
            t = (None, RDFS[p], None)

            for s, _, o, *_ in self.__rdf_statements(t):  # type: ignore
                o_str = str(o)

                if o_str not in blacklist:
                    self.__domain_range_map[s][p] = {
                        "key": self.rdf_id_to_adb_key(o_str),
                        "str": o_str,
                    }

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

    def __insert_adb_docs(self, adb_col_blacklist: Set[str] = set()) -> None:
        """Insert ArangoDB documents into their ArangoDB collection.

        :param adb_col_blacklist: A list of ArangoDB Collections that will not be
            populated on this call of __insert_adb_docs. Essential for allowing List
            construction of RDF Literals.
        :type adb_col_blacklist: Set[str]
        """
        for adb_col in list(self.adb_docs.keys()):
            if adb_col in adb_col_blacklist:
                continue

            action = f"ArangoDB Import: {adb_col}"
            adb_task = self.__adb_iterator.add_task("", action=action)

            docs = list(self.adb_docs[adb_col].values())
            if docs == []:
                continue  # pragma: no cover

            if not self.db.has_collection(adb_col):
                is_edge = {"_from", "_to"} <= docs[0].keys()
                self.db.create_collection(adb_col, edge=is_edge)

            self.db.collection(adb_col).import_bulk(docs, **self.__import_options)
            del self.adb_docs[adb_col]

            self.__adb_iterator.stop_task(adb_task)
            self.__adb_iterator.update(adb_task, visible=False)

        gc.collect()
