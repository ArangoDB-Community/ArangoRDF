#!/usr/bin/env python3
import logging
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
from rdflib import BNode
from rdflib import ConjunctiveGraph as RDFConjunctiveGraph
from rdflib import Dataset as RDFDataset
from rdflib import Graph as RDFGraph
from rdflib import Literal, URIRef
from rdflib.namespace import RDF, RDFS
from rdflib.term import Node
from rich.console import Group
from rich.live import Live

from .abc import Abstract_ArangoRDF
from .typings import ADBDocs, ADBMetagraph, Json, RDFLists, RDFObject, RDFSubject
from .utils import adb_track, logger, rdf_track


class ArangoRDF(Abstract_ArangoRDF):
    """ArangoRDF

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
        self.adb_docs: ADBDocs = defaultdict(lambda: defaultdict(dict))

        logger.info(f"Instantiated ArangoRDF with database '{db.name}'")

    def set_logging(self, level: Union[int, str]) -> None:
        logger.setLevel(level)

    def rdf_to_arangodb_by_rpt(
        self,
        name: str,
        rdf_graph: RDFGraph,
        overwrite_graph: bool = False,
        load_base_ontology: bool = False,
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
        :param load_base_ontology: Imports the arango_rdf/ontologies/rdfowl.ttl
            to the ArangoDB graph to serve as a base ontology. Defaults to False.
        :type load_base_ontology: bool
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

        # Reset Config
        self.adb_docs.clear()
        self.__import_options = import_options
        self.__import_options["on_duplicate"] = "update"

        self.__URIREF_COL = f"{name}_URIRef"
        self.__BNODE_COL = f"{name}_BNode"
        self.__LITERAL_COL = f"{name}_Literal"
        self.__STATEMENT_COL = f"{name}_Statement"

        if overwrite_graph:
            self.db.delete_graph(name, ignore_missing=True, drop_collections=True)

        if load_base_ontology:
            # TODO: Should we call load_base_rpt_ontology as well?
            rdf_graph.parse(f"{Path(__file__).parent}/ontologies/rdfowl.ttl")

        size = len(rdf_graph)
        if batch_size is None:
            batch_size = size

        self.__setup_iterators("RDF → ADB (RPT)", "#08479E", "    ")
        with Live(Group(self.__rdf_iterator, self.__adb_iterator)):
            rdf_task = self.__rdf_iterator.add_task("", total=size)

            s: RDFSubject  # Subject
            p: URIRef  # Predicate
            o: RDFObject  # Object
            sg: URIRef  # Sub Graph

            for i, (s, p, o, *sg) in enumerate(rdf_graph, 1):  # type: ignore
                self.__rdf_iterator.update(rdf_task, advance=1)

                s_col, s_key = self.__process_rpt_term(s)
                o_col, o_key = self.__process_rpt_term(o)

                p_str = str(p)
                p_key = self._rdf_id_to_adb_key(p_str)
                e_key = f"{s_key}-{p_key}-{o_key}"

                self.adb_docs[self.__STATEMENT_COL][e_key] = {
                    "_key": e_key,
                    "_from": f"{s_col}/{s_key}",
                    "_to": f"{o_col}/{o_key}",
                    "_uri": p_str,
                    "_label": p_key,
                    "_sub_graph_uri": str(sg[0]) if sg else "",
                }

                if i % batch_size == 0:
                    self.__insert_adb_docs()

            self.__insert_adb_docs()

        return self.__create_rpt_adb_graph(name)

    def __process_rpt_term(self, t: Union[RDFSubject, RDFObject]) -> Tuple[str, str]:
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
            t_key = self._rdf_id_to_adb_key(t_str)

            self.adb_docs[t_col][t_key] = {
                "_key": t_key,
                "_uri": t_str,
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
                "_lang": t_lang,
                "_datatype": t_datatype,
                "_rdftype": "Literal",
            }

        else:
            raise ValueError()  # pragma: no cover

        return t_col, t_key

    def __create_rpt_adb_graph(self, name: str) -> ADBGraph:
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
        load_base_ontology: bool = False,
        batch_size: Optional[int] = None,
        adb_collection_uri: URIRef = URIRef("http://www.arangodb.com/collection"),
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
        :param load_base_ontology: TODO - properly define
        :type load_base_ontology: bool
        :param batch_size: If specified, runs the ArangoDB Data Import process for every
            **batch_size** triples within the **rdf_graph**.
            If unspecified, **batch_size** will be set to `len(rdf_graph)`.
        :type batch_size: int
        :param adb_collection_uri: TODO - properly define
        :type adb_collection_uri: URIRef
        :param import_options: Keyword arguments to specify additional
            parameters for ArangoDB document insertion. Full parameter list:
            https://docs.python-arango.com/en/main/specs.html#arango.collection.Collection.import_bulk
        :type import_options: Any

        :return: The ArangoDB Graph API wrapper.
        :rtype: arango.graph.Graph
        """

        # Reset config
        self.adb_docs.clear()
        self.__rdf_graph = rdf_graph
        self.__import_options = import_options
        self.__import_options["on_duplicate"] = "update"

        self.__process_value_as_string = False
        self.__adb_col_blacklist: Set[str] = set()
        self.__rdf_lists: RDFLists = defaultdict(lambda: defaultdict(dict))

        # Maps URI strings to ArangoDB Collection names
        self.__col_map: Dict[str, str] = {}
        # Stores unidentified resources
        self.__UNIDENTIFIED_NODE_COL = f"{name}_UnidentifiedNode"
        # Stores edge definitions
        self.__e_col_map: DefaultDict[str, DefaultDict[str, Set[str]]]
        self.__e_col_map = defaultdict(lambda: defaultdict(set))

        if overwrite_graph:
            self.db.delete_graph(name, ignore_missing=True, drop_collections=True)

        if load_base_ontology:
            # TODO: Re-enable when tests are adjusted...
            # rdf_graph.parse(f"{Path(__file__).parent}/ontologies/rdfowl.ttl")
            self.__load_pgt_ontology()
            self.__e_col_map["type"]["from"].add("Property")
            self.__e_col_map["type"]["to"].add("Class")

        size = len(rdf_graph)
        if batch_size is None:
            batch_size = size

        ############## Pre Processing ##############
        self.__setup_iterators("RDF → ADB (PGT Pre-Process)", "#BD0F89", "    ")
        with Live(Group(self.__rdf_iterator, self.__adb_iterator)):
            self.__build_pgt_class_map(load_base_ontology, adb_collection_uri)
            self.__insert_adb_docs()
        ############## ############## ##############

        ############## PGT Processing ##############
        self.__setup_iterators("RDF → ADB (PGT)", "#08479E", "    ")
        with Live(Group(self.__rdf_iterator, self.__adb_iterator)):
            rdf_task = self.__rdf_iterator.add_task("", total=size)

            s: RDFSubject  # Subject
            p: URIRef  # Predicate
            o: RDFObject  # Object

            for i, (s, p, o, *sub_graph) in enumerate(rdf_graph, 1):  # type: ignore
                self.__rdf_iterator.update(rdf_task, advance=1)

                # TODO: Discuss repercussions
                if p == adb_collection_uri:
                    continue

                # TODO: Discuss repercussions
                if o in [RDF.nil, RDF.Alt, RDF.Bag, RDF.List, RDF.Seq]:  # HACK ?
                    continue

                s_str = str(s)
                p_str = str(p)
                o_str = str(o)

                p_metadata = self.__get_rdf_pgt_metadata(p_str, is_predicate=True)
                _, p_key, _ = p_metadata

                rdf_list_col = self.__statement_is_part_of_rdf_list(s, p_str)
                if rdf_list_col:
                    doc = self.__rdf_lists[rdf_list_col][s_str]
                    self.__rdf_val_to_adb_doc_property(doc, p_key, o)

                else:
                    s_metadata = self.__get_rdf_pgt_metadata(s_str)
                    o_metadata = self.__get_rdf_pgt_metadata(o_str)

                    sg = str(sub_graph[0]) if sub_graph else ""

                    self.__process_pgt_subject(s, s_metadata)
                    self.__process_pgt_object(s_metadata, p_metadata, o, o_metadata, sg)
                    self.__process_pgt_edge(s_metadata, p_metadata, o, o_metadata, sg)

                    # THIS IS A HACK
                    # replaces `self.__adb_col_blacklist.add(s_col)`
                    # len(list(self.__rdf_graph.objects(s, p))) > 1:
                    # sum(1 for _ in self.__rdf_graph.objects(s, p)) > 1
                    # if type(o) is Literal and ___:
                    #     self.__adb_col_blacklist.add(s_metadata[-1])

                    if load_base_ontology:
                        self.adb_docs["Property"][p_key] = {
                            **self.adb_docs["Property"][p_key],
                            "_key": p_key,
                            "_uri": p_str,
                            "_rdftype": "URIRef",
                        }

                        e_key = f"{p_key}-type-Property"
                        self.adb_docs["type"][e_key] = {
                            "_key": e_key,
                            "_from": f"Property/{p_key}",
                            "_to": "Class/Property",
                            "_uri": str(RDF.type),
                            "_label": "type",
                            "_sub_graph_uri": "",
                        }

                if i % batch_size == 0:
                    self.__insert_adb_docs(self.__adb_col_blacklist)

        ############## ############## ##############

        ############## Post Processing ##############
        self.__setup_iterators("RDF → ADB (PGT Post-Process)", "#EF7D00", "    ")
        with Live(Group(self.__rdf_iterator, self.__adb_iterator)):
            self.__process_rdf_lists()
            self.__insert_adb_docs()
        ############## ############### ##############

        if self.db.has_collection(self.__UNIDENTIFIED_NODE_COL):
            count = self.db.collection(self.__UNIDENTIFIED_NODE_COL).count()
            logger.info(
                f"""\n
                ----------------
                UnidentifiedNodes found in graph '{name}'.
                No `rdf:type` statement found for the
                following number of URIRefs/BNodes: {count}.
                ----------------
                """
            )

        return self.__create_pgt_adb_graph(name)

    def __load_pgt_ontology(self) -> None:
        """Loads the baseline ontology required to support the
        rdfowl.ttl file ontology that is imported upon setting
        **load_base_ontology to True.

        Introduces 3 new documents ("Class/Class", "Class/Property", "Property/type")
        along with 3 new RDF.type edges.

        Also updates the __class_map dictionary accordingly to map the
        RDF.Class, RDF.Property, and RDF.type URIs to their associated
        ArangoDB collections.
        """

        class_str = str(RDFS.Class)
        self.__col_map[class_str] = "Class"
        self.adb_docs["Class"]["Class"] = {
            "_key": "Class",
            "_uri": class_str,
            "_rdftype": "URIRef",
        }

        property_str = str(RDF.Property)
        self.__col_map[property_str] = "Class"
        self.adb_docs["Class"]["Property"] = {
            "_key": "Property",
            "_uri": property_str,
            "_rdftype": "URIRef",
        }

        type_str = str(RDF.type)
        self.__col_map[type_str] = "Property"
        self.adb_docs["Property"]["type"] = {
            "_key": "type",
            "_uri": type_str,
            "_rdftype": "URIRef",
        }

        self.adb_docs["type"]["type-type-Property"] = {
            "_key": "type-type-Property",
            "_from": "Property/type",
            "_to": "Class/Property",
            "_uri": type_str,
            "_label": "type",
        }

        self.adb_docs["type"]["Property-type-Class"] = {
            "_key": "Property-type-Class",
            "_from": "Class/Property",
            "_to": "Class/Class",
            "_uri": type_str,
            "_label": "type",
        }

        self.adb_docs["type"]["Class-type-Class"] = {
            "_key": "Class-type-Class",
            "_from": "Class/Class",
            "_to": "Class/Class",
            "_uri": type_str,
            "_label": "type",
        }

    def __build_pgt_class_map(
        self, load_base_ontology: bool, adb_collection_uri: URIRef
    ) -> None:
        """A pre-processing step that iterates through the RDF Graph
        statements to build a URI-to-ArangoDB-Collection mapping of all
        RDF Nodes within the graph.

        This step is required for the PGT Processing stage in order to
        ensure each RDF Node is properly identified and categorized under
        a specific ArangoDB collection.

        :param load_base_ontology: TODO-define properly
        :type load_base_ontology: bool
        :param adb_collection_uri: TODO-define properly
        :type adb_collection_uri: URIRef
        """

        adb_col_map: Dict[str, str] = dict()
        type_map: DefaultDict[str, Set[str]] = defaultdict(set)
        subclass_map: DefaultDict[str, Set[str]] = defaultdict(set)

        rdf_task = self.__rdf_iterator.add_task("", total=len(self.__rdf_graph))
        for s, p, o, *_ in self.__rdf_graph:
            self.__rdf_iterator.update(rdf_task, advance=1)

            # TODO: Discuss Repurcssions
            if o in [RDF.nil, RDF.Alt, RDF.Bag, RDF.List, RDF.Seq]:  # HACK ?
                continue

            s_str = str(s)
            p_str = str(p)
            o_str = str(o)

            if load_base_ontology:
                type_map[p_str].add("Property")

            if p == adb_collection_uri:
                if not isinstance(o, Literal):
                    raise ValueError(f"Object {o} must be Literal")  # pragma: no cover

                if s_str in adb_col_map:
                    # TODO: Create custom error
                    raise ValueError(  # pragma: no cover
                        f"""
                        Subject {s} can only have 1 ArangoDB Collection association.
                        Found '{adb_col_map[s_str]}' and '{o_str}'.
                        """
                    )

                adb_col_map[s_str] = o_str

            elif p == RDF.type:
                type_map[s_str].add(o_str)

                if load_base_ontology:
                    type_map[o_str].add("Class")

                    o_key = self._rdf_id_to_adb_key(o_str)
                    e_key = f"{o_key}-type-Class"
                    self.adb_docs["type"][e_key] = {
                        "_key": e_key,
                        "_from": f"Class/{o_key}",
                        "_to": "Class/Class",
                        "_uri": str(RDF.type),
                        "_label": "type",
                        "_sub_graph_uri": "",
                    }

            elif p == RDFS.subClassOf:
                subclass_map[s_str].add(o_str)
                type_map[s_str].add("Class")
                type_map[o_str].add("Class")

            elif p == RDFS.subPropertyOf:
                type_map[s_str].add("Property")
                type_map[o_str].add("Property")

        # Helper function to recursively iterate through `subclass_map`
        def get_depth(class_str: str, depth: int) -> int:
            if class_str not in subclass_map:
                return depth

            for sub_class_str in subclass_map[class_str]:
                if sub_class_str == class_str:
                    return depth

                return get_depth(sub_class_str, depth + 1)

            return -1  # pragma: no cover

        rdf_task = self.__rdf_iterator.add_task("", total=len(type_map))
        for s_str, o_str_set in type_map.items():
            self.__rdf_iterator.update(rdf_task, advance=1)

            # Case 1 (Only one type statement associated to s_str)
            if len(o_str_set) == 1:
                self.__col_map[s_str] = self._rdf_id_to_adb_key(o_str_set.pop())

            # Case 2 (ArangoDB Collection Property)
            elif s_str in adb_col_map:
                self.__col_map[s_str] = adb_col_map[s_str]

            # Case 3 (Taxonomy)
            elif any([o_str in subclass_map for o_str in o_str_set]):
                max_depth = -1
                best_class = ""
                for o_str in o_str_set:
                    depth = get_depth(o_str, 0)

                    if depth > max_depth:
                        max_depth = depth
                        best_class = o_str

                self.__col_map[s_str] = self._rdf_id_to_adb_key(best_class)

            # Case 4 (Multiple types without adb_col_map or sub_class_map entry)
            else:
                self.__col_map[s_str] = self._rdf_id_to_adb_key(sorted(o_str_set)[0])

    def __get_rdf_pgt_metadata(
        self, term_str: str, is_predicate: bool = False
    ) -> Tuple[str, str, str]:
        """Return PGY-relevant metadata associated to the RDF Term.

        :param term_str: The string representation of the RDF Term
        :type term_str: str
        :param is_predicate: Set to true if **term_str represents a predicate
        :return: The string representation, ArangoDB Document Key, and
        ArangoDB Collection name of the RDF Term.
        :rtype: Tuple[str, str, str]
        """
        term_key = self._rdf_id_to_adb_key(term_str)

        if is_predicate:
            return term_str, term_key, term_key

        term_col = self.__col_map.get(term_str, self.__UNIDENTIFIED_NODE_COL)
        return term_str, term_key, term_col

    def __process_pgt_term(
        self,
        t: Union[RDFSubject, RDFObject],
        t_metadata: Tuple[str, str, str],
        s_key: str = "",
        s_col: str = "",
        p_key: str = "",
    ) -> None:
        """Process an RDF Term as an ArangoDB document by PGT.

        :param t: The RDF Term
        :type t: URIRef | BNode | Literal
        :param t_metadata: The PGT Metadata associated to the RDF Term.
        :type t_metadata: Tuple[str, str, str]
        :param s_key: The ArangoDB document key of the Subject associated
            to the RDF Term **t**. Only required if the RDF Term is of type Literal.
        :type s_key: str
        :param s_col: The ArangoDB document key of the Subject associated
            to the RDF Term **t**. Only required if the RDF Term is of type Literal.
        :type s_col: str
        :param p_key: The ArangoDB document key of the Predicate associated
            to the RDF Term **t**. Only required if the RDF Term is of type Literal.
        :type p_key: str
        """

        t_str, t_key, t_col = t_metadata

        if isinstance(t, URIRef):
            self.adb_docs[t_col][t_key] = {
                **self.adb_docs[t_col][t_key],
                "_key": t_key,
                "_uri": t_str,
                "_rdftype": "URIRef",
            }

        elif isinstance(t, BNode):
            self.adb_docs[t_col][t_key] = {
                **self.adb_docs[t_col][t_key],
                "_key": t_key,
                "_rdftype": "BNode",
            }

        elif isinstance(t, Literal) and all([s_col, s_key, p_key]):
            doc = self.adb_docs[s_col][s_key]
            t_value = t_str if isinstance(t.value, date) else t.value or t_str
            self.__rdf_val_to_adb_doc_property(doc, p_key, t_value)

            self.__adb_col_blacklist.add(s_col)  # TODO: REVISIT

        else:
            raise ValueError()  # pragma: no cover

    def __rdf_val_to_adb_doc_property(self, doc: Json, p_key: str, val: Any) -> None:
        """A helper function used to insert an arbitrary RDF value
        as a document property of some arbitrary document.

        If `self.__process_value_as_string` is enabled, the RDF
        value is appended to a string representation of the
        current value of the document property (instead of relying
        on a list structure).

        :param doc: An arbitrary document
        :type doc: Dict[str, Any]
        :param p_key: The property key that will be
            used to store the value.
        :type p_key: str
        :param val: Some document property value to insert.
        :type val: Any
        """

        # This flag is set active in ArangoRDF.__process_rdf_lists()
        if self.__process_value_as_string:
            doc[p_key] += f"'{val}'," if type(val) is str else f"{val},"
            return

        try:
            # Assume p_key is a valid key (#1) and points to a list (#2)
            doc[p_key].append(val)
        except KeyError:
            # Catch assumption #1
            doc[p_key] = val
        except AttributeError:
            # Catch assumption #2
            doc[p_key] = [doc[p_key], val]

    def __process_pgt_subject(
        self, s: RDFSubject, s_metadata: Tuple[str, str, str]
    ) -> None:
        """A wrapper over the function `__process_pgt_term` for easier
        code readability. Processes the RDF Subject into ArangoDB.

        :param s: The RDF Subject to process into ArangoDB
        :type s: URIRef | BNode
        :param s_metadata: The PGT Metadata associated to the RDF Subject.
        :type s_metadata: Tuple[str, str, str]
        """
        self.__process_pgt_term(s, s_metadata)

    def __process_pgt_object(
        self,
        s_metadata: Tuple[str, str, str],
        p_metadata: Tuple[str, str, str],
        o: RDFObject,
        o_metadata: Tuple[str, str, str],
        sg: str,
    ) -> None:
        """Processes the RDF Object into ArangoDB. Given the possibily of
        the RDF Object being used as the "root" of an RDF Collection or
        an RDF Container (i.e an RDF List), this wrapper function is used
        to prevent calling `__process_pgt_term` if it is not required.

        :param s_metadata: The PGT Metadata associated to the
            RDF Subject of the statement containing the RDF Object.
        :type s_metadata: Tuple[str, str, str]
        :param p_metadata: The PGT Metadata associated to the
            RDF Predicate of the statement containing the RDF Object.
        :type p_metadata: Tuple[str, str, str]
        :param o: The RDF Object to process into ArangoDB.
        :type o: URIRef | BNode | Literal
        :param o_metadata: The PGT Metadata associated to the RDF Object.
        :type o_metadata: Tuple[str, str, str]
        :param sg: The string representation of the sub-graph URIRef associated
            to this statement (if any).
        :type sg: str
        """

        s_str, s_key, s_col = s_metadata
        p_str, p_key, _ = p_metadata

        if self.__object_is_head_of_rdf_list(o):
            self.__rdf_lists["_LIST_HEAD"][s_str][p_str] = {
                "root": o,
                "sub_graph": sg,
            }

        else:
            self.__process_pgt_term(o, o_metadata, s_key, s_col, p_key)

    def __process_pgt_edge(
        self,
        s_metadata: Tuple[str, str, str],
        p_metadata: Tuple[str, str, str],
        o: RDFObject,
        o_metadata: Tuple[str, str, str],
        sg: str,
    ) -> None:
        """Processes the RDF Statement as an edge into ArangoDB.

        An edge is only created if:
            1) The RDF Object within the RDF Statement is not a Literal
            2) The RDF Object is not the "root" node of an RDF List structure

        :param s_metadata: The PGT Metadata associated to the
            RDF Subject of the statement containing the RDF Object.
        :type s_metadata: Tuple[str, str, str]
        :param p_metadata: The PGT Metadata associated to the
            RDF Predicate of the statement containing the RDF Object.
        :type p_metadata: Tuple[str, str, str]
        :param o: The RDF Object to process into ArangoDB.
        :type o: URIRef | BNode | Literal
        :param o_metadata: The PGT Metadata associated to the RDF Object.
        :type o_metadata: Tuple[str, str, str]
        :param sg: The string representation of the sub-graph URIRef associated
            to this statement (if any).
        :type sg: str
        """
        _, s_key, s_col = s_metadata
        p_str, p_key, p_col = p_metadata
        _, o_key, o_col = o_metadata

        if not isinstance(o, Literal) and not self.__object_is_head_of_rdf_list(o):
            e_key = f"{s_key}-{p_key}-{o_key}"
            self.adb_docs[p_col][e_key] = {
                "_key": e_key,
                "_from": f"{s_col}/{s_key}",
                "_to": f"{o_col}/{o_key}",
                "_uri": p_str,
                "_label": p_key,
                "_sub_graph_uri": sg,
            }

            self.__e_col_map[p_col]["from"].add(s_col)
            self.__e_col_map[p_col]["to"].add(o_col)

    def __object_is_head_of_rdf_list(self, o: RDFObject) -> bool:
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

    def __statement_is_part_of_rdf_list(self, s: RDFSubject, p_str: str) -> str:
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

        if isinstance(s, BNode):  # TODO: Discuss repercussions of this assumption
            _n = r"^http://www.w3.org/1999/02/22-rdf-syntax-ns#_[0-9]{1,}$"
            li = r"^http://www.w3.org/1999/02/22-rdf-syntax-ns#li$"

            is_part_of_collection = p_str in [str(RDF.first), str(RDF.rest)]
            is_part_of_container = any([re.match(_n, p_str), re.match(li, p_str)])

            if is_part_of_collection:
                return "_COLLECTION_BNODE"

            if is_part_of_container:
                return "_CONTAINER_BNODE"

        return ""

    def __process_rdf_lists(self) -> None:
        """A helper function to help process all RDF Collections & Containers
        within the RDF Graph prior to inserting the documents into ArangoDB.

        This function relies on a Dictionary/Linked-List representation of the
        RDF Lists. This representation is stored via the "_LIST_HEAD",
        "_CONTAINER_BNODE", and "_COLLECTION_BNODE" keys within `self.adb_docs`.

        Given the recursive nature of these RDF Lists, we rely on
        recursion via the `__process_rdf_list_object`, `__unpack_rdf_collection`,
        and `__unpack_rdf_container` functions.

        NOTE: A form of string manipulation is used if Literals are
        present within the RDF List. For example, given the RDF Statement
        ```ex:Doc ex:numbers (1 (2 3)) .```, the equivalent ArangoDB List is
        constructed via a string-based solution:
        "[" → "[1" → "[1, [" → "[1, [2," → "[1, [2, 3" → "[1, [2, 3]" → "[1, [2, 3]]"
        """
        self.__process_value_as_string = True

        list_heads = self.__rdf_lists["_LIST_HEAD"].items()
        rdf_task = self.__rdf_iterator.add_task("", total=len(list_heads))

        for s_str, s_dict in list_heads:
            self.__rdf_iterator.update(rdf_task, advance=1)

            s_metadata = self.__get_rdf_pgt_metadata(s_str)
            _, s_key, s_col = s_metadata

            for p_str, p_dict in s_dict.items():
                p_metadata = self.__get_rdf_pgt_metadata(p_str, is_predicate=True)
                _, p_key, _ = p_metadata

                doc = self.adb_docs[s_col][s_key]
                doc["_key"] = s_key  # NOTE: Is this really necessary?

                root: RDFObject = p_dict["root"]
                sg: str = p_dict["sub_graph"]

                doc[p_key] = ""
                self.__process_rdf_list_object(doc, s_metadata, p_metadata, root, sg)
                doc[p_key] = doc[p_key].rstrip(",")

                # Delete doc[p_key] if there are no Literals within the List
                if set(doc[p_key]) == {"[", "]"}:
                    del doc[p_key]
                else:
                    doc[p_key] = literal_eval(doc[p_key])

        self.__rdf_lists.clear()

    def __process_rdf_list_object(
        self,
        doc: Json,
        s_metadata: Tuple[str, str, str],
        p_metadata: Tuple[str, str, str],
        o: RDFObject,
        sg: str,
    ) -> None:
        """Given an ArangoDB Document, and the RDF List Statement represented
        by `s_metadata, p_metadata, o`, process the value of the object **o**
        into the ArangoDB Document.

        If the Object is part of an RDF Collection Data Structure,
        rely on the recursive `__unpack_rdf_collection` function.

        If the Object is part of an RDF Container Data Structure,
        rely on the recursive `__unpack_rdf_container` function.

        If the Object is none of the above, then it is considered
        as a processable entity.

        :param doc: The ArangoDB Document associated to the RDF List.
        :type doc: Dict[str, Any]
        :param s_metadata: The PGT Metadata associated to the RDF Subject.
        :type s_metadata: Tuple[str, str, str]
        :param p_metadata: The PGT Metadata associated to the RDF Predicate.
        :type p_metadata: Tuple[str, str, str]
        :param o: The RDF List Object to process into ArangoDB.
        :type o: URIRef | BNode | Literal
        :param sg: The string representation of the sub-graph URIRef associated
            to the RDF List Statement (if any).
        :type sg: str
        """
        o_str = str(o)

        if o_str in self.__rdf_lists["_COLLECTION_BNODE"]:
            _, p_key, _ = p_metadata
            doc[p_key] += "["

            next_bnode_dict = self.__rdf_lists["_COLLECTION_BNODE"][o_str]
            self.__unpack_rdf_collection(
                doc, s_metadata, p_metadata, next_bnode_dict, sg
            )

            doc[p_key] = str(doc[p_key]).rstrip(",") + "],"

        elif o_str in self.__rdf_lists["_CONTAINER_BNODE"]:
            _, p_key, _ = p_metadata
            doc[p_key] += "["

            next_bnode_dict = self.__rdf_lists["_CONTAINER_BNODE"][o_str]
            self.__unpack_rdf_container(
                doc, s_metadata, p_metadata, next_bnode_dict, sg
            )

            doc[p_key] = str(doc[p_key]).rstrip(",") + "],"

        elif o_str:
            o_metadata = self.__get_rdf_pgt_metadata(o_str)
            self.__process_pgt_object(s_metadata, p_metadata, o, o_metadata, sg)
            self.__process_pgt_edge(s_metadata, p_metadata, o, o_metadata, sg)

    def __unpack_rdf_collection(
        self,
        doc: Json,
        s_metadata: Tuple[str, str, str],
        p_metadata: Tuple[str, str, str],
        bnode_dict: Dict[str, RDFObject],
        sg: str,
    ) -> None:
        """A recursive function that disassembles the structure of the
        RDF Collection, most notably known for its "first" & "rest" structure.

        :param doc: The ArangoDB Document associated to the RDF Collection.
        :type doc: Dict[str, Any]
        :param s_metadata: The PGT Metadata associated to the RDF Subject.
        :type s_metadata: Tuple[str, str, str]
        :param p_metadata: The PGT Metadata associated to the RDF Predicate.
        :type p_metadata: Tuple[str, str, str]
        :param bnode_dict: A dictionary mapping the RDF.First and RDF.Rest
            values associated to the current BNode of the RDF Collection.
        :type bnode_dict: Dict[str, URIRef | BNode | Literal]
        :param sg: The string representation of the sub-graph URIRef associated
            to the RDF List Statement (if any).
        :type sg: str
        """

        first: RDFObject = bnode_dict["first"]
        self.__process_rdf_list_object(doc, s_metadata, p_metadata, first, sg)

        if "rest" in bnode_dict:
            rest = bnode_dict["rest"]
            next_bnode_dict = self.__rdf_lists["_COLLECTION_BNODE"][str(rest)]
            self.__unpack_rdf_collection(
                doc, s_metadata, p_metadata, next_bnode_dict, sg
            )

    def __unpack_rdf_container(
        self,
        doc: Json,
        s_metadata: Tuple[str, str, str],
        p_metadata: Tuple[str, str, str],
        bnode_dict: Dict[str, Union[RDFObject, List[RDFObject]]],
        sg: str,
    ) -> None:
        """A recursive function that disassembles the structure of the
        RDF Container, most notably known for its linear structure
        (i.e rdf:li & rdf:_n properties)

        :param doc: The ArangoDB Document associated to the RDF Collection.
        :type doc: Dict[str, Any]
        :param s_metadata: The PGT Metadata associated to the RDF Subject.
        :type s_metadata: Tuple[str, str, str]
        :param p_metadata: The PGT Metadata associated to the RDF Predicate.
        :type p_metadata: Tuple[str, str, str]
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
                self.__process_rdf_list_object(doc, s_metadata, p_metadata, o, sg)

    def __create_pgt_adb_graph(self, name: str) -> ADBGraph:
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

    def _rdf_id_to_adb_key(self, rdf_id: str) -> str:
        """Convert an RDF Resource ID string into an ArangoDB Key. For example,
        _rdf_id_to_adb_key("http://www.w3.org/2000/01/rdf-schema#Class") returns
        "Class", and _rdf_id_to_adb_key("n027d9ad4c88b418cb8436f2408f33c8cb3")
        returns "n027d9ad4c88b418cb8436f2408f33c8cb3".

        :param uri_str: The string representation of a URIRef
        :type uri_str: str
        :return: The suffix of the URI String
        :rtype: str
        """
        # NOTE: What if we have two namespaces with the same RDF Label?
        str = re.split("/|#", rdf_id)[-1] or rdf_id
        return self._string_to_arangodb_key(str)

    def _string_to_arangodb_key(self, string: str) -> str:
        """Given a string, derive a valid ArangoDB _key string.
        If unable to derive a valid _key, return hash of original
        string.

        :param string: A (possibly) invalid _key string value.
        :type string: str
        :return: A valid ArangoDB _key value.
        :rtype: str
        """
        res: str = ""
        for s in string:
            if s.isalnum() or s in self.VALID_ADB_KEY_CHARS:
                res += s

        if not res:
            return str(hash(res))  # pragma: no cover

        return res

    def arangodb_to_rdf(
        self,
        name: str,
        rdf_graph: RDFGraph,
        metagraph: ADBMetagraph,
        list_conversion_mode: str = "collection",
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
            processed as individual statements. Defaults to "collection".
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

        # Maps ArangoDB Document Keys to URI strings
        self.__uri_map: Dict[str, str] = {}
        # Maps ArangoDB Document IDs to RDFLib Terms (i.e URIRef, Literal, BNode)
        self.__term_map: Dict[str, Node] = {}

        self.__base_namespace = "http://www.arangodb.com"
        rdf_types = ["URIRef", "BNode", "Literal"]
        key_map: Dict[str, str] = {
            "URIRef": "_uri",
            "Literal": "_value",
            "BNode": "_key",
        }

        adb_key_blacklist = ["_id", "_key", "_rev", "_rdftype", "_uri", "_value"]
        adb_v_col_blacklist = [
            f"{name}_URIRef",
            f"{name}_BNode",
            f"{name}_Literal",
            f"{name}_UnidentifiedNode",
        ]

        doc: Json
        for col in ["Class", "Property"]:  # TODO: Name TBD?
            if col in metagraph["vertexCollections"]:
                for doc in self.db.collection(col):
                    self.__uri_map[doc["_key"]] = doc["_uri"]

        rdf_term: Union[RDFSubject, RDFObject]
        for v_col, _ in metagraph["vertexCollections"].items():
            v_col_uri_str = f"{self.__base_namespace}/{v_col}"
            v_col_rdf_class = URIRef(self.__uri_map.get(v_col, v_col_uri_str))

            self.__setup_iterators(f"     ADB → RDF ({v_col})", "#97C423", "")
            with Live(Group(self.__adb_iterator, self.__rdf_iterator)):
                cursor = self.__fetch_adb_docs(v_col)
                rdf_task = self.__rdf_iterator.add_task("", total=cursor.count())

                for doc in cursor:
                    self.__rdf_iterator.update(rdf_task, advance=1)

                    rdf_type = doc.get("_rdftype", "URIRef")

                    if rdf_type not in rdf_types:  # pragma: no cover
                        raise ValueError(f"Unrecognized type {rdf_type} ({doc})")

                    id = doc.get(key_map[rdf_type], f"{v_col_uri_str}_{doc['_key']}")
                    rdf_term = getattr(sys.modules[__name__], rdf_type)(id)
                    self.__term_map[doc["_id"]] = rdf_term

                    if isinstance(rdf_term, Literal):  # RPT Case
                        continue

                    if v_col not in adb_v_col_blacklist:  # HACK?
                        rdf_graph.add((rdf_term, RDF.type, v_col_rdf_class))

                    # TODO: Iterate through metagraph values instead?
                    for k, v in doc.items():
                        if k not in adb_key_blacklist:  # HACK?
                            p = self.__uri_map.get(k, f"{self.__base_namespace}/{k}")
                            self.__adb_doc_property_to_rdf_val(rdf_term, URIRef(p), v)

        for e_col, _ in metagraph["edgeCollections"].items():
            e_col_uri_str = f"{self.__base_namespace}/{e_col}"

            self.__setup_iterators(f"     ADB → RDF ({e_col})", "#5E3108", "")
            with Live(Group(self.__adb_iterator, self.__rdf_iterator)):
                cursor = self.__fetch_adb_docs(e_col)
                rdf_task = self.__rdf_iterator.add_task("", total=cursor.count())

                for doc in cursor:
                    self.__rdf_iterator.update(rdf_task, advance=1)

                    statement = (
                        self.__term_map[doc["_from"]],
                        URIRef(doc.get("_uri", e_col_uri_str)),
                        self.__term_map[doc["_to"]],
                    )

                    if graph_supports_quads and doc.get("_sub_graph_uri"):
                        rdf_graph.remove(statement)  # type: ignore[no-untyped-call]
                        statement += (URIRef(doc["_sub_graph_uri"]),)  # type: ignore

                    rdf_graph.add(statement)

                    # TODO: When RDF-star support is introduced into `rdflib`:
                    # for k, v in doc.items():
                    #     if k not in adb_key_blacklist:
                    #         ...

        return self.__rdf_graph

    def arangodb_collections_to_rdf(
        self,
        name: str,
        rdf_graph: RDFGraph,
        v_cols: Set[str],
        e_cols: Set[str],
        list_conversion_mode: str = "collection",
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
            processed as individual statements. Defaults to "collection".
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
            name, rdf_graph, metagraph, list_conversion_mode, **export_options
        )

    def arangodb_graph_to_rdf(
        self,
        name: str,
        rdf_graph: RDFGraph,
        list_conversion_mode: str = "collection",
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
            processed as individual statements. Defaults to "collection".
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
            name, rdf_graph, v_cols, e_cols, list_conversion_mode, **export_options
        )

    def __adb_doc_property_to_rdf_val(
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
                    self.__adb_doc_property_to_rdf_val(node, RDF.first, v)

                    rest = RDF.nil if i == len(val) - 1 else BNode()
                    self.__rdf_graph.add((node, RDF.rest, rest))
                    node = rest

            elif self.__list_conversion == "container":
                bnode = BNode()
                self.__rdf_graph.add((s, p, bnode))

                for i, v in enumerate(val, 1):
                    _n = URIRef(f"{str(RDF)}_{i}")
                    self.__adb_doc_property_to_rdf_val(bnode, _n, v)

            elif self.__list_conversion == "static":
                for v in val:
                    self.__adb_doc_property_to_rdf_val(s, p, v)

            else:
                raise ValueError("Invalid **list_conversion_mode value")

        elif type(val) is dict:
            bnode = BNode()
            self.__rdf_graph.add((s, p, bnode))

            for k, v in val.items():
                p_str = self.__uri_map.get(k, f"{self.__base_namespace}/{k}")
                self.__adb_doc_property_to_rdf_val(bnode, URIRef(p_str), v)

        else:
            # TODO: Datatype? Lang?
            self.__rdf_graph.add((s, p, Literal(val)))

    def __setup_iterators(
        self, rdf_iter_text: str, rdf_iter_color: str, adb_iter_text: str
    ) -> None:
        self.__rdf_iterator = rdf_track(rdf_iter_text, rdf_iter_color)
        self.__adb_iterator = adb_track(adb_iter_text)

    def __fetch_adb_docs(self, col: str) -> Result[Cursor]:
        """Fetches ArangoDB documents within a collection.

        :param col: The ArangoDB collection.
        :type col: str
        :return: Result cursor.
        :rtype: arango.cursor.Cursor
        """
        action = f"ArangoDB Export: {col}"
        adb_task = self.__adb_iterator.add_task("", action=action)

        aql = f"FOR doc IN {col} RETURN doc"
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

        for col, data in self.adb_docs.items():
            if not data or col in adb_col_blacklist:
                continue

            docs = list(data.values())

            action = f"ArangoDB Import: {col} ({len(data)})"
            adb_task = self.__adb_iterator.add_task("", action=action)

            if not self.db.has_collection(col):
                is_edge = {"_from", "_to"} <= docs[0].keys()  # HACK?
                self.db.create_collection(col, edge=is_edge)

            self.db.collection(col).import_bulk(docs, **self.__import_options)

            self.__adb_iterator.stop_task(adb_task)
            self.__adb_iterator.update(adb_task, visible=True)

        # For memory purposes, remove any adb_docs data that
        # has just been imported into ArangoDB
        for key in list(self.adb_docs):
            if key not in adb_col_blacklist:
                del self.adb_docs[key]

        # gc.collect() TODO: Check if worth introducing
