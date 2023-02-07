#!/usr/bin/env python3
import logging
import re
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
from rdflib import Graph as RDFGraph
from rdflib import Literal, URIRef
from rdflib.namespace import RDF, RDFS
from rdflib.term import Node

from .abc import Abstract_ArangoRDF
from .typings import (
    ADBDocs,
    ADBMetagraph,
    Json,
    RDFObject,
    RDFSubject,
    RDFTermMetadataPGT,
    RDFTermMetadataRPT,
)
from .utils import logger, progress, track


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

        if not issubclass(type(db), Database):
            msg = "**db** parameter must inherit from arango.database.Database"
            raise TypeError(msg)

        self.__db = db

        # A dictionary mapping all of the to-be-inserted ArangoDB
        # documents to their ArangoDB collection.
        self.adb_docs: ADBDocs = defaultdict(lambda: defaultdict(dict))

        logger.info(f"Instantiated ArangoRDF with database '{db.name}'")

    @property
    def db(self) -> Database:
        return self.__db  # pragma: no cover

    def set_logging(self, level: Union[int, str]) -> None:
        logger.setLevel(level)

    def rdf_to_arangodb_by_rpt(
        self,
        name: str,
        rdf_graph: RDFGraph,
        overwrite_graph: bool = False,
        load_base_ontology: bool = False,
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
        :param import_options: Keyword arguments to specify additional
            parameters for ArangoDB document insertion. Full parameter list:
            https://docs.python-arango.com/en/main/specs.html#arango.collection.Collection.import_bulk
        :type import_options: Any

        :return: The ArangoDB Graph API wrapper.
        :rtype: arango.graph.Graph
        """

        self.__URIREF_COL = f"{name}_URIRef"
        self.__BNODE_COL = f"{name}_BNode"
        self.__LITERAL_COL = f"{name}_Literal"
        self.__STATEMENT_COL = f"{name}_Statement"

        self.adb_docs.clear()

        if overwrite_graph:
            logger.debug("Overwrite graph flag is True. Deleting old graph.")
            self.__db.delete_graph(name, ignore_missing=True, drop_collections=True)

        if load_base_ontology:
            rdf_graph.parse(f"{Path(__file__).parent}/ontologies/rdfowl.ttl")

        s: RDFSubject  # Subject
        p: URIRef  # Predicate
        o: RDFObject  # Object
        sg: URIRef  # Sub Graph

        for s, p, o, *sg in track(
            rdf_graph, len(rdf_graph), "RDF → ADB (RPT)", "#BD0F89"
        ):

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
                "_sub_graph_uri": str(sg[0]) if sg else None,
            }

        adb_graph = self.__create_rpt_adb_graph(name)
        self.__insert_adb_docs(import_options)

        self.adb_docs.clear()

        return adb_graph

    def __process_rpt_term(self, t: Union[RDFSubject, RDFObject]) -> Tuple[str, str]:
        """Process an RDF Term as an ArangoDB document by RPT. Returns the
        ArangoDB Collection & Document Key associated to the RDF term.

        :param t: The RDF Term to process
        :type t: URIRef | BNode | Literal
        :return: The ArangoDB Collection name & Document Key of the RDF Term
        :rtype: Tuple[str, str]
        """

        t_col = ""
        t_str, t_type, t_key = self.__get_rdf_rpt_metadata(t)

        if t_type is URIRef:
            t_col = self.__URIREF_COL
            self.adb_docs[t_col][t_key] = {
                "_key": t_key,
                "_uri": t_str,
                "_rdftype": "URIRef",
            }

        elif t_type is BNode:
            t_col = self.__BNODE_COL
            self.adb_docs[t_col][t_key] = {"_key": t_str, "_rdftype": "BNode"}

        elif t_type is Literal:
            t_col = self.__LITERAL_COL

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
            raise ValueError()

        return t_col, t_key

    def __get_rdf_rpt_metadata(
        self, term: Union[RDFSubject, RDFObject]
    ) -> RDFTermMetadataRPT:
        """Return RPT-relevant metadata associated to the RDF Term.

        :param term: The RDF Term
        :type term: URIRef | BNode | Literal
        :return: The string representation, datatype, and
        ArangoDB Document Key of the RDF Term.
        :rtype: Tuple[str, type, str]
        """
        term_str = str(term)
        term_type = type(term)
        term_key = self._rdf_id_to_adb_key(term_str)

        return term_str, term_type, term_key

    def __create_rpt_adb_graph(self, name: str) -> ADBGraph:
        """Create an ArangoDB graph based on an RPT Transformation.

        :param name: The ArangoDB Graph name
        :type name: str

        :return: The ArangoDB Graph API wrapper.
        :rtype: arango.graph.Graph
        """

        if self.__db.has_graph(name):
            logger.debug(f"Graph {name} already exists")
            return self.__db.graph(name)

        else:
            logger.debug(f"Creating graph {name}")
            return self.__db.create_graph(
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
        :param import_options: Keyword arguments to specify additional
            parameters for ArangoDB document insertion. Full parameter list:
            https://docs.python-arango.com/en/main/specs.html#arango.collection.Collection.import_bulk
        :type import_options: Any

        :return: The ArangoDB Graph API wrapper.
        :rtype: arango.graph.Graph
        """

        # Reset config
        self.adb_docs.clear()
        self.__process_literal_as_string = False

        self.__rdf_graph = rdf_graph
        # Maps URIs to RDF Classes
        self.__class_map: Dict[str, str] = {}
        # Stores unidentified resources
        self.__UNIDENTIFIED_NODE_COL = f"{name}_UnidentifiedNode"
        # Stores edge definitions
        self.__e_col_map: DefaultDict[str, DefaultDict[str, Set[str]]]
        self.__e_col_map = defaultdict(lambda: defaultdict(set))

        if overwrite_graph:
            logger.debug("Overwrite graph flag is True. Deleting old graph.")
            self.__db.delete_graph(name, ignore_missing=True, drop_collections=True)

        if load_base_ontology:
            # TODO: Re-enable when tests are adjusted...
            # rdf_graph.parse(f"{Path(__file__).parent}/ontologies/rdfowl.ttl")
            self.__load_pgt_ontology()
            self.__e_col_map["type"]["from"].add("Property")
            self.__e_col_map["type"]["to"].add("Class")

        self.___build_pgt_class_map(load_base_ontology)

        s: RDFSubject  # Subject
        p: URIRef  # Predicate
        o: RDFObject  # Object
        sg: URIRef  # Sub Graph

        for s, p, o, *sub_graph in track(
            rdf_graph,
            len(rdf_graph),
            "RDF → ADB (PGT)",
            "#BD0F89",
        ):

            # TODO: Discuss repurcussions
            if not load_base_ontology and p == RDF.type:
                continue

            # TODO: Discuss repurcussions
            if o == RDF.nil:  # HACK ?
                continue

            # TODO: Discuss repurcussions
            if o == RDF.Seq:  # HACK?
                continue

            s_str = str(s)
            p_str = str(p)
            o_str = str(o)

            p_metadata = self.__get_rdf_pgt_metadata(p_str, is_predicate=True)
            _, p_key, _ = p_metadata

            if self.__statement_is_part_of_rdf_collection(s, p):
                self.adb_docs["_COLLECTION_BNODE"][s_str][p_key] = o

            elif self.__statement_is_part_of_rdf_container(s, p_str):
                self.__append_adb_docs_entry("_CONTAINER_BNODE", s_str, p_str, o)

            else:
                s_metadata = self.__get_rdf_pgt_metadata(s_str)
                o_metadata = self.__get_rdf_pgt_metadata(o_str)

                sg = str(sub_graph[0]) if sub_graph else None

                self.__process_pgt_subject(s, s_metadata)
                self.__process_pgt_object(s_metadata, p_metadata, o, o_metadata, sg)
                self.__process_pgt_edge(s_metadata, p_metadata, o, o_metadata, sg)

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
                        "_sub_graph_uri": None,
                    }

        self.__process_rdf_lists()

        adb_graph = self.__create_pgt_adb_graph(name)
        self.__insert_adb_docs(import_options)

        if self.adb_docs.get(self.__UNIDENTIFIED_NODE_COL):
            docs = self.adb_docs[self.__UNIDENTIFIED_NODE_COL]

            logger.info(
                f"""\n
                ----------------
                UnidentifiedNodes found ({len(docs)}).
                No `rdf:type` statement found for the following URIs/BNodes:
                {[val for val in docs.values()]}
                ----------------
                """
            )

        self.adb_docs.clear()

        return adb_graph

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
        self.__class_map[class_str] = "Class"
        self.adb_docs["Class"]["Class"] = {
            "_key": "Class",
            "_uri": class_str,
            "_rdftype": "URIRef",
        }

        property_str = str(RDF.Property)
        self.__class_map[property_str] = "Class"
        self.adb_docs["Class"]["Property"] = {
            "_key": "Property",
            "_uri": property_str,
            "_rdftype": "URIRef",
        }

        type_str = str(RDF.type)
        self.__class_map[type_str] = "Property"
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

    def ___build_pgt_class_map(self, load_base_ontology: bool) -> None:
        """A pre-processing step that iterates through the RDF Graph
        statements to build a URI-to-ArangoDB-Collection mapping of all
        RDF Nodes within the graph.

        This step is required for the PGT Processing stage in order to
        ensure each RDF Node is properly identified and categorized under
        a specific ArangoDB collection.

        :param load_base_ontology: TODO-define properly
        :type load_base_ontology: bool
        """
        for s, p, o, *_ in track(
            self.__rdf_graph,
            len(self.__rdf_graph),
            "RDF → ADB (PGT Pre-Process)",
            "#08479E",
        ):
            # TODO: Discuss Repurcssions
            if o in [RDF.Alt, RDF.Bag, RDF.List, RDF.Seq]:  # HACK ?
                continue

            s_str = str(s)
            p_str = str(p)
            o_str = str(o)

            if load_base_ontology:
                self.__class_map[p_str] = "Property"  # Case 7 issue

            if p == RDF.type:

                o_key = self._rdf_id_to_adb_key(o_str)
                self.__class_map[s_str] = o_key

                if load_base_ontology:
                    self.__class_map[o_str] = "Class"  # Case 7 Issue

                    e_key = f"{o_key}-type-Class"
                    self.adb_docs["type"][e_key] = {
                        "_key": e_key,
                        "_from": f"Class/{o_key}",
                        "_to": "Class/Class",
                        "_uri": str(RDF.type),
                        "_label": "type",
                        "_sub_graph_uri": None,
                    }

            elif p == RDFS.subClassOf:
                self.__class_map[s_str] = self.__class_map[o_str] = "Class"

            elif p == RDFS.subPropertyOf:
                self.__class_map[s_str] = self.__class_map[o_str] = "Property"

            else:
                continue

    def __get_rdf_pgt_metadata(
        self, term_str: str, is_predicate: bool = False
    ) -> RDFTermMetadataPGT:
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

        term_col = self.__class_map.get(term_str, self.__UNIDENTIFIED_NODE_COL)
        return term_str, term_key, term_col

    def __process_pgt_term(
        self,
        t: Union[RDFSubject, RDFObject],
        t_metadata: RDFTermMetadataPGT,
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
        :type s_key: Optional[str]
        :param s_col: The ArangoDB document key of the Subject associated
            to the RDF Term **t**. Only required if the RDF Term is of type Literal.
        :type s_col: Optional[str]
        :param p_key: The ArangoDB document key of the Predicate associated
            to the RDF Term **t**. Only required if the RDF Term is of type Literal.
        :type p_key: Optional[str]
        """

        t_type = type(t)
        t_str, t_key, t_col = t_metadata

        if t_type is URIRef:

            self.adb_docs[t_col][t_key] = {
                **self.adb_docs[t_col][t_key],
                "_key": t_key,
                "_uri": t_str,
                "_rdftype": "URIRef",
            }

        elif t_type is BNode:

            self.adb_docs[t_col][t_key] = {
                **self.adb_docs[t_col][t_key],
                "_key": t_key,
                "_rdftype": "BNode",
            }

        elif t_type is Literal and all([s_col, s_key, p_key]):
            t_value = t_str if type(t.value) is date else t.value or t_str
            self.__append_adb_docs_entry(s_col, s_key, p_key, t_value)

        else:
            raise ValueError()

    def __append_adb_docs_entry(
        self, col: str, key: str, sub_key: str, val: Any
    ) -> None:
        """A helper function used to insert an RDF Literal's value
        as a document property to the associated document.

        If `self.__process_literal_as_string` is enabled, the
        RDF Literal value is appended to a string representation of the
        current value of the document property (instead of being appended to
        a list).

        :param col: The ArangoDB collection name of the document.
        :type col: str
        :param key: The ArangoDB document key of the document.
        :type key: str
        :param sub_key: The ArangoDB property key of the document
            that will be used to store the RDF Literal's value.
        :type sub_key: str
        :param val: The value stored within the RDF Literal
        :type val: Any
        """

        # This flag is set active in ArangoRDF.__process_rdf_lists()
        if self.__process_literal_as_string:  # HACK?
            val = f"'{val}'," if type(val) is str else f"{val},"
            self.adb_docs[col][key][sub_key] += val

        else:

            try:
                # Assume p_key is a valid key (#1) and points to a list (#2)
                self.adb_docs[col][key][sub_key].append(val)
            except KeyError:
                # Catch assumption #1
                self.adb_docs[col][key][sub_key] = val
            except AttributeError:
                # Catch assumption #2
                self.adb_docs[col][key][sub_key] = [
                    self.adb_docs[col][key][sub_key],
                    val,
                ]

    def __process_pgt_subject(
        self, s: RDFSubject, s_metadata: RDFTermMetadataPGT
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
        s_metadata: RDFTermMetadataPGT,
        p_metadata: RDFTermMetadataPGT,
        o: RDFObject,
        o_metadata: RDFTermMetadataPGT,
        sg: Optional[str],
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
        :type sg: Optional[str]
        """

        s_str, s_key, s_col = s_metadata
        p_str, p_key, _ = p_metadata

        if self.__object_is_head_of_rdf_list(o):
            self.adb_docs["_LIST_HEAD"][s_str][p_str] = {
                "root": o,
                "sub_graph": sg,
            }

        else:
            self.__process_pgt_term(o, o_metadata, s_key, s_col, p_key)

    def __process_pgt_edge(
        self,
        s_metadata: RDFTermMetadataPGT,
        p_metadata: RDFTermMetadataPGT,
        o: RDFObject,
        o_metadata: RDFTermMetadataPGT,
        sg: Optional[str],
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
        :type sg: Optional[str]
        """
        _, s_key, s_col = s_metadata
        p_str, p_key, p_col = p_metadata
        _, o_key, o_col = o_metadata

        if type(o) is not Literal and not self.__object_is_head_of_rdf_list(o):
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
        a = self.__object_is_head_of_rdf_collection(o)
        b = self.__object_is_head_of_rdf_container(o)
        return a or b

    def __object_is_head_of_rdf_collection(self, o: RDFObject) -> bool:
        """Return True if the RDF Object *o* is  the "root" node
        of some RDF Collection.

        :param o: The RDF Object.
        :type o: URIRef | BNode | Literal
        :return: Whether the object points to an RDF Collection or not.
        :rtype: bool
        """
        a = (o, RDF.first, None) in self.__rdf_graph
        b = (o, RDF.rest, None) in self.__rdf_graph

        return a or b

    def __object_is_head_of_rdf_container(self, o: RDFObject) -> bool:
        """Return True if the RDF Object *o* is the "root" node
        of some RDF Container.

        :param o: The RDF Object.
        :type o: URIRef | BNode | Literal
        :return: Whether the object points to an RDF Container or not.
        :rtype: bool
        """
        a = (o, URIRef(f"{str(RDF)}_1"), None) in self.__rdf_graph
        b = (o, URIRef(f"{str(RDF)}li"), None) in self.__rdf_graph

        return a or b

    def __statement_is_part_of_rdf_collection(self, s: RDFSubject, p: URIRef) -> bool:
        return type(s) is BNode and p in [RDF.first, RDF.rest]

    def __statement_is_part_of_rdf_container(self, s: RDFSubject, p_str: str) -> bool:
        a = re.match(r"^http://www.w3.org/1999/02/22-rdf-syntax-ns#_[0-9]{1,}$", p_str)
        b = re.match(r"^http://www.w3.org/1999/02/22-rdf-syntax-ns#li$", p_str)

        return type(s) is BNode and (a is not None or b is not None)

    def __process_rdf_lists(self) -> None:
        self.__process_literal_as_string = True

        for s_str, s_dict in self.adb_docs["_LIST_HEAD"].items():
            s_metadata = self.__get_rdf_pgt_metadata(s_str)
            _, s_key, s_col = s_metadata

            for p_str, p_dict in s_dict.items():
                p_metadata = self.__get_rdf_pgt_metadata(p_str, is_predicate=True)
                _, p_key, _ = p_metadata

                doc = self.adb_docs[s_col][s_key]

                root: RDFObject = p_dict["root"]
                sg: str = p_dict["sub_graph"]

                doc[p_key] = ""
                self.__process_rdf_list_object(doc, s_metadata, p_metadata, root, sg)
                doc[p_key] = doc[p_key].rstrip(",")

                if set(doc[p_key]) == {"[", "]"}:
                    del doc[p_key]
                else:
                    doc[p_key] = literal_eval(doc[p_key])

        self.adb_docs.pop("_LIST_HEAD", None)
        self.adb_docs.pop("_COLLECTION_BNODE", None)
        self.adb_docs.pop("_CONTAINER_BNODE", None)

    def __process_rdf_list_object(
        self,
        doc: Json,
        s_metadata: RDFTermMetadataPGT,
        p_metadata: RDFTermMetadataPGT,
        o: RDFObject,
        sg: Optional[str],
    ) -> None:
        o_str = str(o)

        if o_str in self.adb_docs["_COLLECTION_BNODE"]:

            _, p_key, _ = p_metadata
            doc[p_key] += "["

            next_bnode_dict = self.adb_docs["_COLLECTION_BNODE"][o_str]
            self.__unpack_rdf_collection(
                doc, s_metadata, p_metadata, next_bnode_dict, sg
            )

            doc[p_key] = doc[p_key].rstrip(",") + "],"

        elif o_str in self.adb_docs["_CONTAINER_BNODE"]:

            _, p_key, _ = p_metadata
            doc[p_key] += "["

            next_bnode_dict = self.adb_docs["_CONTAINER_BNODE"][o_str]
            self.__unpack_rdf_container(
                doc, s_metadata, p_metadata, next_bnode_dict, sg
            )

            doc[p_key] = doc[p_key].rstrip(",") + "],"

        else:
            o_metadata = self.__get_rdf_pgt_metadata(o_str)
            self.__process_pgt_object(s_metadata, p_metadata, o, o_metadata, sg)
            self.__process_pgt_edge(s_metadata, p_metadata, o, o_metadata, sg)

    def __unpack_rdf_collection(
        self,
        doc: Json,
        s_metadata: RDFTermMetadataPGT,
        p_metadata: RDFTermMetadataPGT,
        bnode_dict: Dict[str, Union[RDFObject, str]],
        sg: Optional[str],
    ) -> None:

        first = bnode_dict["first"]
        self.__process_rdf_list_object(doc, s_metadata, p_metadata, first, sg)

        # TODO: What if "rest" points to something else other than a BNode???
        if "rest" in bnode_dict:
            rest_str = str(bnode_dict["rest"])
            next_bnode_dict = self.adb_docs["_COLLECTION_BNODE"][rest_str]
            self.__unpack_rdf_collection(
                doc, s_metadata, p_metadata, next_bnode_dict, sg
            )

    def __unpack_rdf_container(
        self,
        doc: Json,
        s_metadata: RDFTermMetadataPGT,
        p_metadata: RDFTermMetadataPGT,
        bnode_dict: Dict[str, Union[RDFObject, str]],
        sg: Optional[str],
    ) -> None:
        for data in sorted(bnode_dict.items()):
            _, value = data

            # Container Membership Property: rdf:li
            if type(value) is list:
                for o in value:
                    self.__process_rdf_list_object(doc, s_metadata, p_metadata, o, sg)

            # Container Membership Property: rdf:_n
            else:
                self.__process_rdf_list_object(doc, s_metadata, p_metadata, value, sg)

    def __create_pgt_adb_graph(self, name: str) -> ADBGraph:
        """Create an ArangoDB graph based on a PGT Transformation.

        :param name: The ArangoDB Graph name
        :type name: str

        :return: The ArangoDB Graph API wrapper.
        :rtype: arango.graph.Graph
        """
        edge_definitions: List[Dict[str, Union[str, List[str]]]] = []
        for e_col, v_cols in self.__e_col_map.items():
            edge_definitions.append(
                {
                    "from_vertex_collections": list(v_cols["from"]),
                    "edge_collection": e_col,
                    "to_vertex_collections": list(v_cols["to"]),
                }
            )

        non_orphan_collections: Set[str] = set()
        for e_col, data in self.__e_col_map.items():
            non_orphan_collections = non_orphan_collections | data["from"]
            non_orphan_collections = non_orphan_collections | data["to"]

        orphan_collections = list(
            non_orphan_collections
            ^ {self.__UNIDENTIFIED_NODE_COL}
            ^ set(self.__class_map.values())
        )

        return self.__db.create_graph(name, edge_definitions, orphan_collections)

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
        # TODO: What if we have two namespaces with the same RDF Label?
        regex_match = re.split("/|#", rdf_id)[-1]
        return self._string_to_arangodb_key(regex_match)

    def _string_to_arangodb_key(self, string: str) -> str:
        """Given a string, derive a valid ArangoDB _key string.

        :param string: A (possibly) invalid _key string value.
        :type string: str
        :return: A valid ArangoDB _key value.
        :rtype: str
        """
        res: str = ""
        for s in string:
            if s.isalnum() or s in self.VALID_ADB_KEY_CHARS:
                res += s

        return res

    def arangodb_to_rdf(
        self,
        name: str,
        rdf_graph: RDFGraph,
        metagraph: ADBMetagraph,
        list_conversion_mode: str = "collection",
        **query_options: Any,
    ) -> RDFGraph:
        """ """

        self.__base_namespace = "http://www.arangodb.com"
        type_map: Dict[str, RDFObject] = {
            "URIRef": URIRef,
            "Literal": Literal,
            "BNode": BNode,
        }

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

        self.__property_map: Dict[str, str] = {}
        self.__uri_map: Dict[str, str] = {}
        self.__rdf_map: Dict[str, Node] = {}
        self.__rdf_graph = rdf_graph
        self.__list_conversion = list_conversion_mode

        doc: Json
        if "Class" in metagraph["vertexCollections"]:
            for doc in self.__db.collection("Class"):  # Name TBD ?
                self.__uri_map[doc["_key"]] = doc["_uri"]

        if "Property" in metagraph["vertexCollections"]:
            for doc in self.__db.collection("Property"):  # Name TBD ?
                self.__property_map[doc["_key"]] = doc["_uri"]

        rdf_term: Union[RDFSubject, RDFObject]
        for v_col, _ in metagraph["vertexCollections"].items():
            v_col_uri_str = f"{self.__base_namespace}#{v_col}"
            rdf_class = URIRef(self.__uri_map.get(v_col, v_col_uri_str))

            cursor = self.__fetch_adb_docs(v_col, query_options)
            for doc in track(cursor, cursor.count(), f"ADB → RDF ({v_col})", "#97C423"):

                rdf_type = doc.get("_rdftype", "URIRef")
                adb_data_key = key_map[rdf_type]

                id = doc.get(adb_data_key, f"{v_col_uri_str}_{doc['_key']}")

                RDFClass = type_map[rdf_type]
                rdf_term = RDFClass(id)

                # # Only works for RPT transformation
                # kwds = {}
                # if RDFIdentifier is Literal:
                #     # Only allowed to use one of them
                #     if doc.get("_datatype") != xsd_string:
                #         kwds["datatype"] = doc["_datatype"]

                #     elif doc.get("_lang") != "en":
                #         kwds["lang"] = doc["_lang"]

                self.__rdf_map[doc["_id"]] = rdf_term

                if type(rdf_term) is Literal:  # RPT Case
                    continue

                if v_col not in adb_v_col_blacklist:  # HACK?
                    rdf_graph.add((rdf_term, RDF.type, rdf_class))

                # TODO: Should we iterate through metagraph values instead
                # of just everything?
                for k, v in doc.items():
                    if k not in adb_key_blacklist:  # HACK?
                        p = self.__property_map.get(k, f"{self.__base_namespace}#{k}")
                        self.__adb_property_to_rdf(rdf_term, URIRef(p), v)

        for e_col, _ in metagraph["edgeCollections"].items():
            e_col_uri_str = f"{self.__base_namespace}#{e_col}"

            cursor = self.__fetch_adb_docs(e_col, query_options)
            for doc in track(cursor, cursor.count(), f"ADB → RDF ({e_col})", "#5E3108"):

                statement = (
                    self.__rdf_map[doc["_from"]],
                    URIRef(doc.get("_uri", e_col_uri_str)),
                    self.__rdf_map[doc["_to"]],
                )

                if doc.get("_sub_graph_uri"):
                    rdf_graph.remove(statement)  # HACK
                    rdf_graph.add(statement + (URIRef(doc["_sub_graph_uri"]),))
                else:
                    rdf_graph.add(statement)

                # TODO: When RDF-star support is introduced:
                # for key, val in doc.items():
                #     if key not in adb_key_blacklist:
                #         p_str = self.__property_map.get(key, f"{base}#{key}")
                #         self.__adb_property_to_rdf(statement, URIRef(p_str), val)

        return self.__rdf_graph

    def arangodb_collections_to_rdf(
        self,
        name: str,
        rdf_graph: RDFGraph,
        v_cols: Set[str],
        e_cols: Set[str],
        list_conversion_mode: str = "collection",
        **query_options: Any,
    ) -> RDFGraph:
        metagraph: ADBMetagraph = {
            "vertexCollections": {col: set() for col in v_cols},
            "edgeCollections": {col: set() for col in e_cols},
        }

        return self.arangodb_to_rdf(
            name, rdf_graph, metagraph, list_conversion_mode, **query_options
        )

    def arangodb_graph_to_rdf(
        self,
        name: str,
        rdf_graph: RDFGraph,
        list_conversion_mode: str = "collection",
        **query_options: Any,
    ) -> RDFGraph:
        graph = self.__db.graph(name)
        v_cols = graph.vertex_collections()
        e_cols = {col["edge_collection"] for col in graph.edge_definitions()}

        return self.arangodb_collections_to_rdf(
            name, rdf_graph, v_cols, e_cols, list_conversion_mode, **query_options
        )

    def __adb_property_to_rdf(
        self,
        rdf_term: RDFSubject,
        p: URIRef,
        val: Any,
    ) -> None:

        if type(val) is list:

            if self.__list_conversion == "collection":

                bnode = BNode()
                self.__rdf_graph.add((rdf_term, p, bnode))

                for i, v in enumerate(val):
                    self.__adb_property_to_rdf(bnode, RDF.first, v)

                    rest = BNode() if i != len(val) - 1 else RDF.nil
                    self.__rdf_graph.add((bnode, RDF.rest, rest))
                    bnode = rest

            elif self.__list_conversion == "container":

                bnode = BNode()
                self.__rdf_graph.add((rdf_term, p, bnode))

                for i, v in enumerate(val, 1):
                    _n = URIRef(f"{str(RDF)}_{i}")
                    self.__adb_property_to_rdf(bnode, _n, v)

            elif self.__list_conversion == "static":
                for v in val:
                    self.__adb_property_to_rdf(rdf_term, p, v)

            else:
                raise ValueError("Invalid **list_conversion_mode value")

        elif type(val) is dict:

            bnode = BNode()
            self.__rdf_graph.add((rdf_term, p, bnode))

            for k, v in val.items():
                p_str = self.__property_map.get(k, f"{self.__base_namespace}#{k}")
                self.__adb_property_to_rdf(bnode, URIRef(p_str), v)

        else:
            # TODO: Datatype? Lang?
            self.__rdf_graph.add((rdf_term, p, Literal(val)))

    def __fetch_adb_docs(
        self,
        col: str,
        query_options: Any,
    ) -> Result[Cursor]:
        """Fetches ArangoDB documents within a collection.

        :param col: The ArangoDB collection.
        :type col: str
        :param query_options: Keyword arguments to specify AQL query options when
            fetching documents from the ArangoDB instance.
        :type query_options: Any
        :return: Result cursor.
        :rtype: arango.cursor.Cursor
        """
        aql = """
            FOR doc IN @@col
                RETURN doc
        """

        with progress(f"ArangoDB Export: {col}") as p:
            p.add_task("__fetch_adb_docs")

            return self.__db.aql.execute(
                aql, count=True, bind_vars={"@col": col}, **query_options
            )

    def __insert_adb_docs(
        self,
        import_options: Any,
    ) -> None:
        """Insert ArangoDB documents into their ArangoDB collection.

        :param import_options: Keyword arguments to specify additional
            parameters for ArangoDB document insertion. Full parameter list:
            https://docs.python-arango.com/en/main/specs.html#arango.collection.Collection.import_bulk
        """
        import_options["on_duplicate"] = "update"
        for col, data in self.adb_docs.items():
            with progress(f"ArangoDB Import: {col} ({len(data)})") as p:
                p.add_task("__insert_adb_docs")

                documents = data.values() if type(data) is defaultdict else data
                r = self.__db.collection(col).import_bulk(documents, **import_options)
                logger.debug(r)
