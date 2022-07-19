#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# author @David Vidovich (Mission Solutions Group)
# author @Arthur Keen (ArangoDB)

import hashlib
import sys
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Union

# from rdflib.namespace import RDFS, OWL
from arango.database import StandardDatabase
from arango.graph import Graph as ArangoGraph
from rdflib import BNode
from rdflib import Graph as RDFGraph
from rdflib import Literal, URIRef
from tqdm import tqdm


class ArangoRDF:
    def __init__(
        self,
        db: StandardDatabase,
        default_graph: str = "default_graph",
        sub_graph: Optional[str] = None,
    ) -> None:
        """
        Parameters
        ----------
        db: StandardDatabase
            The python-arango database client
        default_graph: str
            The name of the ArangoDB graph that contains all collections
        sub_graph: str | None
            The identifier of the RDF graph that defines an ArangoDB sub-graph
            that only contains the nodes & edges of a specific graph
        """

        self.db: StandardDatabase = db
        self.default_graph = default_graph
        self.sub_graph = sub_graph

        # Create the graph
        if self.db.has_graph(default_graph):
            self.graph = self.db.graph(default_graph)
        else:
            self.graph = self.db.create_graph(default_graph)

        self.__set_sub_graph = sub_graph is not None
        self.rdf_graph = RDFGraph(identifier=sub_graph)

        # Maps the default RDF collection names to the user-specified RDF collection names
        self.col_map: Dict[str, str] = {}

    def init_rdf_collections(
        self,
        iri: str = "IRI",
        bnode: str = "BNode",
        literal: str = "Literal",
        edge: str = "Statement",
    ) -> None:
        """
        Creates the node and edge collections for rdf import.

        Parameters
        ----------
        iri: str
            the name of the collection that will store the IRI nodes (default is "IRI")
        bnode: str
            the name of the collection that will store blank nodes (default is "BNode")
        literal: str
            the name of collection that will store literals (default is "Literal")
        edge: str
            the name of the edge collection that will connect the nodes (default is "Statement")
        """
        # init collections
        self.init_collection(iri, "iri")
        self.init_collection(bnode, "bnode")
        self.init_collection(literal, "literal")
        self.init_edge_collection(
            edge, [iri, bnode], [iri, literal, bnode], "statement"
        )

    def init_collection(self, name: str, default_name: str) -> None:
        """
        Creates collection if it doesn't already exist

        parameters
        ----------
        name: str
            the name of the collection that will be created
        default_name: str
            the name that will be used to reference the collection in the code
        """
        if self.db.has_collection(name) is False:
            self.db.create_collection(name)

        self.col_map[default_name] = name

    def init_edge_collection(
        self,
        name: str,
        parent_collections: List[str],
        child_collections: List[str],
        default_name: str,
    ) -> None:
        """
        Creates edge collection if it doesn't already exist. Appends to and from vertex collections if collection already exists.

        Parameters
        ----------
        name: str
            the name of the edge collection that will be created
        parent_collections: List[str]
            a list of collections that will be added to from_vertex_collections in the edge definition
        child_collections: List[str]
            a list of collections that will be added to to_vertex_collections in the edge definition
        default_name: str
            the name that will be used to reference the collection in the code
        """

        if self.graph.has_edge_collection(name):
            # check edge definition
            edge_defs = self.graph.edge_definitions()
            current_def = None
            for ed in edge_defs:
                if ed["edge_collection"] == name:
                    current_def = ed
                    break

            # check if existing definition includes the intedned collections
            new_from_vc = current_def["from_vertex_collections"]
            for col in parent_collections:
                if col not in current_def["from_vertex_collections"]:
                    new_from_vc.append(col)

            new_to_vc = current_def["to_vertex_collections"]
            for col in child_collections:
                if col not in current_def["to_vertex_collections"]:
                    new_to_vc.append(col)

            # replace def
            self.graph.replace_edge_definition(
                edge_collection=name,
                from_vertex_collections=new_from_vc,
                to_vertex_collections=new_to_vc,
            )

        else:
            self.graph.create_edge_definition(
                edge_collection=name,
                from_vertex_collections=parent_collections,
                to_vertex_collections=child_collections,
            )

        self.col_map[default_name] = name

    def import_rdf(
        self,
        data: str,
        format: str = "xml",
        config: dict = {},
        save_config: bool = False,
        **import_options: Any,
    ) -> ArangoGraph:
        """
        Imports an rdf graph from a file into Arangodb

        Parameters
        ----------
        data: str
            path to rdf file
        format: str
            format of the rdf file (default is "xml")
        config: dict
            configuration options, which currently include:
                normalize_literals: bool
                    normalize the RDF literals. Defaults to False
        save_config: bool
            save the specified configuration into the ArangoDB 'configurations' collection
        """

        self.rdf_graph.parse(data, format=format)

        graph_id = self.rdf_graph.identifier.toPython()

        normalize_literals = config.get("normalize_literals", False)
        if config and save_config:
            config["normalize_literals"] = normalize_literals
            config["default_graph"] = self.default_graph
            if self.__set_sub_graph:
                config["sub_graph"] = self.sub_graph

            self.save_config(config)

        adb_documents = defaultdict(list)

        file_name = data.split("/")[-1]
        for s, p, o in tqdm(self.rdf_graph, desc=file_name, colour="#88a049"):

            # build subject doc
            if isinstance(s, URIRef):
                s_collection = "iri"
                s_doc = self.build_iri_doc(s)
            elif isinstance(s, BNode):
                s_collection = "bnode"
                s_doc = self.build_bnode_doc(s)
            else:
                raise ValueError("Subject must be IRI or Blank Node")

            s_id = self.col_map[s_collection] + "/" + s_doc["_key"]
            adb_documents[s_collection].append(s_doc)

            # build object doc
            if isinstance(o, URIRef):
                o_collection = "iri"
                o_doc = self.build_iri_doc(o)
            elif isinstance(o, BNode):
                o_collection = "bnode"
                o_doc = self.build_bnode_doc(o)
            elif isinstance(o, Literal):
                o_collection = "literal"
                o_doc = self.build_literal_doc(o, normalize_literals)
            else:
                raise ValueError("Object must be IRI, Blank Node, or Literal")

            o_id = self.col_map[o_collection] + "/" + o_doc["_key"]
            adb_documents[o_collection].append(o_doc)

            # build and insert edge
            edge = self.build_statement_edge(p, s_id, o_id, graph_id)

            # add RDF Graph id as edge property
            if self.__set_sub_graph:
                edge["_graph"] = graph_id

            adb_documents["statement"].append(edge)

        # Set default ArangoDB `import_bulk` behavior to update/insert
        if "on_duplicate" not in import_options:
            import_options["on_duplicate"] = "update"

        for collection, doc_list in tqdm(
            adb_documents.items(), colour="#5e3108", desc="/_api/import"
        ):
            self.db.collection(self.col_map[collection]).import_bulk(
                doc_list, **import_options
            )

        return self.graph

    def export_rdf(
        self,
        file_name: Optional[str] = None,
        format: Optional[str] = None,
        **query_options: Any,
    ) -> RDFGraph:
        """
        Builds a rdf graph from the database graph and exports to a file

        Parameters
        ----------
        file_name: str | none
            path to where file will be exported
        format: str | none
            format of the rdf file
        """
        # init rdf graph
        g = RDFGraph()

        aql = """
            FOR edge IN @@col
                RETURN {
                    iri: edge["_iri"],
                    from: DOCUMENT(edge._from),
                    to: DOCUMENT(edge._to)
                }
        """

        data_cursor = self.db.aql.execute(
            aql,
            bind_vars={"@col": self.col_map["statement"]},
            count=True,
            **query_options,
        )

        for data in tqdm(data_cursor, total=data_cursor.count(), colour="CYAN"):
            from_node = self.adb_doc_to_rdf_node(data["from"])
            to_node = self.adb_doc_to_rdf_node(data["to"])
            _iri = data["iri"]

            # add triple to graph
            g.add((from_node, URIRef(_iri), to_node))

        # output graph
        if file_name:
            g.serialize(destination=file_name, format=format)

        return g

    def build_iri_doc(self, iri: URIRef) -> dict:
        return {
            "_key": hashlib.md5(str(iri).encode("utf-8")).hexdigest(),
            "_iri": iri.toPython(),
        }

    def build_bnode_doc(self, bnode: BNode) -> dict:
        return {"_key": bnode.toPython()}

    def build_literal_doc(self, literal: Literal, normalize: bool) -> dict:

        lang = str(literal.language)
        type = str(literal.datatype)
        value = str(literal.value)

        if type == "None":
            type = "http://www.w3.org/2001/XMLSchema#string"

        # rdf strings are the only type allowed to not have a type.  Coerce strings without type to xsd:String
        doc = {"_value": value, "_type": type, "_lang": lang}
        if normalize:
            key_string = value + type + lang
        else:
            key_string = str(time.time())

        doc["_key"] = hashlib.md5(key_string.encode("utf-8")).hexdigest()

        return doc

    def build_statement_edge(
        self, predicate: URIRef, subject_id: str, object_id: str, graph: str
    ) -> dict:
        _iri = predicate.toPython()
        _from = subject_id
        _predicate = hashlib.md5(_iri.encode("utf-8")).hexdigest()
        _to = object_id
        key_string = str(_from + _predicate + _to + graph)
        _key = hashlib.md5(key_string.encode("utf-8")).hexdigest()

        doc = {
            "_key": _key,
            "_iri": _iri,
            "_from": _from,
            "_predicate": _predicate,
            "_to": _to,
        }

        return doc

    def adb_doc_to_rdf_node(self, adb_doc: dict) -> Union[URIRef, BNode, Literal]:
        # build literal
        if "_type" in adb_doc:
            if adb_doc["_lang"] is not None:
                return Literal(adb_doc["_value"], lang=adb_doc["_lang"])
            else:
                return Literal(adb_doc["_value"], datatype=adb_doc["_type"])

        # build URIRef
        if "_iri" in adb_doc:
            return URIRef(adb_doc["_iri"])

        # build BNode
        return BNode(value=adb_doc["_key"])

    def save_config(self, config: dict) -> None:
        if self.db.has_collection("configurations") is False:
            self.db.create_collection("configurations")
        else:
            aql = """
                FOR c IN configurations
                    FILTER c.latest == true
                    UPDATE c WITH { latest: false } INTO configurations
            """

            self.db.aql.execute(aql)

        config["latest"] = True
        config["timestamp"] = time.time()
        self.db.collection("configurations").insert(config)

    def get_config_by_latest(self) -> dict:
        return self.get_config_by_key_value("latest", True)

    def get_config_by_key_value(self, key: str, val: Any) -> dict:
        aql = """
            FOR c IN configurations
                FILTER c[@key] == @val
                SORT c.timestamp DESC
                LIMIT 1
                RETURN UNSET(c, "_id", "_key", "_rev")
        """
        cursor = self.db.aql.execute(aql, bind_vars={"key": key, "val": val})
        if cursor.empty():
            sys.exit("No configuration found")

        return cursor.pop()
