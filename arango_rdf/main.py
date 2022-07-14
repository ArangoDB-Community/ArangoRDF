#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# author @David Vidovich (Mission Solutions Group)
# author @Arthur Keen (ArangoDB)

import hashlib
import string
import sys
import time
from typing import Any, List, Optional, Union

# from rdflib.namespace import RDFS, OWL
from arango import ArangoClient
from arango.collection import EdgeCollection, StandardCollection
from arango.cursor import Cursor
from arango.database import StandardDatabase
from rdflib import BNode, Graph, Literal, URIRef


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
        self.rdf_graph = Graph(identifier=sub_graph)

        self.collections = {}

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

    def init_ontology_collections(
        self,
        cls: str = "Class",
        rel: str = "Relationship",
        prop: str = "Property",
        sub_cls: str = "SubClassOf",
        sub_prop: str = "SubPropertyOf",
        range: str = "Range",
        domain: str = "Domain",
    ):
        """
        Creates the node and edge collections for ontology import.

        Parameters
        ----------
        cls: str
            the name of the collection that will store classes (default is "Class")
        rel: str
            the name of the collection that will store relationships (default is "Relationship")
        prop: str
            the name of the collection that will store properties (default is "Property")
        sub_cls: str
            the name of the edge collection that will connect classes (default is "SubClassOf")
        sub_prop: str
            the name of the edge collection that will connect properties (default is "SubPropertyOf")
        range: str
            the name of the edge collection that will connect relationships to range classes (default is "Range")
        domain: str
            the name of the edge collection that will connect relationships to domain classes (default is "Domain")
        """
        self.init_collection(cls, "class")
        self.init_collection(rel, "rel")
        self.init_collection(prop, "prop")

        self.init_edge_collection(sub_cls, [cls], [cls], "sub_class")
        self.init_edge_collection(sub_prop, [prop], [prop], "sub_prop")
        self.init_edge_collection(range, [rel], [cls], "range")
        self.init_edge_collection(domain, [rel], [cls], "domain")

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
        if self.db.has_collection(name):
            collection = self.db.collection(name)
        else:
            collection = self.db.create_collection(name)
        self.collections[default_name] = collection

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

            collection = self.graph.edge_collection(name)
        else:
            collection = self.graph.create_edge_definition(
                edge_collection=name,
                from_vertex_collections=parent_collections,
                to_vertex_collections=child_collections,
            )

        self.collections[default_name] = collection

    def import_rdf(
        self,
        data: str,
        format: str = "xml",
        config: dict = {},
        save_config: bool = False,
    ) -> None:
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

        if config and save_config:
            config["normalize_literals"] = config.get("normalize_literals", False)
            config["default_graph"] = self.default_graph
            if self.__set_sub_graph:
                config["sub_graph"] = self.sub_graph

            self.save_config(config)

        for s, p, o in self.rdf_graph:

            # build subject doc
            if isinstance(s, URIRef):
                collection = "iri"
                doc = self.build_iri_doc(s)
            elif isinstance(s, BNode):
                collection = "bnode"
                doc = self.build_bnode_doc(s)
            else:
                raise ValueError("Subject must be IRI or Blank Node")

            s_id = self.insert_doc(self.collections[collection], doc)

            # build object doc
            if isinstance(o, URIRef):
                collection = "iri"
                doc = self.build_iri_doc(o)
                o_id = self.insert_doc(self.collections[collection], doc)
            elif isinstance(o, BNode):
                collection = "bnode"
                doc = self.build_bnode_doc(o)
                o_id = self.insert_doc(self.collections[collection], doc)
            elif isinstance(o, Literal):
                collection = "literal"
                normalize = config.get("normalize_literals", False)
                doc = self.build_literal_doc(o, normalize)
                o_id = self.insert_literal_doc(
                    self.collections[collection], doc, normalize
                )
            else:
                raise ValueError("Object must be IRI, Blank Node, or Literal")

            # build and insert edge
            edge = self.build_statement_edge(p, s_id, o_id, graph_id)

            # add RDF Graph id as edge property
            if self.__set_sub_graph:
                edge["graph"] = graph_id

            self.insert_edge(self.collections["statement"], edge)

        return

    def import_ontology(self, data: str, format: str = "xml") -> None:
        """
        Imports an ontology from a file into Arangodb

        Parameters
        ----------
        data: str
            path to rdf file
        format: str
            format of the rdf file (default is "xml")
        """

        self.rdf_graph.parse(data, format=format)
        graph_id = self.rdf_graph.identifier.toPython()

        for s, p, o in self.rdf_graph:
            if isinstance(o, Literal) is False:
                if "#Class" in o.toPython():
                    self.build_and_insert_node(self.collections["class"], s)
                elif "#ObjectProperty" in o.toPython():
                    self.build_and_insert_node(self.collections["rel"], s)
                elif "#DatatypeProperty" in o.toPython():
                    self.build_and_insert_node(self.collections["prop"], s)
                else:
                    raise ValueError(f"Unrecognized object {o.toPython()}")

            else:
                # if predicate is subclass of, add s and o to class collection and connect them w/ subClassOf edge
                if "#subClassOf" in p.toPython():
                    o_id = self.build_and_insert_node(self.collections["class"], o)
                    s_id = self.build_and_insert_node(self.collections["class"], s)

                    edge = self.build_statement_edge(p, s_id, o_id, graph_id)
                    self.insert_edge(self.collections["sub_class"], edge)

                # if predicate is #domain create relationship node and connect to class node
                elif "#domain" in p.toPython():
                    s_id = self.build_and_insert_node(self.collections["rel"], s)
                    o_id = self.build_and_insert_node(self.collections["class"], o)

                    edge = self.build_statement_edge(p, s_id, o_id, graph_id)
                    self.insert_edge(self.collections["domain"], edge)

                elif "#range" in p.toPython():
                    s_id = self.build_and_insert_node(self.collections["rel"], s)
                    o_id = self.build_and_insert_node(self.collections["class"], o)

                    edge = self.build_statement_edge(p, s_id, o_id, graph_id)
                    self.insert_edge(self.collections["range"], edge)

                elif "#subPropertyOf" in p.toPython():
                    o_id = self.build_and_insert_node(self.collections["prop"], o)
                    s_id = self.build_and_insert_node(self.collections["prop"], o)

                    edge = self.build_statement_edge(p, s_id, o_id, graph_id)
                    self.insert_edge(self.collections["sub_prop"], edge)
                else:
                    raise ValueError(f"Unrecognized predicate {p.toPython()}")

    def export(self, file_name: str, format: str) -> None:
        """
        Builds a rdf graph from the database graph and exports to a file

        Parameters
        ----------
        file_name: str
            path to where file will be exported
        format: str
            format of the rdf file
        """
        # init rdf graph
        g = Graph()

        # get all collections from db
        collections = self.db.collections()
        names = []
        for i in collections:
            if i["name"][0] != "_":
                names.append(i["name"])

        all_adb_docs = self.get_all_docs(names)

        for n in all_adb_docs:
            if "_to" in n:
                # find and build subect/object
                to_node = self.find_by_id(n["_to"], all_adb_docs)
                from_node = self.find_by_id(n["_from"], all_adb_docs)

                _iri = n["_iri"]
                # add triple to graph
                g.add((from_node, URIRef(_iri), to_node))

        # output graph
        g.serialize(destination=file_name, format=format)

        return

    def build_and_insert_node(
        self, collection: StandardCollection, doc: Union[URIRef, BNode]
    ) -> dict:
        if isinstance(doc, URIRef):
            node = self.build_iri_doc(doc)
        elif isinstance(doc, BNode):
            node = self.build_bnode_doc(doc)
        else:
            raise ValueError("Document must be IRI or Blank Node")

        return self.insert_doc(collection, node)

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
            key = hashlib.md5(key_string.encode("utf-8")).hexdigest()
            doc["_key"] = key

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

    def insert_doc(self, collection: StandardCollection, doc: dict) -> dict:
        return collection.insert(doc, overwrite_mode="update")["_id"]

    def insert_literal_doc(
        self, collection: StandardCollection, doc: dict, normalize=False
    ) -> dict:
        if normalize:  # TODO: Clean this up
            try:
                return collection.insert(doc)["_id"]
            except:
                main_doc = collection.get(doc["_key"])
                if "duplicates" in main_doc:
                    main_doc["duplicates"].append(doc)
                else:
                    main_doc["duplicates"] = [doc]

                return collection.update(main_doc)["_id"]
        else:
            return collection.insert(doc)["_id"]

    def insert_edge(self, collection: EdgeCollection, edge: dict) -> dict:
        # TODO: Verify intended behavior and consider replacing this spaghet AQL with collection.insert(edge)
        col_name = collection.name
        aql = f"""
            UPSERT {{ _from: '{edge['_from']}', _to: '{edge['_to']}' }}
            INSERT  {edge}
            UPDATE {{}}
            IN {col_name}
            LET doc = IS_NULL(OLD) ? NEW : OLD
            RETURN {{ _id: doc._id}}
        """
        cursor = self.db.aql.execute(aql)

        return cursor.pop()

    def get_all_docs(self, cols: List[str]) -> Cursor:
        docs = []
        for col in cols:
            docs.extend([doc for doc in self.db.collection(col).all()])

        return docs

    def find_by_id(self, id: string, nodes: list) -> Union[URIRef, BNode, Literal]:
        node = None
        for n in nodes:
            if n["_id"] == id:
                node = n
        # build literal
        if "_type" in node:
            if node["_lang"] is not None:
                return Literal(node["_value"], lang=node["_lang"])
            else:
                return Literal(node("_value"), datatype=node["_type"])
        # build URIRef
        if "_iri" in node:
            return URIRef(node["_iri"])
        # build BNode
        return BNode(value=node["_key"])

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
        config["timestamp"] = round(time.time())
        self.db.collection("configurations").insert(config)

    def get_config_by_latest(self) -> dict:
        return self.get_config_by_key_value("latest", True)

    def get_config_by_key_value(self, key: str, val: Any) -> dict:
        aql = f"""
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
