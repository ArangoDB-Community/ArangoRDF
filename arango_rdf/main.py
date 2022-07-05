#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# author @David Vidovich (Mission Solutions Group)
# author @Arthur Keen (ArangoDB)

import hashlib
import string
from typing import List, Union

# from rdflib.namespace import RDFS, OWL
from arango import ArangoClient
from arango.collection import EdgeCollection, StandardCollection
from arango.cursor import Cursor
from rdflib import BNode, Graph, Literal, URIRef


class ArangoRDF:
    def __init__(
        self, host: str, username: str, password: str, database: str, graph: str
    ) -> None:
        """
        Parameters
        ----------
        host: str
            Host url
        username: str
            Username for basic authentication
        password: str
            Password for basic authentication
        database: str
            Database name
        graph: str
            Graph name
        """

        self.connection = ArangoClient(hosts=host)
        sys_db = self.connection.db(
            "_system", username=username, password=password, verify=False
        )

        if sys_db.has_database(database):
            self.db = self.connection.db(
                database, username=username, password=password, verify=False
            )
        else:
            sys_db.create_database(database)
            self.db = self.connection.db(
                database, username=username, password=password, verify=False
            )

        # Create the graph
        if self.db.has_graph(graph):
            self.graph = self.db.graph(graph)
        else:
            self.graph = self.db.create_graph(graph)

        # init rdflib graph
        self.rdf_graph = Graph()

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

    def import_rdf(self, data: str, format: str = "xml") -> None:
        """
        Imports an rdf graph from a file into Arangodb

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

            # build subject doc
            if isinstance(s, URIRef):
                s_id = self.insert_doc(
                    self.collections["iri"],
                    self.build_iri_doc(s, self.collections["iri"]),
                )
            elif isinstance(s, BNode):
                s_id = self.insert_doc(
                    self.collections["bnode"],
                    self.build_bnode_doc(s, self.collections["bnode"]),
                )
            else:
                raise ValueError("Subject must be IRI or Blank Node")

            # build object doc
            if isinstance(o, URIRef):
                o_id = self.insert_doc(
                    self.collections["iri"],
                    self.build_iri_doc(o, self.collections["iri"]),
                )
            elif isinstance(o, BNode):
                o_id = self.insert_doc(
                    self.collections["bnode"],
                    self.build_bnode_doc(o, self.collections["bnode"]),
                )
            elif isinstance(o, Literal):
                o_id = self.insert_doc(
                    self.collections["literal"],
                    self.build_literal_doc(o, self.collections["literal"]),
                )
            else:
                raise ValueError("Object must be IRI, Blank Node, or Literal")

            # build and insert edge
            self.insert_edge(
                self.collections["statement"],
                self.build_statement_edge(
                    p, s_id, o_id, graph_id, self.collections["statement"]
                ),
            )

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

            # if object #Class then add subject to class collection
            if not isinstance(o, Literal):
                if "#Class" in o.toPython():
                    self.build_node(s, self.collections["class"])

                    continue

            # add objectProperty to relationship collection
            if not isinstance(o, Literal):
                if "#ObjectProperty" in o.toPython():
                    self.build_node(s, self.collections["rel"])
                    continue

            if not isinstance(o, Literal):
                if "#DatatypeProperty" in o.toPython():
                    self.build_node(s, self.collections["prop"])
                    continue

            # #if predicate is subclass of, add s and o to class collection and connect them w/ subClassOf edge
            if "#subClassOf" in p.toPython():
                o_id = self.build_node(o, self.collections["class"])
                s_id = self.build_node(s, self.collections["class"])
                self.insert_edge(
                    self.collections["sub_class"],
                    self.build_statement_edge(
                        p, s_id, o_id, graph_id, self.collections["sub_class"]
                    ),
                )
                continue

            # if predicate is #domain create relationship node and connect to class node
            if "#domain" in p.toPython():
                s_id = self.build_node(s, self.collections["rel"])
                o_id = self.build_node(o, self.collections["class"])
                self.insert_edge(
                    self.collections["domain"],
                    self.build_statement_edge(
                        p, s_id, o_id, graph_id, self.collections["domain"]
                    ),
                )
                continue

            if "#range" in p.toPython():
                s_id = self.build_node(s, self.collections["rel"])
                o_id = self.build_node(o, self.collections["class"])
                self.insert_edge(
                    self.collections["range"],
                    self.build_statement_edge(
                        p, s_id, o_id, graph_id, self.collections["range"]
                    ),
                )
                continue

            if "#subPropertyOf" in p.toPython():
                o_id = self.build_node(o, self.collections["prop"])
                s_id = self.build_node(o, self.collections["prop"])
                self.insert_edge(
                    self.collections["sub_prop"],
                    self.build_statement_edge(
                        p, s_id, o_id, graph_id, self.collections["sub_prop"]
                    ),
                )

        return

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

        # get nodes
        nodes = self.get_all(names)
        # all_nodes = []
        # # build triples
        # for collection in nodes:
        #     for n in collection:
        #         all_nodes.append(n)

        for n in nodes:
            if "_to" in n:
                # find and build subect/object
                to_node = self.find_by_id(n["_to"], nodes)
                from_node = self.find_by_id(n["_from"], nodes)

                _iri = n["_iri"]
                # add triple to graph
                g.add((from_node, URIRef(_iri), to_node))

        # output graph
        g.serialize(destination=file_name, format=format)

        return

    def build_node(
        self, doc: Union[URIRef, BNode], collection: StandardCollection
    ) -> dict:
        if isinstance(doc, URIRef):
            node = self.build_iri_doc(doc, collection)
        elif isinstance(doc, BNode):
            node = self.build_bnode_doc(doc, collection)
        else:
            raise ValueError("Document must be IRI or Blank Node")

        node_id = self.insert_doc(collection, node)
        return node_id

    def build_iri_doc(self, iri: URIRef, collection: StandardCollection) -> dict:
        key = hashlib.md5(str(iri).encode("utf-8")).hexdigest()
        id = f"{collection.name}/{key}"

        doc = {"_key": key, "_iri": iri.toPython(), "_id": id}

        return doc

    def build_bnode_doc(self, bnode: BNode, collection: StandardCollection) -> dict:
        key = bnode.toPython()
        id = f"{collection.name}/{key}"

        doc = {"_key": key, "_id": id}

        return doc

    def build_literal_doc(
        self, literal: Literal, collection: StandardCollection
    ) -> dict:

        lang = str(literal.language)
        type = str(literal.datatype)
        value = str(literal.value)
        key_string = value + type + lang
        key = hashlib.md5(key_string.encode("utf-8")).hexdigest()
        id = f"{collection.name}/{key}"
        # rdf strings are the only type allowed to not have a type.  Coerce strings without type to xsd:String
        if type == "None":
            type = "http://www.w3.org/2001/XMLSchema#string"

        doc = {"_id": id, "_key": key, "_value": value, "_type": type, "_lang": lang}
        return doc

    def build_statement_edge(
        self,
        predicate: URIRef,
        subject_id: dict,
        object_id: dict,
        graph: str,
        collection: StandardCollection,
    ) -> dict:
        _iri = predicate.toPython()
        _from = subject_id["_id"]
        _predicate = hashlib.md5(_iri.encode("utf-8")).hexdigest()
        _to = object_id["_id"]
        key_string = str(_from + _predicate + _to + graph)
        _key = hashlib.md5(key_string.encode("utf-8")).hexdigest()
        _id = f"{collection.name}/{_key}"

        doc = {
            "_id": _id,
            "_key": _key,
            "_iri": _iri,
            "_from": _from,
            "_predicate": _predicate,
            "_to": _to,
        }

        return doc

    def insert_doc(self, collection: StandardCollection, doc: dict) -> dict:
        col_name = collection.name

        cursor = self.db.aql.execute(
            f"UPSERT {{ _id: '{doc['_id']}' }}\n"
            f"INSERT  {doc} \n"
            f"UPDATE {{}}\n"
            f"IN {col_name}\n"
            f"LET doc = IS_NULL(OLD) ? NEW : OLD \n"
            f"RETURN {{ _id: doc._id}}"
        )

        return cursor.pop()

    def insert_edge(self, collection: EdgeCollection, edge: dict) -> dict:
        col_name = collection.name
        cursor = self.db.aql.execute(
            f"UPSERT {{ _from: '{edge['_from']}', _to: '{edge['_to']}' }}\n"
            f"INSERT  {edge} \n"
            f"UPDATE {{}}\n"
            f"IN {col_name}\n"
            f"LET doc = IS_NULL(OLD) ? NEW : OLD \n"
            f"RETURN {{ _id: doc._id}}"
        )

        return cursor.pop()

    def get_all(self, cols: List[str]) -> Cursor:

        docs = []
        for col in cols:
            print(col)
            docs.extend([doc for doc in self.db.collection(col).all()])

        return docs

        # col_string = ",".join(cols)
        # col_string = "["+col_string+"]"

        # debug_str = (f"FOR doc in {col_string} \n"
        #             f"RETURN doc")

        # cursor = self.db.aql.execute(f"""
        #     f"FOR doc in {col_string} \n"
        #     f"RETURN doc"
        # """)

        # return cursor

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
