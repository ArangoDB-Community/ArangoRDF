from time import process_time_ns
from typing import List
from rdflib import RDF, Graph, URIRef, Literal, BNode
from rdflib.namespace import RDFS, XSD
from arango import ArangoClient
from arango.collection import StandardCollection, EdgeCollection
import hashlib


class ArangoSemantic():

    def __init__(self, host: str, username: str, password: str, database: str, graph: str) -> None:
        
        self.connection = ArangoClient(hosts=host)
        sys_db = self.connection.db('_system', username=username, password=password, verify=False)

        if sys_db.has_database(database):
            self.db = self.connection.db(database, username=username, password=password, verify=False)
        else:
            sys_db.create_database(database)
            self.db = self.connection.db(database, username=username, password=password, verify=False)

        # Create the graph 
        if self.db.has_graph(graph):
            self.graph = self.db.graph(graph)
        else:
            self.graph = self.db.create_graph(graph)
        
        #init rdflib graph
        self.rdf_graph = Graph()


    def init_rdf_collections(self, iri: str = "IRI", bnode: str = "BNode", literal: str = "Literal", edge: str = "Statment") -> None:
        #init collections
        self.iri_collection = self.init_collection(iri)
        self.bnode_collection = self.init_collection(bnode)
        self.literal_collection = self.init_collection(literal)
        self.statment_collection = self.init_edge_collection(edge, [iri, bnode], [iri, literal, bnode])


    def init_collection(self, name: str) -> StandardCollection:
        if self.db.has_collection(name):
            collection = self.db.collection(name)
        else:
            collection = self.db.create_collection(name)
        return collection

        
    def init_edge_collection(self, name, parent_collections: List[str], child_collections: List[str]) -> EdgeCollection:

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
            self.graph.replace_edge_definition(edge_collection=name,
                                                from_vertex_collections=new_from_vc,
                                                to_vertex_collections=new_to_vc)

            collection = self.graph.edge_collection(name)
        else:
            collection = self.graph.create_edge_definition(
                edge_collection=name,
                from_vertex_collections=parent_collections,
                to_vertex_collections=child_collections)
        
        return collection 


    def import_rdf(self, data: str) -> None:
        
        self.rdf_graph.parse(data)

        graph_id = self.rdf_graph.identifier.toPython()

        count = 1
        for s, p, o in self.rdf_graph:

            #build subject doc
            if isinstance(s, URIRef):
                s_id = self.insert_doc(self.iri_collection, self.build_iri_doc(s))
            elif isinstance(s, BNode):
                s_id = self.insert_doc(self.bnode_collection, self.build_bnode_doc(s))
            else:
                raise ValueError("Subject must be IRI or Blank Node")
            
            #build object doc
            if isinstance(o, URIRef):
                o_id = self.insert_doc(self.iri_collection, self.build_iri_doc(o))
            elif isinstance(o, BNode):
                o_id = self.insert_doc(self.bnode_collection, self.build_bnode_doc(o))
            elif isinstance(o, Literal):
                o_id = self.insert_doc(self.literal_collection, self.build_literal_doc(o))
            else:
                raise ValueError("Object must be IRI, Blank Node, or Literal")
            
            #build and insert edge
            self.insert_edge(self.statment_collection, self.build_statment_edge(p, s_id, o_id, graph_id))
            
            count += 1
        
        return

           
    def build_iri_doc(self, iri: URIRef) -> dict:
        key = hashlib.md5(str(iri).encode('utf-8')).hexdigest()
        id = f"{self.iri_collection.name}/{key}"

        doc = {"_key":key, "_iri":iri.toPython(), "_id": id}
        
        return doc


    def build_bnode_doc(self, bnode: BNode) -> dict:
        key=bnode.toPython()
        id = f"{self.bnode_collection.name}/{key}"

        doc={"_key": key, "_id": id}

        return doc


    def build_literal_doc(self, literal: Literal) -> dict:

        lang = str(literal.language)
        type = str(literal.datatype)
        value = str(literal.value)
        key_string = value+type+lang
        key = hashlib.md5(key_string.encode('utf-8')).hexdigest()
        id = f"{self.literal_collection.name}/{key}"

        doc = {"_id": id, "_key": key, "_value":value, "_type": type, "_lang": lang}
        return doc


    def build_statment_edge(self, predicate: URIRef, subject_id: dict, object_id: dict, graph: str):
        _iri = predicate.toPython()
        _from = subject_id["_id"]
        _predicate = hashlib.md5(_iri.encode('utf-8')).hexdigest()
        _to = object_id["_id"]
        key_string = str(_from+_predicate+_to+graph)
        _key = hashlib.md5(key_string.encode('utf-8')).hexdigest()
        _id = f"{self.statment_collection.name}/{_key}"

        doc = {"_id": _id, "_key": _key, "_iri": _iri, "_from": _from, "_predicate": _predicate, "_to": _to}

        return doc


    def insert_doc(self, collection: StandardCollection, doc: dict) -> dict:
        col_name = collection.name
        
        cursor = self.db.aql.execute(
            f"UPSERT {{ _id: '{doc['_id']}' }}\n"
            f"INSERT  {doc} \n"
            f"UPDATE {{}}\n"
            f"IN {col_name}\n"
            f"LET doc = IS_NULL(OLD) ? NEW : OLD \n"
            f"RETURN {{ _id: doc._id}}")

        return cursor.pop()


    def insert_edge(self, collection: EdgeCollection, edge: dict) -> dict:
        col_name = collection.name
        cursor = self.db.aql.execute(
            f"UPSERT {{ _from: '{edge['_from']}', _to: '{edge['_to']}' }}\n"
            f"INSERT  {edge} \n"
            f"UPDATE {{}}\n"
            f"IN {col_name}\n"
            f"LET doc = IS_NULL(OLD) ? NEW : OLD \n"
            f"RETURN {{ _id: doc._id}}")
        
        return cursor.pop()