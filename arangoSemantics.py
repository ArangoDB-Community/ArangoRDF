from typing import List, Union
from rdflib import Graph, URIRef, Literal, BNode
#from rdflib.namespace import RDFS, OWL
from arango import ArangoClient
from arango.collection import StandardCollection, EdgeCollection
import hashlib


class ArangoSemantics():

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

        self.collections = {}


    def init_rdf_collections(self, iri: str = "IRI", bnode: str = "BNode", literal: str = "Literal", edge: str = "Statment") -> None:
        #init collections
        self.init_collection(iri, "iri")
        self.init_collection(bnode, "bnode")
        self.init_collection(literal, "literal")
        self.init_edge_collection(edge, [iri, bnode], [iri, literal, bnode], "statment")

    def init_ontology_collections(self, cls: str = "Class", rel: str = "Relationship",prop: str = "Property", sub_cls: str = "SubClassOf", sub_prop: str ="SubPropertyOf", range: str = "Range", domain: str = "Domain"):
        self.init_collection(cls, "class")
        self.init_collection(rel, "rel")
        self.init_collection(prop, "prop")

        self.init_edge_collection(sub_cls, [cls], [cls], "sub_class")
        self.init_edge_collection(sub_prop, [prop], [prop], "sub_prop")
        self.init_edge_collection(range, [rel], [cls], "range")
        self.init_edge_collection(domain, [rel],[cls], "domain")
        

    def init_collection(self, name: str, default_name: str) -> None:
        if self.db.has_collection(name):
            collection = self.db.collection(name)
        else:
            collection = self.db.create_collection(name)
        self.collections[default_name] = collection

        
    def init_edge_collection(self, name, parent_collections: List[str], child_collections: List[str], default_name: str) -> None:

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
        
        self.collections[default_name] = collection 


    def import_rdf(self, data: str) -> None:
        
        self.rdf_graph.parse(data)

        graph_id = self.rdf_graph.identifier.toPython()

        for s, p, o in self.rdf_graph:

            #build subject doc
            if isinstance(s, URIRef):
                s_id = self.insert_doc(self.collections["iri"], self.build_iri_doc(s, self.collections["iri"]))
            elif isinstance(s, BNode):
                s_id = self.insert_doc(self.collections["bnode"], self.build_bnode_doc(s, self.collections["bnode"]))
            else:
                raise ValueError("Subject must be IRI or Blank Node")
            
            #build object doc
            if isinstance(o, URIRef):
                o_id = self.insert_doc(self.collections["iri"], self.build_iri_doc(o, self.collections["iri"]))
            elif isinstance(o, BNode):
                o_id = self.insert_doc(self.collections["bnode"], self.build_bnode_doc(o, self.collections["bnode"]))
            elif isinstance(o, Literal):
                o_id = self.insert_doc(self.collections["literal"], self.build_literal_doc(o, self.collections["literal"]))
            else:
                raise ValueError("Object must be IRI, Blank Node, or Literal")
            
            #build and insert edge
            self.insert_edge(self.collections["statment"], self.build_statment_edge(p, s_id, o_id, graph_id, self.collections["statment"]))
            
        
        return

    
    def import_ontology(self, data: str, format="xml") -> None:

        self.rdf_graph.parse(data, format=format)
        graph_id = self.rdf_graph.identifier.toPython()


        for s, p, o in self.rdf_graph:

            #if object #Class then add subject to class collection
            if not isinstance(o, Literal):
                if "#Class" in o.toPython():
                    self.build_node(s, self.collections["class"])
                    
                    continue

            #add objectProperty to relationship collection
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
                self.insert_edge(self.collections["sub_class"], self.build_statment_edge(p, s_id, o_id, graph_id, self.collections["sub_class"]))
                continue
            
            #if predicate is #domain create relationship node and connect to class node
            if "#domain" in p.toPython():
                s_id = self.build_node(s, self.collections["rel"])
                o_id = self.build_node(o, self.collections["class"])
                self.insert_edge(self.collections["domain"], self.build_statment_edge(p, s_id, o_id, graph_id, self.collections["domain"]))
                continue
            
            
            if "#range" in p.toPython():
                s_id = self.build_node(s, self.collections["rel"])
                o_id = self.build_node(o, self.collections["class"])
                self.insert_edge(self.collections["range"], self.build_statment_edge(p, s_id, o_id, graph_id, self.collections["range"]))  
                continue


            if "#subPropertyOf" in p.toPython():
                o_id = self.build_node(o, self.collections["prop"])
                s_id = self.build_node(o, self.collections["prop"])
                self.insert_edge(self.collections["sub_prop"], self.build_statment_edge(p, s_id, o_id, graph_id, self.collections["sub_prop"]))
                

        return 

    def build_node(self, doc: Union[URIRef,BNode], collection: StandardCollection) -> dict:
        if isinstance(doc, URIRef):
            node = self.build_iri_doc(doc, collection)
        elif isinstance(doc, BNode):
            node = self.build_bnode_doc(doc, collection)
        else:
            raise ValueError("Document must be IRI or Blank Node")

        node_id = self.insert_doc(collection, node)
        return node_id


    def build_iri_doc(self, iri: URIRef, collection: StandardCollection) -> dict:
        key = hashlib.md5(str(iri).encode('utf-8')).hexdigest()
        id = f"{collection.name}/{key}"

        doc = {"_key":key, "_iri":iri.toPython(), "_id": id}
        
        return doc


    def build_bnode_doc(self, bnode: BNode,collection: StandardCollection) -> dict:
        key=bnode.toPython()
        id = f"{collection.name}/{key}"

        doc={"_key": key, "_id": id}

        return doc


    def build_literal_doc(self, literal: Literal, collection: StandardCollection) -> dict:

        lang = str(literal.language)
        type = str(literal.datatype)
        value = str(literal.value)
        key_string = value+type+lang
        key = hashlib.md5(key_string.encode('utf-8')).hexdigest()
        id = f"{collection.name}/{key}"

        doc = {"_id": id, "_key": key, "_value":value, "_type": type, "_lang": lang}
        return doc


    def build_statment_edge(self, predicate: URIRef, subject_id: dict, object_id: dict, graph: str, collection: StandardCollection):
        _iri = predicate.toPython()
        _from = subject_id["_id"]
        _predicate = hashlib.md5(_iri.encode('utf-8')).hexdigest()
        _to = object_id["_id"]
        key_string = str(_from+_predicate+_to+graph)
        _key = hashlib.md5(key_string.encode('utf-8')).hexdigest()
        _id = f"{collection.name}/{_key}"

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