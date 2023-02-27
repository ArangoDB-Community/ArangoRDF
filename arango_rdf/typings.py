__all__ = ["Json", "ADBMetagraph", "ADBDocs", "RDFLists", "RDFObject", "RDFSubject"]

from typing import Any, DefaultDict, Dict, Set, Union

from rdflib import BNode, Literal, URIRef

Json = Dict[str, Any]
ADBMetagraph = Dict[str, Dict[str, Set[str]]]

# ADBDocsRPT = DefaultDict[str, List[Json]]
ADBDocs = DefaultDict[str, DefaultDict[str, Json]]

RDFObject = Union[URIRef, BNode, Literal]
RDFSubject = Union[URIRef, BNode]

RDFLists = DefaultDict[str, DefaultDict[str, Json]]
