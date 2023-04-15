__all__ = [
    "Json",
    "ADBMetagraph",
    "ADBDocs",
    "RDFLists",
    "RDFObject",
    "RDFSubject",
    "TermMetadata",
    "PredicateScope",
    "TypeMap",
]

from typing import Any, DefaultDict, Dict, Set, Tuple, Union

from rdflib import BNode, Literal, URIRef

Json = Dict[str, Any]
ADBMetagraph = Dict[str, Dict[str, Set[str]]]

# ADBDocsRPT = DefaultDict[str, List[Json]]
ADBDocs = DefaultDict[str, DefaultDict[str, Json]]

RDFObject = Union[URIRef, BNode, Literal]
RDFSubject = Union[URIRef, BNode]

RDFLists = DefaultDict[str, DefaultDict[RDFSubject, Json]]
TermMetadata = Tuple[str, str, str, str]

PredicateScope = DefaultDict[URIRef, DefaultDict[str, Set[Tuple[str, str]]]]
TypeMap = DefaultDict[RDFSubject, Set[str]]
