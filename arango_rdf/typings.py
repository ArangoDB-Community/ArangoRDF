__all__ = [
    "Json",
    "ADBMetagraph",
    "ADBDocs",
    "RDFLists",
    "RDFTerm",
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

RDFTerm = Union[URIRef, BNode, Literal]

RDFLists = DefaultDict[str, DefaultDict[RDFTerm, Json]]
TermMetadata = Tuple[str, str, str, str]

PredicateScope = DefaultDict[URIRef, DefaultDict[str, Set[Tuple[str, str]]]]
TypeMap = DefaultDict[RDFTerm, Set[str]]
