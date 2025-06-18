__all__ = [
    "Json",
    "ADBMetagraph",
    "ADBDocs",
    "RDFListHeads",
    "RDFListData",
    "RDFTerm",
    "RDFTermMeta",
    "PredicateScope",
    "TypeMap",
]

from typing import Any, DefaultDict, Dict, List, Set, Tuple, Union

from rdflib import BNode, Literal, URIRef

Json = Dict[str, Any]
Jsons = List[Json]
ADBMetagraph = Dict[str, Dict[str, Set[str]]]

# ADBDocsRPT = DefaultDict[str, List[Json]]
ADBDocs = DefaultDict[str, DefaultDict[str, Json]]

RDFTerm = Union[URIRef, BNode, Literal]
RDFTermMeta = Tuple[RDFTerm, str, str, str]  # RDFTermMeta

RDFListHeads = DefaultDict[RDFTerm, Dict[RDFTerm, Json]]
RDFListData = DefaultDict[str, DefaultDict[RDFTerm, Json]]

PredicateScope = DefaultDict[URIRef, DefaultDict[str, Set[Tuple[str, str]]]]
TypeMap = DefaultDict[RDFTerm, Set[str]]
