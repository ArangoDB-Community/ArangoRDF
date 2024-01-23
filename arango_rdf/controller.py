#!/usr/bin/env python3
from typing import Set

from arango.database import StandardDatabase
from rdflib import Graph

from .abc import AbstractArangoRDFController
from .typings import RDFTerm
from .utils import Tree


class ArangoRDFController(AbstractArangoRDFController):
    """Controller used in RDF-to-ArangoDB (PGT).

    Responsible for handling how the ArangoDB Collection Mapping Process
    identifies the "ideal RDFS Class" among a selection of RDFS Classes
    for a given RDF Resource.

    The "ideal RDFS Class" is defined as an RDFS Class whose local name best
    represents the RDF Resource in question. This local name will be
    used as the ArangoDB Collection name that will store **rdf_resource**.

    `Read more about how the PGT ArangoDB Collection Mapping
    Process works here
    <./rdf_to_arangodb_pgt.html#arangodb-collection-mapping-process>`_.
    """

    def __init__(self) -> None:
        self.db: StandardDatabase
        self.rdf_graph: Graph

    def identify_best_class(
        self,
        rdf_resource: RDFTerm,
        class_set: Set[str],
        subclass_tree: Tree,
    ) -> str:
        """Find the ideal RDFS Class among a selection of RDFS Classes. Essential
        for the ArangoDB Collection Mapping Process used in RDF-to-ArangoDB (PGT).

        `Read more about how the PGT ArangoDB Collection Mapping
        Process works here
        <./rdf_to_arangodb_pgt.html#arangodb-collection-mapping-process>`_.

        The "ideal RDFS Class" is defined as an RDFS Class whose local name best
        represents the RDF Resource in question. This local name will be
        used as the ArangoDB Collection name that will store **rdf_resource**.

        This system is a work-in-progress. Users are welcome to overwrite this
        method via their own implementation of the `ArangoRDFController`
        Class. Users are able to access the RDF Graph of the current
        RDF-to-ArangoDB transformation via `self.rdf_graph`, and the
        database instance via the  `self.db`.

        :param rdf_resource: The RDF Resource in question.
        :type rdf_resource: URIRef | BNode
        :param class_set: A set of RDFS Class URIs that
            are associated to **rdf_resource** via the `RDF.Type`
            relationship, either via explicit definition or via
            domain/range inference.
        :type class_set: Set[str]
        :param subclass_tree: The Tree data structure representing
            the RDFS subClassOf Taxonomy.
            See :func:`arango_rdf.main.ArangoRDF.__build_subclass_tree` for more info.
        :type subclass_tree: arango_rdf.utils.Tree
        :return: The string representation of the URI of the most suitable
            RDFS Class URI among the set of RDFS Classes to use as the ArangoDB
            Document Collection name for **rdf_resource**.
        :rtype: str
        """
        # These are accessible!
        # print(self.db)
        # print(self.rdf_graph)

        best_class = ""

        if len(class_set) == 1:
            best_class = list(class_set)[0]

        elif any([c in subclass_tree for c in class_set]):
            best_depth = -1

            for c in sorted(class_set):
                depth = subclass_tree.get_node_depth(c)

                if depth > best_depth:
                    best_depth = depth
                    best_class = c

        else:
            best_class = sorted(class_set)[0]

        return best_class
