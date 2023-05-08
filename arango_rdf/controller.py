#!/usr/bin/env python3
from typing import Set

from arango.database import StandardDatabase
from rdflib import Graph

from .abc import AbstractArangoRDFController
from .typings import RDFTerm
from .utils import Tree


class ArangoRDFController(AbstractArangoRDFController):
    """ArangoDB-RDF controller.

    You can derive your own custom ArangoRDFController.
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

        The "ideal RDFS Class" is defined as an RDFS Class whose local name can be
        used as the ArangoDB Document Collection that will store **rdf_resource**.

        This system is a work-in-progress. Users are welcome to overwrite this
        method via their own implementation of the `ArangoRDFController`
        Python Class.

        NOTE: Users are able to access the RDF Graph of the current
        RDF-to-ArangoDB transformation via the `self.rdf_graph`
        instance variable, and the database instance via the
        `self.db` instance variable.

        The current identification process goes as follows:
        1) If an RDF Resource only has one `rdf:type` statement
            (either by explicit definition or by domain/range inference),
            then the local name of the single RDFS Class is used as the ArangoDB
            Document Collection name. For example,
            <http://example.com/Bob> <rdf:type> <http://example.com/Person>
            would place the JSON Document for <http://example.com/Bob>
            under the ArangoDB "Person" Document Collection.

        2) If an RDF Resource has multiple `rdf:type` statements
            (either by explicit definition or by domain/range inference),
            with some (or all) of the RDFS Classes of those statements
            belonging in an `rdfs:subClassOf` Taxonomy, then the
            local name of the "most specific" Class within the Taxonomy is
            used (i.e the Class with the biggest depth). If there is a
            tie between 2+ Classes, then the URIs are alphabetically
            sorted & the first one is picked. Relies on **subclass_tree**.

        3) If an RDF Resource has multiple `rdf:type` statements, with
            none of the RDFS Classes of those statements belonging in an
            `rdfs:subClassOf` Taxonomy, then the URIs are
            alphabetically sorted & the first one is picked. The local
            name of the selected URI will be designated as the Document
            Collection for **rdf_resource**.

        :param rdf_resource: The RDF Resource in question.
        :type rdf_resource: URIRef | BNode
        :param class_set: A set of RDFS Class URIs that
            are associated to **rdf_resource** via the `RDF.Type`
            relationship, either via explicit definition or via
            domain/range inference.
        :type class_set: Set[str]
        :param subclass_tree: The Tree data structure representing
            the RDFS subClassOf Taxonomy. See `ArangoRDF.__build_subclass_tree()`
            for more info.
        :type subclass_tree: arango_rdf.utils.Tree
        :return: The most suitable RDFS Class URI among the set of RDFS Classes
            to use as the ArangoDB Document Collection name associated to
            **rdf_resource**.
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
