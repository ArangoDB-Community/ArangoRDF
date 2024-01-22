The identification process is represented as follows:

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

1) `<http://www.arangodb.com/collection>`
Any RDF Statement of the form <http://example.com/Bob> <adb:collection> "Person"
will map the Subject to the ArangoDB Person" document collection.

2) <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> (rdf:type)
- This strategy is divided into 3 cases:
    2.1) If an RDF Resource only has one `rdf:type` statement,
        then the local name of the RDF Object is used as the ArangoDB
        Document Collection name. For example,
        <http://example.com/Bob> <rdf:type> <http://example.com/Person>
        would create an JSON Document for <http://example.com/Bob>,
        and place it under the "Person" Document Collection.
        NOTE: The RDF Object will also have its own JSON Document
        created, and will be placed under the "Class"
        Document Collection.

    2.2) If an RDF Resource has multiple `rdf:type` statements,
        with some (or all) of the RDF Objects of those statements
        belonging in an `rdfs:subClassOf` Taxonomy, then the
        local name of the "most specific" Class within the Taxonomy is
        used (i.e the Class with the biggest depth). If there is a
        tie between 2+ Classes, then the URIs are alphabetically
        sorted & the first one is picked.

    2.3) If an RDF Resource has multiple `rdf:type` statements, with
        none of the RDF Objects of those statements belonging in an
        `rdfs:subClassOf` Taxonomy, then the URIs are
        alphabetically sorted & the first one is picked. The local
        name of the selected URI will be designated as the Document
        collection for that Resource.

NOTE 1: If **contextualize_graph** is set to True, then additional
    `rdf:type` statements may be generated via ArangoRDF's Domain & Range
    Inference feature. These "synthetic" statements will be considered when
    mapping RDF Resources to the correct ArangoDB Collections, but ONLY if
    there were no "original" rdf:type statements to consider for
    the given RDF Resource.

NOTE 2: The ArangoDB Collection Mapping algorithm is a Work in Progress,
    and will most likely be subject to change for the time being.


To demo the ArangoDB Collection Mapping process,
        let us consider the following RDF Graph:

        ```
        @prefix ex: <http://example.com/> .
        @prefix adb: <http://www.arangodb.com/> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

        ex:B rdfs:subClassOf ex:A .
        ex:C rdfs:subClassOf ex:A .
        ex:D rdfs:subClassOf ex:C .

        ex:alex rdf:type ex:A .

        ex:sam ex:age 25 .
        ex:age rdfs:domain ex:A

        ex:john rdf:type ex:B .
        ex:john rdf:type ex:D .

        ex:mike rdf:type ex:G
        ex:mike rdf:type ex:F
        ex:mike rdf:type ex:E

        ex:frank adb:collection "Z" .
        ex:frank rdf:type D .

        ex:bob ex:name "Bob" .
        ```
        Given the RDF TTL Snippet above, we can derive the following
        ArangoDB Collection mappings:

        ex:alex --> "A"
            - This RDF Resource only has one associated `rdf:type` statement.

        ex:sam --> "A"
            - Although this RDF Resource has no `rdf:type` associated statement,
            we can infer from the domain of the property it uses (ex:age) that
            it is of type ex:A.

        ex:john --> "D"
            - This RDF Resource has 2 `rdf:type` statements, but `ex:D` is "deeper"
            than `ex:B` when considering the `rdfs:subClassOf` Taxonomy.

        ex:mike --> "E"
            - This RDF Resource has multiple `rdf:type` statements, with
            none belonging to the `rdfs:subClassOf` Taxonomy.
            Therefore, Alphabetical Sorting is used.

        ex:frank --> "Z"
            - This RDF Resource has an `adb:collection` statement associated
            to it, which is prioritized over any other `rdf:type`
            statement it may have.

        ex:bob --> "UnknownResource"
            - This RDF Resource has neither an `rdf:type` statement
            nor an `adb:collection` statement associated to it. It
            is therefore placed under the "UnknownResource"
            Document Collection.


        A common use case would look like this:

        .. code-block:: python
            from rdflib import Graph
            from arango_rdf import ArangoRDF

            adbrdf = ArangoRDF(db)

            g = Graph()
            g.parse(...)
            g.add(...)

            adb_col_statements = adbrdf.write_adb_col_statements(g)
            adb_col_statements.serialize(...)
            adb_col_statements.add(...)
            adb_col_statements.remove(...)

            adbrdf.rdf_to_arangodb_by_pgt(
                'MyGraph', rdf_graph=g, adb_col_statements=adb_col_statements
            )


        For example, the `adb_col_statements` may look like this:

        .. code-block::
            @prefix adb: <http://www.arangodb.com/> .

            <http://example.com/bob> adb:collection "Person" .
            <http://example.com/alex> adb:collection "Person" .
            <http://example.com/name> adb:collection "Property" .
            <http://example.com/Person> adb:collection "Class" .
            <http://example.com/charlie> adb:collection "Dog" .
