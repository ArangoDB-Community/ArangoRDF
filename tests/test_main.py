from typing import Any, Dict

import pytest
from rdflib import Dataset
from rdflib import Graph as RDFGraph
from rdflib import Literal, URIRef
from rdflib.namespace import RDF, RDFS

from arango_rdf import ArangoRDF

from .conftest import adbrdf, db, get_rdf_graph


def test_constructor() -> None:
    bad_db: Dict[str, Any] = dict()

    with pytest.raises(TypeError):
        ArangoRDF(bad_db)


@pytest.mark.parametrize(
    "name, rdf_graph, num_urirefs, num_bnodes, num_literals, load_base_ontology",
    [
        ("Case_1_RPT", get_rdf_graph("cases/1.ttl"), 3, 0, 0, False),
        ("Case_2_1_RPT", get_rdf_graph("cases/2_1.ttl"), 4, 0, 2, False),
        ("Case_2_2_RPT", get_rdf_graph("cases/2_2.ttl"), 4, 0, 0, False),
        ("Case_2_3_RPT", get_rdf_graph("cases/2_3.ttl"), 5, 0, 0, False),
        ("Case_2_4_RPT", get_rdf_graph("cases/2_4.ttl"), 4, 0, 0, False),
        ("Case_3_1_RPT", get_rdf_graph("cases/3_1.ttl"), 1, 0, 4, False),
        ("Case_3_2_RPT", get_rdf_graph("cases/3_2.ttl"), 1, 0, 2, False),
        ("Case_4_RPT", get_rdf_graph("cases/4.ttl"), 2, 3, 3, False),
        ("Case_5_RPT", get_rdf_graph("cases/5.ttl"), 1, 1, 0, False),
        ("Case_6_RPT", get_rdf_graph("cases/6.trig"), 8, 0, 1, False),
        ("Case_7_RPT", get_rdf_graph("cases/7.ttl"), 3, 0, 0, False),
        ("RDFOwl", RDFGraph(), 42, 0, 1, True),
    ],
)
def test_rpt_basic_cases(
    name: str,
    rdf_graph: RDFGraph,
    num_urirefs: int,
    num_bnodes: int,
    num_literals: int,
    load_base_ontology: bool,
) -> None:
    STATEMENT_COL = f"{name}_Statement"
    URIREF_COL = f"{name}_URIRef"
    BNODE_COL = f"{name}_BNode"
    LITERAL_COL = f"{name}_Literal"

    # RDF to ArangoDB
    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(name, rdf_graph, True, load_base_ontology)

    assert adb_graph.edge_collection(STATEMENT_COL).count() == len(rdf_graph)
    assert adb_graph.vertex_collection(URIREF_COL).count() == num_urirefs
    assert adb_graph.vertex_collection(BNODE_COL).count() == num_bnodes
    assert adb_graph.vertex_collection(LITERAL_COL).count() == num_literals

    print("\n")

    # ArangoDB to RDF
    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    assert len(rdf_graph) == len(rdf_graph_2)

    if type(rdf_graph_2) is not Dataset:
        assert num_urirefs + num_bnodes + num_literals == len(rdf_graph_2.all_nodes())

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_1_PGT", get_rdf_graph("cases/1.ttl"))],
)
def test_pgt_case_1(name: str, rdf_graph: RDFGraph) -> None:
    # RDF to ArangoDB
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(name, rdf_graph, True, True)

    assert adb_graph.has_vertex_collection("Class")
    assert adb_graph.vertex_collection("Class").has("Class")
    assert adb_graph.vertex_collection("Class").has("Person")
    assert adb_graph.has_vertex_collection("Property")
    assert adb_graph.vertex_collection("Property").has("meets")
    assert adb_graph.vertex_collection("Property").has("type")
    assert adb_graph.has_vertex_collection("Person")
    assert adb_graph.vertex_collection("Person").has("alice")
    assert adb_graph.vertex_collection("Person").has("bob")
    assert adb_graph.has_edge_collection("type")
    assert adb_graph.edge_collection("type").has("alice-type-Person")
    assert adb_graph.edge_collection("type").has("bob-type-Person")
    assert adb_graph.edge_collection("type").has("Person-type-Class")
    assert adb_graph.has_edge_collection("meets")
    assert adb_graph.edge_collection("meets").has("alice-meets-bob")

    print("\n")

    # ArangoDB to RDF
    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    alice = URIRef("http://example.com/alice")
    bob = URIRef("http://example.com/bob")
    meets = URIRef("http://example.com/meets")
    person = URIRef("http://example.com/Person")

    # Original Statement assertions
    assert (alice, RDF.type, person) in rdf_graph_2
    assert (bob, RDF.type, person) in rdf_graph_2
    assert (alice, meets, bob) in rdf_graph_2

    # Ontology Assertions
    assert (person, RDF.type, RDFS.Class) in rdf_graph_2
    assert (meets, RDF.type, RDF.Property) in rdf_graph_2
    assert (RDF.Property, RDF.type, RDFS.Class) in rdf_graph_2
    assert (RDFS.Class, RDF.type, RDFS.Class) in rdf_graph_2
    assert (RDF.type, RDF.type, RDF.Property) in rdf_graph_2

    assert len(rdf_graph_2) == 8

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_2_1_PGT", get_rdf_graph("cases/2_1.ttl"))],
)
def test_pgt_case_2_1(name: str, rdf_graph: RDFGraph) -> None:
    # RDF to ArangoDB
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(name, rdf_graph, True, True)

    assert adb_graph.has_vertex_collection("Class")
    assert adb_graph.vertex_collection("Class").has("Class")
    assert adb_graph.vertex_collection("Class").has("Person")
    assert adb_graph.has_vertex_collection("Property")
    assert adb_graph.vertex_collection("Property").has("mentor")
    assert (
        adb_graph.vertex_collection("Property").get("mentor")["name"] == "mentor's name"
    )
    assert adb_graph.vertex_collection("Property").has("type")
    assert adb_graph.has_vertex_collection("Person")
    assert adb_graph.vertex_collection("Person").has("Sam")
    assert adb_graph.vertex_collection("Person").has("Lee")

    assert adb_graph.has_edge_collection("type")
    assert adb_graph.edge_collection("type").has("Sam-type-Person")
    assert adb_graph.edge_collection("type").has("Lee-type-Person")
    assert adb_graph.edge_collection("type").has("Person-type-Class")
    assert adb_graph.has_edge_collection("mentor")
    assert adb_graph.edge_collection("mentor").has("Sam-mentor-Lee")

    print("\n")

    # ArangoDB to RDF
    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    sam = URIRef("http://example.com/Sam")
    lee = URIRef("http://example.com/Lee")
    mentor = URIRef("http://example.com/mentor")
    mentor_name = URIRef("http://example.com/name")
    person = URIRef("http://example.com/Person")

    # Original Statement assertions
    assert (sam, RDF.type, person) in rdf_graph_2
    assert (lee, RDF.type, person) in rdf_graph_2
    assert (sam, mentor, lee) in rdf_graph_2
    assert (mentor, RDFS.label, Literal("project supervisor")) in rdf_graph_2
    assert (mentor, mentor_name, Literal("mentor's name")) in rdf_graph_2

    # Ontology Assertions
    assert (person, RDF.type, RDFS.Class) in rdf_graph_2
    assert (mentor, RDF.type, RDF.Property) in rdf_graph_2
    assert (mentor_name, RDF.type, RDF.Property) in rdf_graph_2
    assert (RDFS.label, RDF.type, RDF.Property) in rdf_graph_2
    assert (RDF.Property, RDF.type, RDFS.Class) in rdf_graph_2
    assert (RDFS.Class, RDF.type, RDFS.Class) in rdf_graph_2
    assert (RDF.type, RDF.type, RDF.Property) in rdf_graph_2

    assert len(rdf_graph_2) == 12

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_2_2_PGT", get_rdf_graph("cases/2_2.ttl"))],
)
def test_pgt_case_2_2(name: str, rdf_graph: RDFGraph) -> None:
    # RDF to ArangoDB
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(name, rdf_graph, True, True)

    assert adb_graph.has_vertex_collection("Class")
    assert adb_graph.vertex_collection("Class").has("Class")
    assert adb_graph.vertex_collection("Class").has("Property")
    assert adb_graph.has_vertex_collection("Property")
    assert adb_graph.vertex_collection("Property").has("mentorJoe")
    assert adb_graph.vertex_collection("Property").has("type")
    assert adb_graph.vertex_collection("Property").has("alias")

    assert adb_graph.has_vertex_collection(f"{name}_UnidentifiedNode")
    assert adb_graph.vertex_collection(f"{name}_UnidentifiedNode").has("Martin")
    assert adb_graph.vertex_collection(f"{name}_UnidentifiedNode").has("Joe")
    assert adb_graph.vertex_collection(f"{name}_UnidentifiedNode").has("teacher")
    assert adb_graph.has_edge_collection("type")
    assert adb_graph.edge_collection("type").has("mentorJoe-type-Property")
    assert adb_graph.has_edge_collection("mentorJoe")
    assert adb_graph.edge_collection("mentorJoe").has("Martin-mentorJoe-Joe")

    print("\n")

    # ArangoDB to RDF
    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    martin = URIRef("http://example.com/Martin")
    joe = URIRef("http://example.com/Joe")
    mentorJoe = URIRef("http://example.com/mentorJoe")
    alias = URIRef("http://example.com/alias")
    teacher = URIRef("http://example.com/teacher")

    # Original Statement assertions
    assert (martin, mentorJoe, joe) in rdf_graph_2
    assert (mentorJoe, alias, teacher) in rdf_graph_2

    # Ontology Assertions
    assert (mentorJoe, RDF.type, RDF.Property) in rdf_graph_2
    assert (alias, RDF.type, RDF.Property) in rdf_graph_2
    assert (RDF.Property, RDF.type, RDFS.Class) in rdf_graph_2
    assert (RDFS.Class, RDF.type, RDFS.Class) in rdf_graph_2
    assert (RDF.type, RDF.type, RDF.Property) in rdf_graph_2

    assert len(rdf_graph_2) == 7

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_2_3_PGT", get_rdf_graph("cases/2_3.ttl"))],
)
def test_pgt_case_2_3(name: str, rdf_graph: RDFGraph) -> None:
    # RDF to ArangoDB
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(name, rdf_graph, True, True)

    assert adb_graph.has_vertex_collection("Class")
    assert adb_graph.vertex_collection("Class").has("Class")
    assert adb_graph.vertex_collection("Class").has("Property")
    assert adb_graph.vertex_collection("Class").has("Person")
    assert adb_graph.has_vertex_collection("Property")
    assert adb_graph.vertex_collection("Property").has("supervise")
    assert adb_graph.vertex_collection("Property").has("type")
    assert adb_graph.has_vertex_collection("Person")
    assert adb_graph.vertex_collection("Person").has("Jan")
    assert adb_graph.vertex_collection("Person").has("Leo")

    assert adb_graph.has_edge_collection("subPropertyOf")
    assert adb_graph.edge_collection("subPropertyOf").has(
        "supervise-subPropertyOf-administer"
    )

    print("\n")

    # ArangoDB to RDF
    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    jan = URIRef("http://example.com/Jan")
    leo = URIRef("http://example.com/Leo")
    supervise = URIRef("http://example.com/supervise")
    administer = URIRef("http://example.com/administer")
    person = URIRef("http://example.com/Person")

    # Original Statement assertions
    assert (jan, RDF.type, person) in rdf_graph_2
    assert (leo, RDF.type, person) in rdf_graph_2
    assert (jan, supervise, leo) in rdf_graph_2
    assert (supervise, RDFS.subPropertyOf, administer) in rdf_graph_2

    # Ontology Assertions
    assert (person, RDF.type, RDFS.Class) in rdf_graph_2
    assert (administer, RDF.type, RDF.Property) in rdf_graph_2
    assert (supervise, RDF.type, RDF.Property) in rdf_graph_2
    assert (RDFS.subPropertyOf, RDF.type, RDF.Property) in rdf_graph_2
    assert (RDF.Property, RDF.type, RDFS.Class) in rdf_graph_2
    assert (RDFS.Class, RDF.type, RDFS.Class) in rdf_graph_2
    assert (RDF.type, RDF.type, RDF.Property) in rdf_graph_2

    assert len(rdf_graph_2) == 11

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_2_4_PGT", get_rdf_graph("cases/2_4.ttl"))],
)
def test_pgt_case_2_4(name: str, rdf_graph: RDFGraph) -> None:
    # RDF to ArangoDB
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(name, rdf_graph, True, True)

    assert adb_graph.has_edge_collection("type")
    assert adb_graph.edge_collection("type").has("friend-type-relation")
    assert adb_graph.edge_collection("type").has("friend-type-Property")
    assert adb_graph.edge_collection("type").has("relation-type-Class")

    assert adb_graph.has_edge_collection("friend")
    assert adb_graph.edge_collection("friend").has("Tom-friend-Chris")

    print("\n")

    # ArangoDB to RDF
    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    tom = URIRef("http://example.com/Tom")
    chris = URIRef("http://example.com/Chris")
    friend = URIRef("http://example.com/friend")
    relation = URIRef("http://example.com/relation")

    # Original Statement assertions
    assert (tom, friend, chris) in rdf_graph_2
    assert (friend, RDF.type, relation) in rdf_graph_2

    # Ontology Assertions
    assert (friend, RDF.type, RDF.Property) in rdf_graph_2
    assert (relation, RDF.type, RDFS.Class) in rdf_graph_2
    assert (RDF.Property, RDF.type, RDFS.Class) in rdf_graph_2
    assert (RDFS.Class, RDF.type, RDFS.Class) in rdf_graph_2
    assert (RDF.type, RDF.type, RDF.Property) in rdf_graph_2

    assert len(rdf_graph_2) == 7

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_3_1_PGT", get_rdf_graph("cases/3_1.ttl"))],
)
def test_pgt_case_3_1(name: str, rdf_graph: RDFGraph) -> None:
    # RDF to ArangoDB
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(name, rdf_graph, True, True)

    assert adb_graph.has_vertex_collection(f"{name}_UnidentifiedNode")
    assert adb_graph.vertex_collection(f"{name}_UnidentifiedNode").has("book")
    doc = adb_graph.vertex_collection(f"{name}_UnidentifiedNode").get("book")
    assert doc["index"] == "55"
    assert doc["cover"] == 20
    assert doc["pages"] == 100
    assert doc["publish_date"] == "1963-03-22"

    assert adb_graph.edge_definitions() == [
        {
            "edge_collection": "type",
            "from_vertex_collections": ["Property"],
            "to_vertex_collections": ["Class"],
        }
    ]

    assert adb_graph.has_vertex_collection("Property")
    assert adb_graph.vertex_collection("Property").has("index")
    assert adb_graph.vertex_collection("Property").has("pages")
    assert adb_graph.has_edge_collection("type")
    assert adb_graph.edge_collection("type").has("index-type-Property")

    print("\n")

    # ArangoDB to RDF
    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    book = URIRef("http://example.com/book")
    publish_date = URIRef("http://example.com/publish_date")
    pages = URIRef("http://example.com/pages")
    cover = URIRef("http://example.com/cover")
    index = URIRef("http://example.com/index")

    # Original Statement assertions
    assert (book, publish_date, Literal("1963-03-22")) in rdf_graph_2
    assert (book, pages, Literal(100)) in rdf_graph_2
    assert (book, cover, Literal(20)) in rdf_graph_2
    assert (book, index, Literal("55")) in rdf_graph_2

    # Ontology Assertions
    assert (publish_date, RDF.type, RDF.Property) in rdf_graph_2
    assert (pages, RDF.type, RDF.Property) in rdf_graph_2
    assert (cover, RDF.type, RDF.Property) in rdf_graph_2
    assert (index, RDF.type, RDF.Property) in rdf_graph_2
    assert (RDF.Property, RDF.type, RDFS.Class) in rdf_graph_2
    assert (RDFS.Class, RDF.type, RDFS.Class) in rdf_graph_2
    assert (RDF.type, RDF.type, RDF.Property) in rdf_graph_2

    assert len(rdf_graph_2) == 11

    db.delete_graph(name, drop_collections=True)


# NOTE: No current support for Literal datatype persistence in PGT Transformation
@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_3_2_PGT", get_rdf_graph("cases/3_2.ttl"))],
)
def test_pgt_case_3_2(name: str, rdf_graph: RDFGraph) -> None:
    # RDF to ArangoDB
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(name, rdf_graph, True, True)

    assert adb_graph.has_vertex_collection(f"{name}_UnidentifiedNode")
    doc = adb_graph.vertex_collection(f"{name}_UnidentifiedNode").get("book")
    assert "title" in doc
    assert type(doc["title"]) is list
    assert set(doc["title"]) == {"Book", "Bog"}

    print("\n")

    # ArangoDB to RDF (List Conversion Method = "collection")
    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    book = URIRef("http://example.com/book")
    title = URIRef("http://example.com/title")

    # Original Statement assertions
    assert (book, title, None) in rdf_graph_2
    assert (None, RDF.first, Literal("Bog")) in rdf_graph_2
    assert (None, RDF.first, Literal("Book")) in rdf_graph_2

    # Ontology Assertions
    assert (title, RDF.type, RDF.Property) in rdf_graph_2
    assert (RDF.Property, RDF.type, RDFS.Class) in rdf_graph_2
    assert (RDFS.Class, RDF.type, RDFS.Class) in rdf_graph_2
    assert (RDF.type, RDF.type, RDF.Property) in rdf_graph_2

    assert len(rdf_graph_2) == 9

    print("\n")

    # ArangoDB to RDF (List Conversion Method = "container")
    rdf_graph_3 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)(), "container")

    # Original Statement assertions
    assert (book, title, None) in rdf_graph_3
    assert (None, None, Literal("Bog")) in rdf_graph_3
    assert (None, None, Literal("Book")) in rdf_graph_3

    # Ontology Assertions
    assert (title, RDF.type, RDF.Property) in rdf_graph_3
    assert (RDF.Property, RDF.type, RDFS.Class) in rdf_graph_3
    assert (RDFS.Class, RDF.type, RDFS.Class) in rdf_graph_3
    assert (RDF.type, RDF.type, RDF.Property) in rdf_graph_3

    assert len(rdf_graph_3) == 7

    print("\n")

    # ArangoDB to RDF (List Conversion Method = "static")
    rdf_graph_4 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)(), "static")

    # Original Statement assertions
    assert (book, title, Literal("Book")) in rdf_graph_4
    assert (book, title, Literal("Bog")) in rdf_graph_4

    # Ontology Assertions
    assert (title, RDF.type, RDF.Property) in rdf_graph_4
    assert (RDF.Property, RDF.type, RDFS.Class) in rdf_graph_4
    assert (RDFS.Class, RDF.type, RDFS.Class) in rdf_graph_4
    assert (RDF.type, RDF.type, RDF.Property) in rdf_graph_4

    assert len(rdf_graph_4) == 6

    # ArangoDB to RDF (List Conversion Method = "bad_name")
    with pytest.raises(ValueError):
        adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)(), "bad_name")

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_4_PGT", get_rdf_graph("cases/4.ttl"))],
)
def test_pgt_case_4(name: str, rdf_graph: RDFGraph) -> None:
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(name, rdf_graph, True, True)

    assert adb_graph.has_vertex_collection(f"{name}_UnidentifiedNode")
    assert adb_graph.vertex_collection(f"{name}_UnidentifiedNode").has("List1")
    doc = adb_graph.vertex_collection(f"{name}_UnidentifiedNode").get("List1")

    assert "contents" in doc
    assert type(doc["contents"]) is list
    assert set(doc["contents"]) == {"one", "two", "three"}

    print("\n")

    # ArangoDB to RDF
    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    list1 = URIRef("http://example.com/List1")
    contents = URIRef("http://example.com/contents")

    # Original Statement assertions
    assert (list1, contents, None) in rdf_graph_2

    # Ontology Assertions
    assert (contents, RDF.type, RDF.Property) in rdf_graph_2
    assert (RDF.Property, RDF.type, RDFS.Class) in rdf_graph_2
    assert (RDFS.Class, RDF.type, RDFS.Class) in rdf_graph_2
    assert (RDF.type, RDF.type, RDF.Property) in rdf_graph_2

    assert len(rdf_graph_2) == 11

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_5_PGT", get_rdf_graph("cases/5.ttl"))],
)
def test_pgt_case_5(name: str, rdf_graph: RDFGraph) -> None:
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(name, rdf_graph, True, True)

    assert adb_graph.vertex_collection(f"{name}_UnidentifiedNode").count() == 2
    assert adb_graph.edge_collection("nationality").count() == 1

    print("\n")

    # ArangoDB to RDF
    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    bob = URIRef("http://example.com/bob")
    nationality = URIRef("http://example.com/nationality")

    # Original Statement assertions
    assert (bob, nationality, None) in rdf_graph_2

    # Ontology Assertions
    assert (nationality, RDF.type, RDF.Property) in rdf_graph_2
    assert (RDF.Property, RDF.type, RDFS.Class) in rdf_graph_2
    assert (RDFS.Class, RDF.type, RDFS.Class) in rdf_graph_2
    assert (RDF.type, RDF.type, RDF.Property) in rdf_graph_2

    assert len(rdf_graph_2) == 5

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_6_PGT", get_rdf_graph("cases/6.trig"))],
)
def test_pgt_case_6(name: str, rdf_graph: RDFGraph) -> None:
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(name, rdf_graph, True, True)

    assert adb_graph.has_vertex_collection("Person")
    doc = adb_graph.vertex_collection("Person").get("Monica")
    assert doc["name"] == "Monica"

    assert adb_graph.vertex_collection("Skill").count() == 2
    assert adb_graph.vertex_collection("Website").count() == 1

    edge = adb_graph.edge_collection("hasSkill").get("Monica-hasSkill-Management")
    assert edge["_sub_graph_uri"] == "http://example.com/Graph1"

    edge = adb_graph.edge_collection("type").get("Monica-type-Person")
    assert edge["_sub_graph_uri"] == "http://example.com/Graph2"

    print("\n")

    # ArangoDB to RDF
    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    person = URIRef("http://example.com/Person")
    monica = URIRef("http://example.com/Monica")
    monica_name = URIRef("http://example.com/name")
    monica_homepage = URIRef("http://example.com/homepage")

    homepage = URIRef("http://www.Monicahompage.org")
    management = URIRef("http://example.com/Management")
    skill = URIRef("http://example.com/Skill")
    hasSkill = URIRef("http://example.com/hasSkill")
    programming = URIRef("http://example.com/Programming")
    website = URIRef("http://example.com/Website")

    graph1 = URIRef("http://example.com/Graph1")
    graph2 = URIRef("http://example.com/Graph2")

    # Original Statement assertions
    assert (monica, monica_name, Literal("Monica")) in rdf_graph_2

    assert (management, RDF.type, skill, graph1) in rdf_graph_2
    assert (monica, hasSkill, management, graph1) in rdf_graph_2
    assert (monica, monica_homepage, homepage, graph1) in rdf_graph_2

    assert (programming, RDF.type, skill, graph2) in rdf_graph_2
    assert (homepage, RDF.type, website, graph2) in rdf_graph_2
    assert (monica, RDF.type, person, graph2) in rdf_graph_2
    assert (monica, hasSkill, programming, graph2) in rdf_graph_2

    # Ontology Assertions
    assert (monica_name, RDF.type, RDF.Property) in rdf_graph_2
    assert (monica_homepage, RDF.type, RDF.Property) in rdf_graph_2
    assert (hasSkill, RDF.type, RDF.Property) in rdf_graph_2
    assert (website, RDF.type, RDFS.Class) in rdf_graph_2
    assert (skill, RDF.type, RDFS.Class) in rdf_graph_2
    assert (person, RDF.type, RDFS.Class) in rdf_graph_2
    assert (RDF.Property, RDF.type, RDFS.Class) in rdf_graph_2
    assert (RDFS.Class, RDF.type, RDFS.Class) in rdf_graph_2
    assert (RDF.type, RDF.type, RDF.Property) in rdf_graph_2

    assert len(rdf_graph_2) == 20

    db.delete_graph(name, drop_collections=True)


# NOTE: Official assertions are TBD, given Case 7 dispute
@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_7_PGT", get_rdf_graph("cases/7.ttl"))],
)
def test_pgt_case_7(name: str, rdf_graph: RDFGraph) -> None:
    adbrdf.rdf_to_arangodb_by_pgt(name, rdf_graph, True, True)

    # TODO
    # assert True == False

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Collection_PGT", get_rdf_graph("collection.ttl"))],
)
def test_pgt_collection(name: str, rdf_graph: RDFGraph) -> None:
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(name, rdf_graph, True, True)

    doc = adb_graph.vertex_collection("TestDoc").get("Doc")
    assert "numbers" in doc
    assert doc["numbers"] == [
        1,
        [2, 3],
        [[4, 5]],
        [[6, 7]],
        [[8, 9], [10, 11]],
        [[[12], 13], 14],
        [15, [16, [17]]],
        18,
    ]
    assert "nested_container" in doc
    assert doc["nested_container"] == [[1, 2], [6, [7, 8, 9]]]
    assert "random" in doc
    assert doc["random"] == [["a", 1, ["b", 2, ["c", 3], 4], 5], [], True, 6.5]
    assert "planets" not in doc

    assert adb_graph.edge_collection("planets").count() == 4
    assert adb_graph.edge_collection("random").count() == 1
    assert adb_graph.edge_collection("random").get("Doc-random-Mars")

    print("\n")

    # ArangoDB to RDF
    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    assert len(rdf_graph_2) == 132
    doc = URIRef("http://example.org/test#Doc")
    numbers = URIRef("http://example.org/test#numbers")
    planets = URIRef("http://example.org/test#planets")
    random = URIRef("http://example.org/test#random")
    nested_container = URIRef("http://example.org/test#nested_container")

    assert (doc, numbers, None) in rdf_graph_2
    assert (doc, planets, None) in rdf_graph_2
    assert (doc, random, None) in rdf_graph_2
    assert (doc, nested_container, None) in rdf_graph_2

    assert len([i for i in rdf_graph_2.triples((None, RDF.first, None))]) == 55
    assert len([i for i in rdf_graph_2.triples((None, RDF.rest, None))]) == 55

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Container_PGT", get_rdf_graph("container.ttl"))],
)
def test_pgt_container(name: str, rdf_graph: RDFGraph) -> None:
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(name, rdf_graph, True, True)

    doc = adb_graph.vertex_collection("TestDoc").get("Doc")
    assert "numbers" in doc
    assert doc["numbers"] == [
        1,
        [2, 3],
        [[4, 5]],
        [[6, 7]],
        [[8, 9], [10, 11]],
        [[[12], 13], 14],
        [15, [16, [17]]],
        18,
    ]
    assert "planets" not in doc

    assert adb_graph.edge_collection("planets").count() == 4

    print("\n")

    # ArangoDB to RDF
    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(
        name, type(rdf_graph)(), list_conversion_mode="container"
    )

    assert len(rdf_graph_2) == 49
    doc = URIRef("http://example.org/test#Doc")
    numbers = URIRef("http://example.org/test#numbers")
    planets = URIRef("http://example.org/test#planets")

    assert (doc, numbers, None) in rdf_graph_2
    assert (doc, planets, None) in rdf_graph_2

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name",
    [("TestGraph")],
)
def test_adb_doc_with_dict_property_to_rdf(name: str) -> None:
    db.delete_graph(name, ignore_missing=True, drop_collections=True)
    db.create_graph(name, orphan_collections=["TestDoc"])

    doc = {
        "_key": "1",
        "val": {
            "sub_val_1": 1,
            "sub_val_2": {"sub_val_3": 3, "sub_val_4": [4]},
            "sub_val_5": [{"sub_val_6": 6}, {"sub_val_7": 7}],
        },
    }

    db.collection("TestDoc").insert(doc)

    adb = "http://www.arangodb.com#"
    test_doc = URIRef(f"{adb}TestDoc_1")

    rdf_graph = adbrdf.arangodb_graph_to_rdf("TestGraph", RDFGraph())
    assert len(rdf_graph) == 15
    assert (test_doc, URIRef(f"{adb}val"), None) in rdf_graph
    assert (None, URIRef(f"{adb}sub_val_1"), Literal(1)) in rdf_graph
    assert (None, URIRef(f"{adb}sub_val_2"), None) in rdf_graph
    assert (None, URIRef(f"{adb}sub_val_3"), Literal(3)) in rdf_graph
    assert (None, URIRef(f"{adb}sub_val_4"), None) in rdf_graph
    assert (None, RDF.first, Literal(4)) in rdf_graph
    assert (None, URIRef(f"{adb}sub_val_5"), None) in rdf_graph
    assert (None, URIRef(f"{adb}sub_val_6"), Literal(6)) in rdf_graph
    assert (None, URIRef(f"{adb}sub_val_7"), Literal(7)) in rdf_graph

    # TODO: Should this bring back the original dict structure?
    # Need to discuss...
    # adb_graph = adbrdf.rdf_to_arangodb_by_pgt(f"{name}2", rdf_graph)
    # db.delete_graph(f"{name}2", drop_collections=True)

    db.delete_graph(name, drop_collections=True)
