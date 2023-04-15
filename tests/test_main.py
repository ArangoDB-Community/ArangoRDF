from typing import Any, Dict

import pytest
from rdflib import ConjunctiveGraph as RDFConjunctiveGraph
from rdflib import Dataset
from rdflib import Graph as RDFGraph
from rdflib import Literal, URIRef
from rdflib.namespace import RDF, RDFS

from arango_rdf import ArangoRDF

from .conftest import (
    META_GRAPH_ALL_RESOURCES,
    META_GRAPH_CONTEXTUALIZE_STATEMENTS,
    META_GRAPH_IDENTIFIED_RESOURCES,
    META_GRAPH_LITERAL_STATEMENTS,
    META_GRAPH_NON_LITERAL_STATEMENTS,
    META_GRAPH_SIZE,
    META_GRAPH_UNKNOWN_RESOURCES,
    adbrdf,
    db,
    get_adb_graph_count,
    get_meta_graph,
    get_rdf_graph,
    outersect_graphs,
)

# def adbrdf.rdf_id_to_adb_key(rdf_id: str):
#     # return hashlib.md5(rdf_id.encode()).hexdigest()
#     return xxhash.xxh64(rdf_id.encode()).hexdigest()


def test_constructor() -> None:
    bad_db: Dict[str, Any] = dict()

    with pytest.raises(TypeError):
        ArangoRDF(bad_db)


@pytest.mark.parametrize(
    "name, rdf_graph, num_triples, num_urirefs, num_bnodes, \
        num_literals, contextualize_graph",
    [
        ("Case_1_RPT", get_rdf_graph("cases/1.ttl"), 3, 3, 0, 0, False),
        ("Case_1_RPT", get_rdf_graph("cases/1.ttl"), 12, 9, 0, 0, True),
        ("Case_2_1_RPT", get_rdf_graph("cases/2_1.ttl"), 5, 4, 0, 2, False),
        ("Case_2_2_RPT", get_rdf_graph("cases/2_2.ttl"), 2, 4, 0, 0, False),
        ("Case_2_3_RPT", get_rdf_graph("cases/2_3.ttl"), 4, 5, 0, 0, False),
        ("Case_2_4_RPT", get_rdf_graph("cases/2_4.ttl"), 2, 4, 0, 0, False),
        ("Case_3_1_RPT", get_rdf_graph("cases/3_1.ttl"), 4, 1, 0, 4, False),
        ("Case_3_2_RPT", get_rdf_graph("cases/3_2.ttl"), 2, 1, 0, 2, False),
        ("Case_4_RPT", get_rdf_graph("cases/4.ttl"), 7, 2, 3, 3, False),
        ("Case_5_RPT", get_rdf_graph("cases/5.ttl"), 2, 1, 1, 1, False),
        ("Case_6_RPT", get_rdf_graph("cases/6.trig"), 11, 9, 0, 1, False),
        ("Case_7_RPT", get_rdf_graph("cases/7.ttl"), 20, 17, 0, 1, False),
        (
            "Meta_RPT",
            get_meta_graph(),
            META_GRAPH_SIZE,
            META_GRAPH_ALL_RESOURCES,
            0,
            META_GRAPH_LITERAL_STATEMENTS,
            False,
        ),
        (
            "Meta_RPT",
            get_meta_graph(),
            META_GRAPH_SIZE + META_GRAPH_CONTEXTUALIZE_STATEMENTS,
            META_GRAPH_ALL_RESOURCES,
            0,
            META_GRAPH_LITERAL_STATEMENTS,
            True,
        ),
    ],
)
def test_rpt_cases(
    name: str,
    rdf_graph: RDFGraph,
    num_triples: int,
    num_urirefs: int,
    num_bnodes: int,
    num_literals: int,
    contextualize_graph: bool,
) -> None:
    STATEMENT_COL = f"{name}_Statement"
    URIREF_COL = f"{name}_URIRef"
    BNODE_COL = f"{name}_BNode"
    LITERAL_COL = f"{name}_Literal"

    # RDF to ArangoDB
    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph,
        contextualize_graph,
        overwrite_graph=True,
    )

    assert adb_graph.edge_collection(STATEMENT_COL).count() == num_triples
    assert adb_graph.vertex_collection(URIREF_COL).count() == num_urirefs
    assert adb_graph.vertex_collection(BNODE_COL).count() == num_bnodes
    assert adb_graph.vertex_collection(LITERAL_COL).count() == num_literals

    print("\n")

    # ArangoDB to RDF
    rdf_graph_2, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    if contextualize_graph:
        assert len(rdf_graph_2) >= len(rdf_graph)
    else:
        assert len(rdf_graph_2) == len(rdf_graph)

    if not isinstance(rdf_graph_2, Dataset):
        assert num_urirefs + num_bnodes + num_literals == len(rdf_graph_2.all_nodes())

    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0
    if not contextualize_graph:
        assert len(outersect_graphs(rdf_graph_2, rdf_graph)) == 0
    else:
        for _, p, _, *_ in outersect_graphs(rdf_graph_2, rdf_graph):
            assert p in {RDF.type, RDFS.domain, RDFS.range}

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Meta_PGT", get_meta_graph())],
)
def test_pgt_meta(name: str, rdf_graph: RDFConjunctiveGraph) -> None:
    adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph,
        contextualize_graph=True,
        overwrite_graph=True,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == META_GRAPH_ALL_RESOURCES
    assert e_count == META_GRAPH_NON_LITERAL_STATEMENTS
    assert (
        db.collection(f"{name}_UnknownResource").count() == META_GRAPH_UNKNOWN_RESOURCES
    )

    rdf_graph_2, adb_mapping = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())
    assert len(rdf_graph_2) == len(rdf_graph) + META_GRAPH_CONTEXTUALIZE_STATEMENTS
    assert len(adb_mapping) == META_GRAPH_IDENTIFIED_RESOURCES
    assert {
        str(l) for l in adb_mapping.objects(subject=None, predicate=None, unique=True)
    } == {"Class", "Property", "List", "Ontology"}

    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0
    diff = outersect_graphs(rdf_graph_2, rdf_graph)
    assert len(diff) == META_GRAPH_CONTEXTUALIZE_STATEMENTS

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_1_PGT", get_rdf_graph("cases/1.ttl"))],
)
def test_pgt_case_1(name: str, rdf_graph: RDFGraph) -> None:
    size = len(rdf_graph)
    unique_nodes = 4
    identified_unique_nodes = 4
    non_literal_statements = 3
    contextualize_statements = 4

    # RDF to ArangoDB
    rdf_graph = adbrdf.load_meta_ontology(rdf_graph)
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name, rdf_graph, overwrite_graph=True, contextualize_graph=True
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == META_GRAPH_ALL_RESOURCES + unique_nodes
    assert (
        e_count
        == META_GRAPH_NON_LITERAL_STATEMENTS
        + non_literal_statements
        + contextualize_statements
    )

    _class = adbrdf.rdf_id_to_adb_key(str(RDFS.Class))
    _type = adbrdf.rdf_id_to_adb_key(str(RDF.type))
    _person = adbrdf.rdf_id_to_adb_key("http://example.com/Person")
    _meets = adbrdf.rdf_id_to_adb_key("http://example.com/meets")
    _alice = adbrdf.rdf_id_to_adb_key("http://example.com/alice")
    _bob = adbrdf.rdf_id_to_adb_key("http://example.com/bob")

    assert adb_graph.has_vertex_collection("Class")
    assert adb_graph.vertex_collection("Class").has(_class)
    assert adb_graph.vertex_collection("Class").has(_person)
    assert adb_graph.has_vertex_collection("Property")
    assert adb_graph.vertex_collection("Property").has(_meets)
    assert adb_graph.vertex_collection("Property").has(_type)
    assert adb_graph.has_vertex_collection("Person")
    assert adb_graph.vertex_collection("Person").has(_alice)
    assert adb_graph.vertex_collection("Person").has(_bob)
    assert adb_graph.has_edge_collection("type")
    assert adb_graph.edge_collection("type").has(f"{_alice}-{_type}-{_person}")
    assert adb_graph.edge_collection("type").has(f"{_bob}-{_type}-{_person}")
    assert adb_graph.edge_collection("type").has(f"{_person}-{_type}-{_class}")
    assert adb_graph.has_edge_collection("meets")
    assert adb_graph.edge_collection("meets").has(f"{_alice}-{_meets}-{_bob}")

    print("\n")

    # ArangoDB to RDF
    rdf_graph_2, adb_mapping = adbrdf.arangodb_graph_to_rdf(name, RDFConjunctiveGraph())

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

    assert (
        len(rdf_graph_2)
        == META_GRAPH_SIZE
        + META_GRAPH_CONTEXTUALIZE_STATEMENTS
        + size
        + contextualize_statements
    )
    assert len(adb_mapping) == META_GRAPH_IDENTIFIED_RESOURCES + identified_unique_nodes
    assert {
        str(l) for l in adb_mapping.objects(subject=None, predicate=None, unique=True)
    } == {"Class", "Property", "List", "Ontology", "Person"}

    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0
    diff = outersect_graphs(rdf_graph_2, rdf_graph)
    assert len(diff) == META_GRAPH_CONTEXTUALIZE_STATEMENTS + contextualize_statements
    for _, p, _, *_ in outersect_graphs(rdf_graph_2, rdf_graph):
        assert p in {RDF.type, RDFS.domain, RDFS.range}

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_2_1_PGT", get_rdf_graph("cases/2_1.ttl"))],
)
def test_pgt_case_2_1(name: str, rdf_graph: RDFGraph) -> None:
    size = len(rdf_graph)
    unique_nodes = 5
    identified_unique_nodes = 5
    non_literal_statements = 3
    contextualize_statements = 5

    # RDF to ArangoDB
    rdf_graph = adbrdf.load_meta_ontology(rdf_graph)
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name, rdf_graph, overwrite_graph=True, contextualize_graph=True
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == META_GRAPH_ALL_RESOURCES + unique_nodes
    assert (
        e_count
        == META_GRAPH_NON_LITERAL_STATEMENTS
        + non_literal_statements
        + contextualize_statements
    )

    _class = adbrdf.rdf_id_to_adb_key(str(RDFS.Class))
    _type = adbrdf.rdf_id_to_adb_key(str(RDF.type))
    _label = adbrdf.rdf_id_to_adb_key(str(RDFS.label))
    _person = adbrdf.rdf_id_to_adb_key("http://example.com/Person")
    _name = adbrdf.rdf_id_to_adb_key("http://example.com/name")
    _mentor = adbrdf.rdf_id_to_adb_key("http://example.com/mentor")
    _sam = adbrdf.rdf_id_to_adb_key("http://example.com/Sam")
    _lee = adbrdf.rdf_id_to_adb_key("http://example.com/Lee")

    assert adb_graph.has_vertex_collection("Class")
    assert adb_graph.vertex_collection("Class").has(_class)
    assert adb_graph.vertex_collection("Class").has(_person)
    assert adb_graph.has_vertex_collection("Property")
    assert adb_graph.vertex_collection("Property").has(_type)
    assert adb_graph.vertex_collection("Property").has(_label)
    assert adb_graph.vertex_collection("Property").has(_name)
    assert adb_graph.vertex_collection("Property").has(_mentor)
    doc = adb_graph.vertex_collection("Property").get(_mentor)
    assert doc["label"] == "project supervisor"
    assert doc["name"] == "mentor's name"
    assert adb_graph.has_vertex_collection("Person")
    assert adb_graph.vertex_collection("Person").has(_sam)
    assert adb_graph.vertex_collection("Person").has(_lee)

    assert adb_graph.has_edge_collection("type")
    assert adb_graph.edge_collection("type").has(f"{_sam}-{_type}-{_person}")
    assert adb_graph.edge_collection("type").has(f"{_lee}-{_type}-{_person}")
    assert adb_graph.edge_collection("type").has(f"{_person}-{_type}-{_class}")
    assert adb_graph.has_edge_collection("mentor")
    assert adb_graph.edge_collection("mentor").has(f"{_sam}-{_mentor}-{_lee}")

    print("\n")

    # ArangoDB to RDF
    rdf_graph_2, adb_mapping = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

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

    assert (
        len(rdf_graph_2)
        == META_GRAPH_SIZE
        + META_GRAPH_CONTEXTUALIZE_STATEMENTS
        + size
        + contextualize_statements
    )
    assert len(adb_mapping) == META_GRAPH_IDENTIFIED_RESOURCES + identified_unique_nodes
    assert {
        str(l) for l in adb_mapping.objects(subject=None, predicate=None, unique=True)
    } == {"Class", "Property", "List", "Ontology", "Person"}

    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0
    diff = outersect_graphs(rdf_graph_2, rdf_graph)
    assert len(diff) == META_GRAPH_CONTEXTUALIZE_STATEMENTS + contextualize_statements
    for _, p, _, *_ in diff:
        assert p in {RDF.type, RDFS.domain, RDFS.range}

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_2_2_PGT", get_rdf_graph("cases/2_2.ttl"))],
)
def test_pgt_case_2_2(name: str, rdf_graph: RDFGraph) -> None:
    size = len(rdf_graph)
    unique_nodes = 5
    identified_unique_nodes = 2
    non_literal_statements = 2
    contextualize_statements = 2

    # RDF to ArangoDB
    rdf_graph = adbrdf.load_meta_ontology(rdf_graph)
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name, rdf_graph, overwrite_graph=True, contextualize_graph=True
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == META_GRAPH_ALL_RESOURCES + unique_nodes
    assert (
        e_count
        == META_GRAPH_NON_LITERAL_STATEMENTS
        + non_literal_statements
        + contextualize_statements
    )

    _class = adbrdf.rdf_id_to_adb_key(str(RDFS.Class))
    _property = adbrdf.rdf_id_to_adb_key(str(RDF.Property))
    _type = adbrdf.rdf_id_to_adb_key(str(RDF.type))
    _alias = adbrdf.rdf_id_to_adb_key("http://example.com/alias")
    _mentorJoe = adbrdf.rdf_id_to_adb_key("http://example.com/mentorJoe")
    _teacher = adbrdf.rdf_id_to_adb_key("http://example.com/teacher")
    _joe = adbrdf.rdf_id_to_adb_key("http://example.com/Joe")
    _martin = adbrdf.rdf_id_to_adb_key("http://example.com/Martin")

    assert adb_graph.has_vertex_collection("Class")
    assert adb_graph.vertex_collection("Class").has(_class)
    assert adb_graph.vertex_collection("Class").has(_property)
    assert adb_graph.has_vertex_collection("Property")
    assert adb_graph.vertex_collection("Property").has(_mentorJoe)
    assert adb_graph.vertex_collection("Property").has(_type)
    assert adb_graph.vertex_collection("Property").has(_alias)

    assert adb_graph.has_vertex_collection(f"{name}_UnknownResource")
    assert adb_graph.vertex_collection(f"{name}_UnknownResource").has(_martin)
    assert adb_graph.vertex_collection(f"{name}_UnknownResource").has(_joe)
    assert adb_graph.vertex_collection(f"{name}_UnknownResource").has(_teacher)
    assert adb_graph.has_edge_collection("type")
    assert adb_graph.edge_collection("type").has(f"{_mentorJoe}-{_type}-{_property}")
    assert adb_graph.has_edge_collection("mentorJoe")
    assert adb_graph.edge_collection("mentorJoe").has(f"{_martin}-{_mentorJoe}-{_joe}")

    print("\n")

    # ArangoDB to RDF
    rdf_graph_2, adb_mapping = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

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

    assert (
        len(rdf_graph_2)
        == META_GRAPH_SIZE
        + META_GRAPH_CONTEXTUALIZE_STATEMENTS
        + size
        + contextualize_statements
    )
    assert len(adb_mapping) == META_GRAPH_IDENTIFIED_RESOURCES + identified_unique_nodes
    assert {
        str(l) for l in adb_mapping.objects(subject=None, predicate=None, unique=True)
    } == {"Class", "Property", "List", "Ontology"}

    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0
    diff = outersect_graphs(rdf_graph_2, rdf_graph)
    assert len(diff) == META_GRAPH_CONTEXTUALIZE_STATEMENTS + contextualize_statements
    for _, p, _, *_ in diff:
        assert p in {RDF.type, RDFS.domain, RDFS.range}

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_2_3_PGT", get_rdf_graph("cases/2_3.ttl"))],
)
def test_pgt_case_2_3(name: str, rdf_graph: RDFGraph) -> None:
    size = len(rdf_graph)
    unique_nodes = 5
    identified_unique_nodes = 5
    non_literal_statements = 4
    contextualize_statements = 5

    # RDF to ArangoDB
    rdf_graph = adbrdf.load_meta_ontology(rdf_graph)
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name, rdf_graph, overwrite_graph=True, contextualize_graph=True
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == META_GRAPH_ALL_RESOURCES + unique_nodes
    assert (
        e_count
        == META_GRAPH_NON_LITERAL_STATEMENTS
        + non_literal_statements
        + contextualize_statements
    )

    _class = adbrdf.rdf_id_to_adb_key(str(RDFS.Class))
    _property = adbrdf.rdf_id_to_adb_key(str(RDF.Property))
    _type = adbrdf.rdf_id_to_adb_key(str(RDF.type))
    _subPropertyOf = adbrdf.rdf_id_to_adb_key(str(RDFS.subPropertyOf))
    _person = adbrdf.rdf_id_to_adb_key("http://example.com/Person")
    _supervise = adbrdf.rdf_id_to_adb_key("http://example.com/supervise")
    _administer = adbrdf.rdf_id_to_adb_key("http://example.com/administer")
    _jan = adbrdf.rdf_id_to_adb_key("http://example.com/Jan")
    _leo = adbrdf.rdf_id_to_adb_key("http://example.com/Leo")

    assert adb_graph.has_vertex_collection("Class")
    assert adb_graph.vertex_collection("Class").has(_class)
    assert adb_graph.vertex_collection("Class").has(_property)
    assert adb_graph.vertex_collection("Class").has(_person)
    assert adb_graph.has_vertex_collection("Property")
    assert adb_graph.vertex_collection("Property").has(_supervise)
    assert adb_graph.vertex_collection("Property").has(_type)
    assert adb_graph.has_vertex_collection("Person")
    assert adb_graph.vertex_collection("Person").has(_jan)
    assert adb_graph.vertex_collection("Person").has(_leo)

    assert adb_graph.has_edge_collection("subPropertyOf")
    assert adb_graph.edge_collection("subPropertyOf").has(
        f"{_supervise}-{_subPropertyOf}-{_administer}"
    )

    print("\n")

    # ArangoDB to RDF
    rdf_graph_2, adb_mapping = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

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

    assert (
        len(rdf_graph_2)
        == META_GRAPH_SIZE
        + META_GRAPH_CONTEXTUALIZE_STATEMENTS
        + size
        + contextualize_statements
    )
    assert len(adb_mapping) == META_GRAPH_IDENTIFIED_RESOURCES + identified_unique_nodes
    assert {
        str(l) for l in adb_mapping.objects(subject=None, predicate=None, unique=True)
    } == {"Class", "Property", "List", "Ontology", "Person"}

    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0
    diff = outersect_graphs(rdf_graph_2, rdf_graph)
    assert len(diff) == META_GRAPH_CONTEXTUALIZE_STATEMENTS + contextualize_statements
    for _, p, _, *_ in outersect_graphs(rdf_graph_2, rdf_graph):
        assert p in {RDF.type, RDFS.domain, RDFS.range}

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_2_4_PGT", get_rdf_graph("cases/2_4.ttl"))],
)
def test_pgt_case_2_4(name: str, rdf_graph: RDFGraph) -> None:
    size = len(rdf_graph)
    unique_nodes = 4
    identified_unique_nodes = 2
    non_literal_statements = 2
    contextualize_statements = 1

    # RDF to ArangoDB
    rdf_graph = adbrdf.load_meta_ontology(rdf_graph)
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name, rdf_graph, overwrite_graph=True, contextualize_graph=True
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == META_GRAPH_ALL_RESOURCES + unique_nodes
    assert (
        e_count
        == META_GRAPH_NON_LITERAL_STATEMENTS
        + non_literal_statements
        + contextualize_statements
    )

    _class = adbrdf.rdf_id_to_adb_key(str(RDFS.Class))
    _property = adbrdf.rdf_id_to_adb_key(str(RDF.Property))
    _type = adbrdf.rdf_id_to_adb_key(str(RDF.type))
    _relation = adbrdf.rdf_id_to_adb_key("http://example.com/relation")
    _friend = adbrdf.rdf_id_to_adb_key("http://example.com/friend")
    _tom = adbrdf.rdf_id_to_adb_key("http://example.com/Tom")
    _chris = adbrdf.rdf_id_to_adb_key("http://example.com/Chris")

    assert adb_graph.has_edge_collection("type")
    assert adb_graph.edge_collection("type").has(f"{_friend}-{_type}-{_relation}")
    assert adb_graph.edge_collection("type").has(f"{_friend}-{_type}-{_property}")
    assert adb_graph.edge_collection("type").has(f"{_relation}-{_type}-{_class}")

    assert adb_graph.has_edge_collection("friend")
    assert adb_graph.edge_collection("friend").has(f"{_tom}-{_friend}-{_chris}")

    assert not adb_graph.has_vertex_collection("relation")
    assert adb_graph.vertex_collection("Property").has(_friend)

    print("\n")

    # ArangoDB to RDF
    rdf_graph_2, adb_mapping = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

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

    assert (
        len(rdf_graph_2)
        == META_GRAPH_SIZE
        + META_GRAPH_CONTEXTUALIZE_STATEMENTS
        + size
        + contextualize_statements
    )
    assert len(adb_mapping) == META_GRAPH_IDENTIFIED_RESOURCES + identified_unique_nodes
    assert {
        str(l) for l in adb_mapping.objects(subject=None, predicate=None, unique=True)
    } == {"Class", "Property", "List", "Ontology"}

    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0
    diff = outersect_graphs(rdf_graph_2, rdf_graph)
    assert len(diff) == META_GRAPH_CONTEXTUALIZE_STATEMENTS + contextualize_statements
    for _, p, _, *_ in outersect_graphs(rdf_graph_2, rdf_graph):
        assert p in {RDF.type, RDFS.domain, RDFS.range}

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_3_1_PGT", get_rdf_graph("cases/3_1.ttl"))],
)
def test_pgt_case_3_1(name: str, rdf_graph: RDFGraph) -> None:
    size = len(rdf_graph)
    unique_nodes = 5
    identified_unique_nodes = 4
    non_literal_statements = 0
    contextualize_statements = 4

    # RDF to ArangoDB
    rdf_graph = adbrdf.load_meta_ontology(rdf_graph)
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name, rdf_graph, overwrite_graph=True, contextualize_graph=True
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == META_GRAPH_ALL_RESOURCES + unique_nodes
    assert (
        e_count
        == META_GRAPH_NON_LITERAL_STATEMENTS
        + non_literal_statements
        + contextualize_statements
    )

    _property = adbrdf.rdf_id_to_adb_key(str(RDF.Property))
    _type = adbrdf.rdf_id_to_adb_key(str(RDF.type))
    _book = adbrdf.rdf_id_to_adb_key("http://example.com/book")
    _index = adbrdf.rdf_id_to_adb_key("http://example.com/index")
    _pages = adbrdf.rdf_id_to_adb_key("http://example.com/pages")

    assert adb_graph.has_vertex_collection(f"{name}_UnknownResource")
    assert adb_graph.vertex_collection(f"{name}_UnknownResource").has(_book)
    doc = adb_graph.vertex_collection(f"{name}_UnknownResource").get(_book)
    assert doc["index"] == "55"
    assert doc["cover"] == 20
    assert doc["pages"] == 100
    assert doc["publish_date"] == "1963-03-22"

    assert adb_graph.has_vertex_collection("Property")
    assert adb_graph.vertex_collection("Property").has(_index)
    assert adb_graph.vertex_collection("Property").has(_pages)
    assert adb_graph.has_edge_collection("type")
    assert adb_graph.edge_collection("type").has(f"{_index}-{_type}-{_property}")

    print("\n")

    # ArangoDB to RDF
    rdf_graph_2, adb_mapping = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

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

    assert (
        len(rdf_graph_2)
        == META_GRAPH_SIZE
        + META_GRAPH_CONTEXTUALIZE_STATEMENTS
        + size
        + contextualize_statements
    )
    assert len(adb_mapping) == META_GRAPH_IDENTIFIED_RESOURCES + identified_unique_nodes
    assert {
        str(l) for l in adb_mapping.objects(subject=None, predicate=None, unique=True)
    } == {"Class", "Property", "List", "Ontology"}

    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0
    diff = outersect_graphs(rdf_graph_2, rdf_graph)
    assert len(diff) == META_GRAPH_CONTEXTUALIZE_STATEMENTS + contextualize_statements
    for _, p, _, *_ in outersect_graphs(rdf_graph_2, rdf_graph):
        assert p in {RDF.type, RDFS.domain, RDFS.range}

    db.delete_graph(name, drop_collections=True)


# TODO - REVISIT
# NOTE: No current support for Literal datatype persistence in PGT Transformation
# i.e we lose the @en or @da language suffix
@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_3_2_PGT", get_rdf_graph("cases/3_2.ttl"))],
)
def test_pgt_case_3_2(name: str, rdf_graph: RDFGraph) -> None:
    unique_nodes = 1
    non_literal_statements = 0

    # RDF to ArangoDB
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name, rdf_graph, overwrite_graph=True, contextualize_graph=False
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == unique_nodes
    assert e_count == non_literal_statements

    _book = adbrdf.rdf_id_to_adb_key("http://example.com/book")

    assert adb_graph.has_vertex_collection(f"{name}_UnknownResource")
    doc = adb_graph.vertex_collection(f"{name}_UnknownResource").get(_book)
    assert "title" in doc
    assert type(doc["title"]) is list
    assert set(doc["title"]) == {"Book", "Bog"}

    print("\n")

    # ArangoDB to RDF (List Conversion Method = "collection")
    rdf_graph_2, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)(), "collection")

    book = URIRef("http://example.com/book")

    # TODO REVIST
    # title = URIRef("http://example.com/title")
    adb_graph_namepspace = f"{db._conn._url_prefixes[0]}/{name}#"
    title = URIRef(f"{adb_graph_namepspace}title")

    assert (book, title, None) in rdf_graph_2
    assert (None, RDF.first, Literal("Bog")) in rdf_graph_2
    assert (None, RDF.first, Literal("Book")) in rdf_graph_2

    assert len(rdf_graph_2) == 5

    print("\n")

    # ArangoDB to RDF (List Conversion Method = "container")
    rdf_graph_3, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)(), "container")

    assert (book, title, None) in rdf_graph_3
    assert (None, None, Literal("Bog")) in rdf_graph_3
    assert (None, None, Literal("Book")) in rdf_graph_3

    assert len(rdf_graph_3) == 3

    print("\n")

    # ArangoDB to RDF (List Conversion Method = "static")
    rdf_graph_4, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)(), "static")

    assert (book, title, Literal("Book")) in rdf_graph_4
    assert (book, title, Literal("Bog")) in rdf_graph_4

    assert len(rdf_graph_4) == 2

    # ArangoDB to RDF (List Conversion Method = "bad_name")
    with pytest.raises(ValueError):
        adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)(), "bad_name")

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_4_PGT", get_rdf_graph("cases/4.ttl"))],
)
def test_pgt_case_4(name: str, rdf_graph: RDFGraph) -> None:
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name, rdf_graph, overwrite_graph=True, contextualize_graph=False
    )

    _list1 = adbrdf.rdf_id_to_adb_key("http://example.com/List1")

    assert adb_graph.has_vertex_collection(f"{name}_UnknownResource")
    assert adb_graph.vertex_collection(f"{name}_UnknownResource").has(_list1)
    doc = adb_graph.vertex_collection(f"{name}_UnknownResource").get(_list1)

    assert "contents" in doc
    assert type(doc["contents"]) is list
    assert set(doc["contents"]) == {"one", "two", "three"}

    print("\n")

    # ArangoDB to RDF
    rdf_graph_2, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    list1 = URIRef("http://example.com/List1")
    # TODO - REVISIT
    # contents = URIRef("http://example.com/contents")
    adb_graph_namepspace = f"{db._conn._url_prefixes[0]}/{name}#"
    contents = URIRef(f"{adb_graph_namepspace}contents")

    assert (list1, contents, Literal("one")) in rdf_graph_2
    assert (list1, contents, Literal("two")) in rdf_graph_2
    assert (list1, contents, Literal("three")) in rdf_graph_2

    assert len(rdf_graph_2) == 3

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_5_PGT", get_rdf_graph("cases/5.ttl"))],
)
def test_pgt_case_5(name: str, rdf_graph: RDFGraph) -> None:
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name, rdf_graph, overwrite_graph=True, contextualize_graph=False
    )

    assert adb_graph.edge_collection("nationality").count() == 1
    assert adb_graph.vertex_collection(f"{name}_UnknownResource").count() == 2
    for doc in adb_graph.vertex_collection(f"{name}_UnknownResource"):
        if doc["_rdftype"] == "URIRef":
            assert doc["_label"] == "bob"
        elif doc["_rdftype"] == "BNode":
            assert doc["country"] == "Canada"
        else:
            assert False  # Should not be here

    print("\n")

    # ArangoDB to RDF
    rdf_graph_2, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    adb_graph_namepspace = f"{db._conn._url_prefixes[0]}/{name}#"
    bob = URIRef("http://example.com/bob")
    nationality = URIRef("http://example.com/nationality")
    country = URIRef(f"{adb_graph_namepspace}country")

    # Original Statement assertions
    assert (bob, nationality, None) in rdf_graph_2
    bnode = rdf_graph_2.value(bob, nationality)
    assert (bnode, country, Literal("Canada")) in rdf_graph_2
    assert len(rdf_graph_2) == 2

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_6_PGT", get_rdf_graph("cases/6.trig"))],
)
def test_pgt_case_6(name: str, rdf_graph: RDFGraph) -> None:
    size = len(rdf_graph)
    unique_nodes = 13
    identified_unique_nodes = 12
    non_literal_statements = 10
    contextualize_statements = 14

    rdf_graph = adbrdf.load_meta_ontology(rdf_graph)
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name, rdf_graph, overwrite_graph=True, contextualize_graph=True
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == META_GRAPH_ALL_RESOURCES + unique_nodes
    assert (
        e_count
        == META_GRAPH_NON_LITERAL_STATEMENTS
        + non_literal_statements
        + contextualize_statements
    )

    _type = adbrdf.rdf_id_to_adb_key(str(RDF.type))
    _domain = adbrdf.rdf_id_to_adb_key(str(RDFS.domain))
    _subClassOf = adbrdf.rdf_id_to_adb_key(str(RDFS.subClassOf))
    _monica = adbrdf.rdf_id_to_adb_key("http://example.com/Monica")
    _person = adbrdf.rdf_id_to_adb_key("http://example.com/Person")
    _monica = adbrdf.rdf_id_to_adb_key("http://example.com/Monica")
    _hasSkill = adbrdf.rdf_id_to_adb_key("http://example.com/hasSkill")
    _management = adbrdf.rdf_id_to_adb_key("http://example.com/Management")
    _entity = adbrdf.rdf_id_to_adb_key("http://example.com/Entity")
    _homepage = adbrdf.rdf_id_to_adb_key("http://example.com/homepage")
    _employer = adbrdf.rdf_id_to_adb_key("http://example.com/employer")
    _name = adbrdf.rdf_id_to_adb_key("http://example.com/name")

    assert adb_graph.has_vertex_collection("Person")
    doc = adb_graph.vertex_collection("Person").get(_monica)
    assert doc["name"] == "Monica"

    assert adb_graph.vertex_collection("Skill").count() == 2
    assert adb_graph.vertex_collection("Website").count() == 1

    edge = adb_graph.edge_collection("hasSkill").get(
        f"{_monica}-{_hasSkill}-{_management}"
    )
    assert edge["_sub_graph_uri"] == "http://example.com/Graph1"

    edge = adb_graph.edge_collection("type").get(f"{_monica}-{_type}-{_person}")
    assert edge["_sub_graph_uri"] == "http://example.com/Graph2"

    edge = adb_graph.edge_collection("type").get(f"{_monica}-{_type}-{_entity}")
    assert edge["_sub_graph_uri"] == "http://example.com/Graph1"

    assert adb_graph.edge_collection("subClassOf").has(
        f"{_person}-{_subClassOf}-{_entity}"
    )

    for _from in [_hasSkill, _homepage, _name, _employer]:
        assert adb_graph.edge_collection("domain").has(f"{_from}-{_domain}-{_entity}")
        assert not adb_graph.edge_collection("domain").has(
            f"{_from}-{_domain}-{_person}"
        )

    print("\n")

    # ArangoDB to RDF
    rdf_graph_2, adb_mapping = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

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
    # NOTE: We lose the Sub Graph URI here...
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

    assert (
        len(rdf_graph_2)
        == META_GRAPH_SIZE
        + META_GRAPH_CONTEXTUALIZE_STATEMENTS
        + size
        + contextualize_statements
    )
    assert len(adb_mapping) == META_GRAPH_IDENTIFIED_RESOURCES + identified_unique_nodes
    assert {
        str(l) for l in adb_mapping.objects(subject=None, predicate=None, unique=True)
    } == {"Skill", "Person", "Website", "List", "Ontology", "Property", "Class"}

    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0
    diff = outersect_graphs(rdf_graph_2, rdf_graph)
    assert len(diff) == META_GRAPH_CONTEXTUALIZE_STATEMENTS + contextualize_statements
    for _, p, _, *_ in outersect_graphs(rdf_graph_2, rdf_graph):
        assert p in {RDF.type, RDFS.domain, RDFS.range}

    db.delete_graph(name, drop_collections=True)


# NOTE: Official assertions are TBD, given Case 7 dispute
@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_7_PGT", get_rdf_graph("cases/7.ttl"))],
)
def test_pgt_case_7(name: str, rdf_graph: RDFGraph) -> None:
    size = len(rdf_graph)
    unique_nodes = 17
    identified_unique_nodes = 17
    non_literal_statements = size - 1
    contextualize_statements = 13
    adb_col_uri_statements = 1

    rdf_graph = adbrdf.load_meta_ontology(rdf_graph)
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name, rdf_graph, overwrite_graph=True, contextualize_graph=True
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == META_GRAPH_ALL_RESOURCES + unique_nodes
    assert (
        e_count
        == META_GRAPH_NON_LITERAL_STATEMENTS
        + non_literal_statements
        + contextualize_statements
    )

    _type = adbrdf.rdf_id_to_adb_key(str(RDF.type))
    _alice = adbrdf.rdf_id_to_adb_key("http://example.com/alice")
    _author = adbrdf.rdf_id_to_adb_key("http://example.com/Author")
    _arson = adbrdf.rdf_id_to_adb_key("http://example.com/Arson")
    _charlie = adbrdf.rdf_id_to_adb_key("http://example.com/charlie")
    _marty = adbrdf.rdf_id_to_adb_key("http://example.com/marty")
    _livingthing = adbrdf.rdf_id_to_adb_key("http://example.com/LivingThing")
    _animal = adbrdf.rdf_id_to_adb_key("http://example.com/Animal")
    _zenkey = adbrdf.rdf_id_to_adb_key("http://example.com/Zenkey")
    _human = adbrdf.rdf_id_to_adb_key("http://example.com/Human")
    _john = adbrdf.rdf_id_to_adb_key("http://example.com/john")
    _singer = adbrdf.rdf_id_to_adb_key("http://example.com/Singer")
    _writer = adbrdf.rdf_id_to_adb_key("http://example.com/Writer")
    _artist = adbrdf.rdf_id_to_adb_key("http://example.com/Artist")
    _guitarist = adbrdf.rdf_id_to_adb_key("http://example.com/Guitarist")

    assert adb_graph.has_vertex_collection("Arson")
    assert not adb_graph.has_vertex_collection("Author")
    assert adb_graph.edge_collection("type").get(f"{_alice}-{_type}-{_author}")
    assert adb_graph.edge_collection("type").get(f"{_alice}-{_type}-{_arson}")

    assert adb_graph.has_vertex_collection("Zenkey")
    assert adb_graph.has_vertex_collection("Human")
    assert not adb_graph.has_vertex_collection("Animal")
    assert not adb_graph.has_vertex_collection("LivingThing")
    assert adb_graph.edge_collection("type").get(f"{_charlie}-{_type}-{_livingthing}")
    assert adb_graph.edge_collection("type").get(f"{_charlie}-{_type}-{_animal}")
    assert adb_graph.edge_collection("type").get(f"{_charlie}-{_type}-{_zenkey}")
    assert adb_graph.edge_collection("type").get(f"{_marty}-{_type}-{_livingthing}")
    assert adb_graph.edge_collection("type").get(f"{_marty}-{_type}-{_animal}")
    assert adb_graph.edge_collection("type").get(f"{_marty}-{_type}-{_human}")

    assert adb_graph.has_vertex_collection("Artist")
    assert not adb_graph.has_vertex_collection("Singer")
    assert not adb_graph.has_vertex_collection("Writer")
    assert not adb_graph.has_vertex_collection("Guitarist")
    assert adb_graph.edge_collection("type").get(f"{_john}-{_type}-{_singer}")
    assert adb_graph.edge_collection("type").get(f"{_john}-{_type}-{_writer}")
    assert adb_graph.edge_collection("type").get(f"{_john}-{_type}-{_guitarist}")
    assert not adb_graph.edge_collection("type").has(f"{_john}-{_type}-{_artist}")

    # ArangoDB to RDF
    rdf_graph_2, adb_mapping = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    alice = URIRef("http://example.com/alice")
    arson = URIRef("http://example.com/Arson")
    author = URIRef("http://example.com/Author")

    object = URIRef("http://example.com/Object")
    thing = URIRef("http://example.com/Thing")
    living_thing = URIRef("http://example.com/LivingThing")
    animal = URIRef("http://example.com/Animal")
    human = URIRef("http://example.com/Human")
    donkey = URIRef("http://example.com/Donkey")
    zebra = URIRef("http://example.com/Zebra")
    zenkey = URIRef("http://example.com/Zenkey")
    charlie = URIRef("http://example.com/charlie")
    marty = URIRef("http://example.com/marty")

    singer = URIRef("http://example.com/Singer")
    writer = URIRef("http://example.com/Writer")
    guitarist = URIRef("http://example.com/Guitarist")
    john = URIRef("http://example.com/john")

    # Original Statement assertions
    assert (alice, RDF.type, arson) in rdf_graph_2
    assert (alice, RDF.type, author) in rdf_graph_2

    assert (thing, RDFS.subClassOf, object) in rdf_graph_2
    assert (living_thing, RDFS.subClassOf, thing) in rdf_graph_2
    assert (animal, RDFS.subClassOf, living_thing) in rdf_graph_2
    assert (human, RDFS.subClassOf, animal) in rdf_graph_2
    assert (donkey, RDFS.subClassOf, animal) in rdf_graph_2
    assert (zebra, RDFS.subClassOf, animal) in rdf_graph_2
    assert (zenkey, RDFS.subClassOf, donkey) in rdf_graph_2
    assert (zenkey, RDFS.subClassOf, zebra) in rdf_graph_2
    assert (charlie, RDF.type, living_thing) in rdf_graph_2
    assert (charlie, RDF.type, animal) in rdf_graph_2
    assert (charlie, RDF.type, zenkey) in rdf_graph_2
    assert (marty, RDF.type, living_thing) in rdf_graph_2
    assert (marty, RDF.type, animal) in rdf_graph_2
    assert (marty, RDF.type, human) in rdf_graph_2

    assert (john, RDF.type, singer) in rdf_graph_2
    assert (john, RDF.type, writer) in rdf_graph_2
    assert (john, RDF.type, guitarist) in rdf_graph_2

    assert (
        len(rdf_graph_2)
        == META_GRAPH_SIZE
        + META_GRAPH_CONTEXTUALIZE_STATEMENTS
        + size
        + contextualize_statements
        - adb_col_uri_statements
    )
    assert len(adb_mapping) == META_GRAPH_IDENTIFIED_RESOURCES + identified_unique_nodes
    assert {
        str(l) for l in adb_mapping.objects(subject=None, predicate=None, unique=True)
    } == {"Zenkey", "Arson", "Class", "Ontology", "Artist", "Property", "List", "Human"}

    diff_1 = outersect_graphs(rdf_graph, rdf_graph_2)
    assert len(diff_1) == 1
    assert (john, adbrdf.adb_col_uri, Literal("Artist")) in diff_1

    diff_2 = outersect_graphs(rdf_graph_2, rdf_graph)
    assert len(diff_2) == META_GRAPH_CONTEXTUALIZE_STATEMENTS + contextualize_statements
    for _, p, _, *_ in outersect_graphs(rdf_graph_2, rdf_graph):
        assert p in {RDF.type, RDFS.domain, RDFS.range}

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Collection_PGT", get_rdf_graph("collection.ttl"))],
)
def test_pgt_collection(name: str, rdf_graph: RDFGraph) -> None:
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name, rdf_graph, overwrite_graph=True, contextualize_graph=False
    )

    _doc = adbrdf.rdf_id_to_adb_key("http://example.com/Doc")
    _random = adbrdf.rdf_id_to_adb_key("http://example.com/random")
    _mars = adbrdf.rdf_id_to_adb_key("http://example.com/Mars")

    doc = adb_graph.vertex_collection("TestDoc").get(_doc)
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
    assert adb_graph.edge_collection("random").get(f"{_doc}-{_random}-{_mars}")

    print("\n")

    # ArangoDB to RDF
    rdf_graph_2, adb_mapping = adbrdf.arangodb_graph_to_rdf(
        name, type(rdf_graph)(), "collection"
    )

    assert len(rdf_graph_2) == 123
    assert len(adb_mapping) == 7
    doc = URIRef("http://example.com/Doc")
    planets = URIRef("http://example.com/planets")

    # TODO - REVISIT
    # numbers = URIRef("http://example.com/numbers")
    # random = URIRef("http://example.com/random")
    # nested_container = URIRef("http://example.com/nested_container")
    adb_graph_namepspace = f"{db._conn._url_prefixes[0]}/{name}#"
    numbers = URIRef(f"{adb_graph_namepspace}numbers")
    random = URIRef(f"{adb_graph_namepspace}random")
    nested_container = URIRef(f"{adb_graph_namepspace}nested_container")

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
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name, rdf_graph, overwrite_graph=True, contextualize_graph=False
    )

    _doc = adbrdf.rdf_id_to_adb_key("http://example.com/Doc")

    doc = adb_graph.vertex_collection("TestDoc").get(_doc)
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
    rdf_graph_2, adb_mapping = adbrdf.arangodb_graph_to_rdf(
        name, type(rdf_graph)(), list_conversion_mode="container"
    )

    assert len(rdf_graph_2) == 42
    assert len(adb_mapping) == 7
    doc = URIRef("http://example.com/Doc")
    planets = URIRef("http://example.com/planets")

    # TODO - REVISIT
    adb_graph_namepspace = f"{db._conn._url_prefixes[0]}/{name}#"
    numbers = URIRef(f"{adb_graph_namepspace}numbers")

    # TODO - REVISIT
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

    rdf_graph, adb_mapping = adbrdf.arangodb_graph_to_rdf(
        name, RDFGraph(), "collection"
    )

    adb_graph_namespace = f"{db._conn._url_prefixes[0]}/{name}#"
    test_doc = URIRef(f"{adb_graph_namespace}1")

    assert len(rdf_graph) == 14
    assert len(adb_mapping) == 1
    assert (test_doc, URIRef(f"{adb_graph_namespace}val"), None) in rdf_graph
    assert (None, URIRef(f"{adb_graph_namespace}sub_val_1"), Literal(1)) in rdf_graph
    assert (None, URIRef(f"{adb_graph_namespace}sub_val_2"), None) in rdf_graph
    assert (None, URIRef(f"{adb_graph_namespace}sub_val_3"), Literal(3)) in rdf_graph
    assert (None, URIRef(f"{adb_graph_namespace}sub_val_4"), None) in rdf_graph
    assert (None, RDF.first, Literal(4)) in rdf_graph
    assert (None, URIRef(f"{adb_graph_namespace}sub_val_5"), None) in rdf_graph
    assert (None, URIRef(f"{adb_graph_namespace}sub_val_6"), Literal(6)) in rdf_graph
    assert (None, URIRef(f"{adb_graph_namespace}sub_val_7"), Literal(7)) in rdf_graph

    # TODO: Should this bring back the original dict structure?
    # Need to discuss...
    # adb_graph = adbrdf.rdf_to_arangodb_by_pgt(f"{name}2", rdf_graph)
    # db.delete_graph(f"{name}2", drop_collections=True)

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize("name", [("fraud-detection"), ("imdb")])
def test_adb_native_graph_to_rdf(name: str) -> None:
    adb_graph = db.graph(name)
    rdf_graph, _ = adbrdf.arangodb_graph_to_rdf(name, RDFGraph())

    adb_graph_namespace = f"{db._conn._url_prefixes[0]}/{name}#"

    for v_col in adb_graph.vertex_collections():
        for doc in db.collection(v_col):
            term = URIRef(f"{adb_graph_namespace}{doc['_key']}")
            for k, v in doc.items():
                if k not in ["_key", "_id", "_rev"]:
                    property = URIRef(f"{adb_graph_namespace}{k}")
                    assert (term, property, Literal(v)) in rdf_graph

    for e_d in adb_graph.edge_definitions():
        e_col = e_d["edge_collection"]
        e_col_uri = URIRef(f"{adb_graph_namespace}{e_col}")

        for edge in db.collection(e_col):
            term = URIRef(f"{adb_graph_namespace}{edge['_key']}")
            subject = URIRef(f"{adb_graph_namespace}{edge['_from'].split('/')[-1]}")
            object = URIRef(f"{adb_graph_namespace}{edge['_to'].split('/')[-1]}")

            assert (subject, e_col_uri, object) in rdf_graph

            for k, v in edge.items():
                if k not in ["_key", "_id", "_rev", "_from", "_to"]:
                    property = URIRef(f"{adb_graph_namespace}{k}")
                    assert (term, property, Literal(v)) in rdf_graph
