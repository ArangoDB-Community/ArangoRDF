import os
from typing import Dict

import pytest
from farmhash import Fingerprint64 as FP64
from rdflib import RDF, RDFS, BNode
from rdflib import ConjunctiveGraph as RDFConjunctiveGraph
from rdflib import Graph as RDFGraph
from rdflib import Literal, URIRef

from arango_rdf import ArangoRDF

from .conftest import (
    META_GRAPH_ALL_RESOURCES,
    META_GRAPH_CONTEXTS,
    META_GRAPH_CONTEXTUALIZE_STATEMENTS,
    META_GRAPH_IDENTIFIED_RESOURCES,
    META_GRAPH_LITERAL_STATEMENTS,
    META_GRAPH_NON_LITERAL_STATEMENTS,
    META_GRAPH_SIZE,
    META_GRAPH_UNKNOWN_RESOURCES,
    PROJECT_DIR,
    adbrdf,
    db,
    get_adb_graph_count,
    get_meta_graph,
    get_rdf_graph,
    outersect_graphs,
)


def test_constructor() -> None:
    bad_db = None

    with pytest.raises(TypeError):
        ArangoRDF(bad_db)

    bad_controller = None
    with pytest.raises(TypeError):
        ArangoRDF(db, bad_controller)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_1_RPT", get_rdf_graph("cases/1.ttl"))],
)
def test_rpt_case_1_0(name: str, rdf_graph: RDFGraph) -> None:
    num_triples = 3
    num_urirefs = 3
    num_bnodes = 0
    num_literals = 0

    _class = adbrdf.rdf_id_to_adb_key(str(RDFS.Class))
    _property = adbrdf.rdf_id_to_adb_key(str(RDF.Property))
    _type = adbrdf.rdf_id_to_adb_key(str(RDF.type))
    _domain = adbrdf.rdf_id_to_adb_key(str(RDFS.domain))
    _range = adbrdf.rdf_id_to_adb_key(str(RDFS.range))

    _person = adbrdf.rdf_id_to_adb_key("http://example.com/Person")
    _meets = adbrdf.rdf_id_to_adb_key("http://example.com/meets")
    _alice = adbrdf.rdf_id_to_adb_key("http://example.com/alice")
    _bob = adbrdf.rdf_id_to_adb_key("http://example.com/bob")

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph,
        contextualize_graph=False,
        overwrite_graph=True,
        use_async=False,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == num_urirefs + num_bnodes + num_literals
    assert e_count == num_triples

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_person)
    assert URIREF_COL.has(_alice)
    assert URIREF_COL.has(_bob)

    STATEMENT_COL = adb_graph.vertex_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(str(FP64(f"{_alice}-{_type}-{_person}")))
    assert STATEMENT_COL.has(str(FP64(f"{_bob}-{_type}-{_person}")))
    assert STATEMENT_COL.has(str(FP64(f"{_alice}-{_meets}-{_bob}")))

    rdf_graph_2, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    assert len(rdf_graph_2) == len(rdf_graph)
    assert num_urirefs + num_bnodes + num_literals == len(rdf_graph_2.all_nodes())
    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(outersect_graphs(rdf_graph_2, rdf_graph)) == 0

    db.delete_graph(name, drop_collections=True)

    num_triples = 14
    num_urirefs = 11
    num_bnodes = 0
    num_literals = 0

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph,
        contextualize_graph=True,
        overwrite_graph=True,
        use_async=False,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == num_urirefs + num_bnodes + num_literals
    assert e_count == num_triples

    STATEMENT_COL = adb_graph.vertex_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(str(FP64(f"{_person}-{_type}-{_class}")))
    assert STATEMENT_COL.has(str(FP64(f"{_meets}-{_type}-{_property}")))
    assert STATEMENT_COL.has(str(FP64(f"{_meets}-{_domain}-{_person}")))
    assert STATEMENT_COL.has(str(FP64(f"{_meets}-{_range}-{_person}")))

    rdf_graph_3, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    assert len(rdf_graph_3) >= len(rdf_graph)
    assert num_urirefs + num_bnodes + num_literals == len(rdf_graph_3.all_nodes())
    assert len(outersect_graphs(rdf_graph, rdf_graph_3)) == 0

    diff = outersect_graphs(rdf_graph_3, rdf_graph)
    predicates = {p for p in diff.predicates(unique=True)}
    assert predicates <= {RDF.type, RDFS.domain, RDFS.range}

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_2_1_RPT", get_rdf_graph("cases/2_1.ttl"))],
)
def test_rpt_case_2_1(name: str, rdf_graph: RDFGraph) -> None:
    num_triples = 5
    num_urirefs = 4
    num_bnodes = 0
    num_literals = 2

    _type = adbrdf.rdf_id_to_adb_key(str(RDF.type))
    _label = adbrdf.rdf_id_to_adb_key(str(RDFS.label))
    _person = adbrdf.rdf_id_to_adb_key("http://example.com/Person")
    _name = adbrdf.rdf_id_to_adb_key("http://example.com/name")
    _mentor = adbrdf.rdf_id_to_adb_key("http://example.com/mentor")
    _sam = adbrdf.rdf_id_to_adb_key("http://example.com/Sam")
    _lee = adbrdf.rdf_id_to_adb_key("http://example.com/Lee")
    _project_supervisor = adbrdf.rdf_id_to_adb_key("project supervisor")
    _mentors_name = adbrdf.rdf_id_to_adb_key("mentor's name")

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph,
        contextualize_graph=False,
        overwrite_graph=True,
        use_async=False,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == num_urirefs + num_bnodes + num_literals
    assert e_count == num_triples

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_person)
    assert URIREF_COL.has(_sam)
    assert URIREF_COL.has(_lee)
    assert URIREF_COL.has(_mentor)

    LITERAL_COL = adb_graph.vertex_collection(f"{name}_Literal")
    assert LITERAL_COL.has(_project_supervisor)
    assert LITERAL_COL.has(_mentors_name)

    STATEMENT_COL = adb_graph.vertex_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(str(FP64(f"{_sam}-{_mentor}-{_lee}")))
    assert STATEMENT_COL.has(str(FP64(f"{_mentor}-{_label}-{_project_supervisor}")))
    assert STATEMENT_COL.has(str(FP64(f"{_mentor}-{_name}-{_mentors_name}")))
    assert STATEMENT_COL.has(str(FP64(f"{_sam}-{_type}-{_person}")))
    assert STATEMENT_COL.has(str(FP64(f"{_lee}-{_type}-{_person}")))

    rdf_graph_2, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    assert len(rdf_graph_2) == len(rdf_graph)
    assert num_urirefs + num_bnodes + num_literals == len(rdf_graph_2.all_nodes())
    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(outersect_graphs(rdf_graph_2, rdf_graph)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_2_2_RPT", get_rdf_graph("cases/2_2.ttl"))],
)
def test_rpt_case_2_2(name: str, rdf_graph: RDFGraph) -> None:
    num_triples = 2
    num_urirefs = 4
    num_bnodes = 0
    num_literals = 0

    _martin = adbrdf.rdf_id_to_adb_key("http://example.com/Martin")
    _mentorJoe = adbrdf.rdf_id_to_adb_key("http://example.com/mentorJoe")
    _joe = adbrdf.rdf_id_to_adb_key("http://example.com/Joe")
    _alias = adbrdf.rdf_id_to_adb_key("http://example.com/alias")
    _teacher = adbrdf.rdf_id_to_adb_key("http://example.com/teacher")

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph,
        contextualize_graph=False,
        overwrite_graph=True,
        use_async=False,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == num_urirefs + num_bnodes + num_literals
    assert e_count == num_triples

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_martin)
    assert URIREF_COL.has(_joe)
    assert URIREF_COL.has(_mentorJoe)
    assert URIREF_COL.has(_teacher)

    STATEMENT_COL = adb_graph.vertex_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(str(FP64(f"{_martin}-{_mentorJoe}-{_joe}")))
    assert STATEMENT_COL.has(str(FP64(f"{_mentorJoe}-{_alias}-{_teacher}")))

    rdf_graph_2, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    assert len(rdf_graph_2) == len(rdf_graph)
    assert num_urirefs + num_bnodes + num_literals == len(rdf_graph_2.all_nodes())
    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(outersect_graphs(rdf_graph_2, rdf_graph)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_2_3_RPT", get_rdf_graph("cases/2_3.ttl"))],
)
def test_rpt_case_2_3(name: str, rdf_graph: RDFGraph) -> None:
    num_triples = 4
    num_urirefs = 5
    num_bnodes = 0
    num_literals = 0

    _type = adbrdf.rdf_id_to_adb_key(str(RDF.type))
    _subPropertyOf = adbrdf.rdf_id_to_adb_key(str(RDFS.subPropertyOf))
    _person = adbrdf.rdf_id_to_adb_key("http://example.com/Person")
    _supervise = adbrdf.rdf_id_to_adb_key("http://example.com/supervise")
    _administer = adbrdf.rdf_id_to_adb_key("http://example.com/administer")
    _jan = adbrdf.rdf_id_to_adb_key("http://example.com/Jan")
    _leo = adbrdf.rdf_id_to_adb_key("http://example.com/Leo")

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph,
        contextualize_graph=False,
        overwrite_graph=True,
        use_async=False,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == num_urirefs + num_bnodes + num_literals
    assert e_count == num_triples

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_person)
    assert URIREF_COL.has(_jan)
    assert URIREF_COL.has(_leo)
    assert URIREF_COL.has(_administer)

    STATEMENT_COL = adb_graph.vertex_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(str(FP64(f"{_jan}-{_type}-{_person}")))
    assert STATEMENT_COL.has(str(FP64(f"{_leo}-{_type}-{_person}")))
    assert STATEMENT_COL.has(str(FP64(f"{_jan}-{_supervise}-{_leo}")))
    assert STATEMENT_COL.has(str(FP64(f"{_supervise}-{_subPropertyOf}-{_administer}")))

    rdf_graph_2, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    assert len(rdf_graph_2) == len(rdf_graph)
    assert num_urirefs + num_bnodes + num_literals == len(rdf_graph_2.all_nodes())
    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(outersect_graphs(rdf_graph_2, rdf_graph)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_2_4_RPT", get_rdf_graph("cases/2_4.ttl"))],
)
def test_rpt_case_2_4(name: str, rdf_graph: RDFGraph) -> None:
    num_triples = 2
    num_urirefs = 4
    num_bnodes = 0
    num_literals = 0

    _type = adbrdf.rdf_id_to_adb_key(str(RDF.type))
    _relation = adbrdf.rdf_id_to_adb_key("http://example.com/relation")
    _friend = adbrdf.rdf_id_to_adb_key("http://example.com/friend")
    _tom = adbrdf.rdf_id_to_adb_key("http://example.com/Tom")
    _chris = adbrdf.rdf_id_to_adb_key("http://example.com/Chris")

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph,
        contextualize_graph=False,
        overwrite_graph=True,
        use_async=False,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == num_urirefs + num_bnodes + num_literals
    assert e_count == num_triples

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_tom)
    assert URIREF_COL.has(_chris)
    assert URIREF_COL.has(_friend)
    assert URIREF_COL.has(_relation)

    STATEMENT_COL = adb_graph.vertex_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(str(FP64(f"{_tom}-{_friend}-{_chris}")))
    assert STATEMENT_COL.has(str(FP64(f"{_friend}-{_type}-{_relation}")))

    rdf_graph_2, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    assert len(rdf_graph_2) == len(rdf_graph)
    assert num_urirefs + num_bnodes + num_literals == len(rdf_graph_2.all_nodes())
    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(outersect_graphs(rdf_graph_2, rdf_graph)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_3_1_RPT", get_rdf_graph("cases/3_1.ttl"))],
)
def test_rpt_case_3_1(name: str, rdf_graph: RDFGraph) -> None:
    num_triples = 4
    num_urirefs = 1
    num_bnodes = 0
    num_literals = 4

    _xsd_integer = "http://www.w3.org/2001/XMLSchema#integer"
    _book = adbrdf.rdf_id_to_adb_key("http://example.com/book")
    _index = adbrdf.rdf_id_to_adb_key("http://example.com/index")
    _pages = adbrdf.rdf_id_to_adb_key("http://example.com/pages")
    _cover = adbrdf.rdf_id_to_adb_key("http://example.com/cover")
    _publish_date = adbrdf.rdf_id_to_adb_key("http://example.com/publish_date")
    _date = adbrdf.rdf_id_to_adb_key("1963-03-22")
    _100 = adbrdf.rdf_id_to_adb_key("100")
    _20 = adbrdf.rdf_id_to_adb_key("20")
    _55 = adbrdf.rdf_id_to_adb_key("55")

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph,
        contextualize_graph=False,
        overwrite_graph=True,
        use_async=False,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == num_urirefs + num_bnodes + num_literals
    assert e_count == num_triples

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_book)

    LITERAL_COL = adb_graph.vertex_collection(f"{name}_Literal")
    assert LITERAL_COL.has(_date)
    assert LITERAL_COL.has(_100)
    assert LITERAL_COL.get(_100)["_datatype"] == _xsd_integer
    assert LITERAL_COL.has(_20)
    assert LITERAL_COL.get(_20)["_datatype"] == _xsd_integer
    assert LITERAL_COL.has(_55)

    STATEMENT_COL = adb_graph.vertex_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(str(FP64(f"{_book}-{_publish_date}-{_date}")))
    assert STATEMENT_COL.has(str(FP64(f"{_book}-{_pages}-{_100}")))
    assert STATEMENT_COL.has(str(FP64(f"{_book}-{_cover}-{_20}")))
    assert STATEMENT_COL.has(str(FP64(f"{_book}-{_index}-{_55}")))

    rdf_graph_2, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    assert len(rdf_graph_2) == len(rdf_graph)
    assert num_urirefs + num_bnodes + num_literals == len(rdf_graph_2.all_nodes())
    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(outersect_graphs(rdf_graph_2, rdf_graph)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_3_2_RPT", get_rdf_graph("cases/3_2.ttl"))],
)
def test_rpt_case_3_2(name: str, rdf_graph: RDFGraph) -> None:
    num_triples = 2
    num_urirefs = 1
    num_bnodes = 0
    num_literals = 2

    _book = adbrdf.rdf_id_to_adb_key("http://example.com/book")
    _title = adbrdf.rdf_id_to_adb_key("http://example.com/title")
    _book_en = adbrdf.rdf_id_to_adb_key("Book")
    _book_da = adbrdf.rdf_id_to_adb_key("Bog")

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph,
        contextualize_graph=False,
        overwrite_graph=True,
        use_async=False,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == num_urirefs + num_bnodes + num_literals
    assert e_count == num_triples

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_book)

    LITERAL_COL = adb_graph.vertex_collection(f"{name}_Literal")
    assert LITERAL_COL.has(_book_en)
    assert LITERAL_COL.get(_book_en)["_lang"] == "en"
    assert LITERAL_COL.has(_book_da)
    assert LITERAL_COL.get(_book_da)["_lang"] == "da"

    STATEMENT_COL = adb_graph.vertex_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(str(FP64(f"{_book}-{_title}-{_book_en}")))
    assert STATEMENT_COL.has(str(FP64(f"{_book}-{_title}-{_book_da}")))

    rdf_graph_2, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    assert len(rdf_graph_2) == len(rdf_graph)
    assert num_urirefs + num_bnodes + num_literals == len(rdf_graph_2.all_nodes())
    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(outersect_graphs(rdf_graph_2, rdf_graph)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_4_RPT", get_rdf_graph("cases/4.ttl"))],
)
def test_rpt_case_4(name: str, rdf_graph: RDFGraph) -> None:
    num_triples = 7
    num_urirefs = 2
    num_bnodes = 3
    num_literals = 3

    list1 = URIRef("http://example.com/List1")
    contents = URIRef("http://example.com/contents")

    _list1 = adbrdf.rdf_id_to_adb_key("http://example.com/List1")
    _contents = adbrdf.rdf_id_to_adb_key("http://example.com/contents")
    _one = adbrdf.rdf_id_to_adb_key("one")
    _two = adbrdf.rdf_id_to_adb_key("two")
    _three = adbrdf.rdf_id_to_adb_key("three")
    _first = adbrdf.rdf_id_to_adb_key(str(RDF.first))
    _rest = adbrdf.rdf_id_to_adb_key(str(RDF.rest))
    _nil = adbrdf.rdf_id_to_adb_key(str(RDF.nil))

    bnode_1 = rdf_graph.value(list1, contents)
    _bnode_1 = adbrdf.rdf_id_to_adb_key(str(bnode_1))
    bnode_2 = rdf_graph.value(bnode_1, RDF.rest)
    _bnode_2 = adbrdf.rdf_id_to_adb_key(str(bnode_2))
    bnode_3 = rdf_graph.value(bnode_2, RDF.rest)
    _bnode_3 = adbrdf.rdf_id_to_adb_key(str(bnode_3))

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph,
        contextualize_graph=False,
        overwrite_graph=True,
        use_async=False,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == num_urirefs + num_bnodes + num_literals
    assert e_count == num_triples

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_list1)

    LITERAL_COL = adb_graph.vertex_collection(f"{name}_Literal")
    assert LITERAL_COL.has(_one)
    assert LITERAL_COL.has(_two)
    assert LITERAL_COL.has(_three)

    STATEMENT_COL = adb_graph.vertex_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(str(FP64(f"{_list1}-{_contents}-{_bnode_1}")))
    assert STATEMENT_COL.has(str(FP64(f"{_bnode_1}-{_first}-{_one}")))
    assert STATEMENT_COL.has(str(FP64(f"{_bnode_1}-{_rest}-{_bnode_2}")))
    assert STATEMENT_COL.has(str(FP64(f"{_bnode_2}-{_first}-{_two}")))
    assert STATEMENT_COL.has(str(FP64(f"{_bnode_2}-{_rest}-{_bnode_3}")))
    assert STATEMENT_COL.has(str(FP64(f"{_bnode_3}-{_first}-{_three}")))
    assert STATEMENT_COL.has(str(FP64(f"{_bnode_3}-{_rest}-{_nil}")))

    rdf_graph_2, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    assert len(rdf_graph_2) == len(rdf_graph)
    assert num_urirefs + num_bnodes + num_literals == len(rdf_graph_2.all_nodes())

    bnode_4 = rdf_graph_2.value(list1, contents)
    bnode_5 = rdf_graph_2.value(bnode_4, RDF.rest)
    bnode_6 = rdf_graph_2.value(bnode_5, RDF.rest)
    assert (list1, contents, bnode_4) in rdf_graph_2
    assert (bnode_4, RDF.first, Literal("one")) in rdf_graph_2
    assert (bnode_5, RDF.first, Literal("two")) in rdf_graph_2
    assert (bnode_6, RDF.first, Literal("three")) in rdf_graph_2

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_5_RPT", get_rdf_graph("cases/5.ttl"))],
)
def test_rpt_case_5(name: str, rdf_graph: RDFGraph) -> None:
    num_triples = 2
    num_urirefs = 1
    num_bnodes = 1
    num_literals = 1

    bob = URIRef("http://example.com/bob")
    nationality = URIRef("http://example.com/nationality")
    country = URIRef("http://example.com/country")

    _bob = adbrdf.rdf_id_to_adb_key(str(bob))
    _country = adbrdf.rdf_id_to_adb_key("http://example.com/country")
    _nationality = adbrdf.rdf_id_to_adb_key(str(nationality))
    _canada = adbrdf.rdf_id_to_adb_key("Canada")
    _bnode = adbrdf.rdf_id_to_adb_key(str(rdf_graph.value(bob, nationality)))

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph,
        contextualize_graph=False,
        overwrite_graph=True,
        use_async=False,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == num_urirefs + num_bnodes + num_literals
    assert e_count == num_triples

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_bob)

    LITERAL_COL = adb_graph.vertex_collection(f"{name}_Literal")
    assert LITERAL_COL.has(_canada)

    STATEMENT_COL = adb_graph.vertex_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(str(FP64(f"{_bob}-{_nationality}-{_bnode}")))
    assert STATEMENT_COL.has(str(FP64(f"{_bnode}-{_country}-{_canada}")))

    rdf_graph_2, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    assert len(rdf_graph_2) == len(rdf_graph)
    assert num_urirefs + num_bnodes + num_literals == len(rdf_graph_2.all_nodes())
    bnode_2 = rdf_graph_2.value(bob, nationality)
    assert (bnode_2, country, Literal("Canada")) in rdf_graph_2

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_6_RPT", get_rdf_graph("cases/6.trig"))],
)
def test_rpt_case_6(name: str, rdf_graph: RDFGraph) -> None:
    num_triples = 12
    num_urirefs = 9
    num_bnodes = 0
    num_literals = 2

    _type = adbrdf.rdf_id_to_adb_key(str(RDF.type))
    _subClassOf = adbrdf.rdf_id_to_adb_key(str(RDFS.subClassOf))
    _monica = adbrdf.rdf_id_to_adb_key("http://example.com/Monica")
    _person = adbrdf.rdf_id_to_adb_key("http://example.com/Person")
    _monica = adbrdf.rdf_id_to_adb_key("http://example.com/Monica")
    _hasSkill = adbrdf.rdf_id_to_adb_key("http://example.com/hasSkill")
    _management = adbrdf.rdf_id_to_adb_key("http://example.com/Management")
    _entity = adbrdf.rdf_id_to_adb_key("http://example.com/Entity")

    graph1 = "http://example.com/Graph1"
    graph2 = "http://example.com/Graph2"

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph,
        contextualize_graph=False,
        overwrite_graph=True,
        use_async=False,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == num_urirefs + num_bnodes + num_literals
    assert e_count == num_triples

    STATEMENT_COL = adb_graph.vertex_collection(f"{name}_Statement")
    e1 = STATEMENT_COL.get(str(FP64(f"{_monica}-{_type}-{_entity}")))
    assert e1["_sub_graph_uri"] == graph1
    e2 = STATEMENT_COL.get(str(FP64(f"{_monica}-{_hasSkill}-{_management}")))
    assert e2["_sub_graph_uri"] == graph1
    e3 = STATEMENT_COL.get(str(FP64(f"{_monica}-{_type}-{_person}")))
    assert e3["_sub_graph_uri"] == graph2
    e4 = STATEMENT_COL.get(str(FP64(f"{_person}-{_subClassOf}-{_entity}")))
    assert e4["_sub_graph_uri"] == graph2

    rdf_graph_2, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())
    assert len(rdf_graph_2) == len(rdf_graph)
    assert num_urirefs + num_bnodes + num_literals == len(rdf_graph_2.all_nodes())
    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(outersect_graphs(rdf_graph_2, rdf_graph)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_7_RPT", get_rdf_graph("cases/7.ttl"))],
)
def test_rpt_case_7(name: str, rdf_graph: RDFGraph) -> None:
    num_triples = 21
    num_urirefs = 17
    num_bnodes = 0
    num_literals = 1

    adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph,
        contextualize_graph=False,
        overwrite_graph=True,
        use_async=False,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == num_urirefs + num_bnodes + num_literals
    assert e_count == num_triples

    rdf_graph_2, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    assert len(rdf_graph_2) == len(rdf_graph)
    assert num_urirefs + num_bnodes + num_literals == len(rdf_graph_2.all_nodes())
    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(outersect_graphs(rdf_graph_2, rdf_graph)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_8_RPT", get_rdf_graph("cases/8.ttl"))],
)
def test_rpt_case_8(name: str, rdf_graph: RDFGraph) -> None:
    num_triples = 2
    num_urirefs = 2
    num_bnodes = 0
    num_literals = 1

    alice = URIRef("http://example.com/alice")
    bob = URIRef("http://example.com/bob")
    likes = URIRef("http://example.com/likes")
    certainty = URIRef("http://example.com/certainty")
    certainty_val = Literal(
        "0.5", datatype=URIRef("http://www.w3.org/2001/XMLSchema#decimal")
    )

    _alice = adbrdf.rdf_id_to_adb_key(str(alice))
    _bob = adbrdf.rdf_id_to_adb_key(str(bob))
    _certainty = adbrdf.rdf_id_to_adb_key("http://example.com/certainty")
    _05 = adbrdf.rdf_id_to_adb_key("0.5")

    _alice_likes_bob = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=RDF.type, object=RDF.Statement))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph,
        contextualize_graph=False,
        overwrite_graph=True,
        use_async=False,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == num_urirefs + num_bnodes + num_literals
    assert e_count == num_triples

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_alice)
    assert URIREF_COL.has(_bob)

    LITERAL_COL = adb_graph.vertex_collection(f"{name}_Literal")
    assert LITERAL_COL.has(_05)

    STATEMENT_COL = adb_graph.vertex_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(_alice_likes_bob)
    assert STATEMENT_COL.has(str(FP64(f"{_alice_likes_bob}-{_certainty}-{_05}")))

    rdf_graph_2, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())
    assert len(rdf_graph_2) == len(rdf_graph) + 1

    statement = rdf_graph_2.value(predicate=RDF.type, object=RDF.Statement)
    assert (alice, likes, bob) in rdf_graph_2
    assert (statement, RDF.subject, alice) in rdf_graph_2
    assert (statement, RDF.predicate, likes) in rdf_graph_2
    assert (statement, RDF.object, bob) in rdf_graph_2
    assert (statement, certainty, certainty_val) in rdf_graph_2
    assert (statement, adbrdf.adb_key_uri, Literal(_alice_likes_bob)) in rdf_graph_2

    db.delete_graph(name, drop_collections=True)

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph_2,
        contextualize_graph=False,
        overwrite_graph=True,
        use_async=False,
    )

    STATEMENT_COL = adb_graph.vertex_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(_alice_likes_bob)
    assert STATEMENT_COL.has(str(FP64(f"{_alice_likes_bob}-{_certainty}-{_05}")))

    rdf_graph_3, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_2)())
    assert len(rdf_graph_3) == len(rdf_graph_2)
    assert len(outersect_graphs(rdf_graph_3, rdf_graph_2)) == 0
    assert len(outersect_graphs(rdf_graph_2, rdf_graph_3)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_9_RPT", get_rdf_graph("cases/9.ttl"))],
)
def test_rpt_case_9(name: str, rdf_graph: RDFGraph) -> None:
    # Case 9 not yet supported
    pass


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_10_RPT", get_rdf_graph("cases/10.ttl"))],
)
def test_rpt_case_10(name: str, rdf_graph: RDFGraph) -> None:
    num_triples = 2
    num_urirefs = 3
    num_bnodes = 0
    num_literals = 0

    alice = URIRef("http://example.com/alice")
    bobshomepage = URIRef("http://example.com/bobshomepage")
    mainpage = URIRef("http://example.com/mainPage")
    writer = URIRef("http://example.com/writer")
    source = URIRef("http://example.com/source")

    _alice = adbrdf.rdf_id_to_adb_key(str(alice))
    _bobshomepage = adbrdf.rdf_id_to_adb_key(str(bobshomepage))
    _mainpage = adbrdf.rdf_id_to_adb_key(str(mainpage))
    _source = adbrdf.rdf_id_to_adb_key(str(source))
    _mainpage_writer_alice = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=RDF.type, object=RDF.Statement))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph,
        contextualize_graph=False,
        overwrite_graph=True,
        use_async=False,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == num_urirefs + num_bnodes + num_literals
    assert e_count == num_triples

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_alice)
    assert URIREF_COL.has(_mainpage)
    assert URIREF_COL.has(_bobshomepage)

    STATEMENT_COL = adb_graph.vertex_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(_mainpage_writer_alice)
    assert STATEMENT_COL.has(
        str(FP64(f"{_bobshomepage}-{_source}-{_mainpage_writer_alice}"))
    )

    rdf_graph_2, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())
    assert len(rdf_graph_2) == len(rdf_graph) + 1

    statement = rdf_graph_2.value(predicate=RDF.type, object=RDF.Statement)
    assert (mainpage, writer, alice) in rdf_graph_2
    assert (statement, RDF.subject, mainpage) in rdf_graph_2
    assert (statement, RDF.predicate, writer) in rdf_graph_2
    assert (statement, RDF.object, alice) in rdf_graph_2
    assert (bobshomepage, source, statement) in rdf_graph_2
    assert (
        statement,
        adbrdf.adb_key_uri,
        Literal(_mainpage_writer_alice),
    ) in rdf_graph_2

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_11_1_RPT", get_rdf_graph("cases/11_1.ttl"))],
)
def test_rpt_case_11_1(name: str, rdf_graph: RDFGraph) -> None:
    num_triples = 2
    num_urirefs = 3
    num_bnodes = 0
    num_literals = 0

    alice = URIRef("http://example.com/alice")
    bobshomepage = URIRef("http://example.com/bobshomepage")
    mainpage = URIRef("http://example.com/mainPage")
    writer = URIRef("http://example.com/writer")
    source = URIRef("http://example.com/source")

    _alice = adbrdf.rdf_id_to_adb_key(str(alice))
    _bobshomepage = adbrdf.rdf_id_to_adb_key(str(bobshomepage))
    _mainpage = adbrdf.rdf_id_to_adb_key(str(mainpage))
    _source = adbrdf.rdf_id_to_adb_key(str(source))
    _mainpage_writer_alice = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=RDF.type, object=RDF.Statement))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph,
        contextualize_graph=False,
        overwrite_graph=True,
        use_async=False,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == num_urirefs + num_bnodes + num_literals
    assert e_count == num_triples

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_alice)
    assert URIREF_COL.has(_mainpage)
    assert URIREF_COL.has(_bobshomepage)

    STATEMENT_COL = adb_graph.vertex_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(_mainpage_writer_alice)
    assert STATEMENT_COL.has(
        str(FP64(f"{_mainpage_writer_alice}-{_source}-{_bobshomepage}"))
    )

    rdf_graph_2, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())
    assert len(rdf_graph_2) == len(rdf_graph) + 1

    statement = rdf_graph_2.value(predicate=RDF.type, object=RDF.Statement)
    assert (mainpage, writer, alice) in rdf_graph_2
    assert (statement, RDF.subject, mainpage) in rdf_graph_2
    assert (statement, RDF.predicate, writer) in rdf_graph_2
    assert (statement, RDF.object, alice) in rdf_graph_2
    assert (statement, source, bobshomepage) in rdf_graph_2
    assert (
        statement,
        adbrdf.adb_key_uri,
        Literal(_mainpage_writer_alice),
    ) in rdf_graph_2

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_11_2_RPT", get_rdf_graph("cases/11_2.ttl"))],
)
def test_rpt_case_11_2(name: str, rdf_graph: RDFGraph) -> None:
    num_triples = 3
    num_urirefs = 3
    num_bnodes = 0
    num_literals = 1

    alice = URIRef("http://example.com/alice")
    friend = URIRef("http://example.com/friend")
    bob = URIRef("http://example.com/bob")
    mentionedby = URIRef("http://example.com/mentionedBy")
    alex = URIRef("http://example.com/alex")
    age = URIRef("http://example.com/age")

    _alice = adbrdf.rdf_id_to_adb_key(str(alice))
    _bob = adbrdf.rdf_id_to_adb_key(str(bob))
    _mentionedby = adbrdf.rdf_id_to_adb_key(str(mentionedby))
    _alex = adbrdf.rdf_id_to_adb_key(str(alex))
    _age = adbrdf.rdf_id_to_adb_key(str(age))
    _25 = adbrdf.rdf_id_to_adb_key("25")
    _alice_friend_bob = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=RDF.type, object=RDF.Statement))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph,
        contextualize_graph=False,
        overwrite_graph=True,
        use_async=False,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == num_urirefs + num_bnodes + num_literals
    assert e_count == num_triples

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_alice)
    assert URIREF_COL.has(_bob)
    assert URIREF_COL.has(_alex)

    STATEMENT_COL = adb_graph.vertex_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(_alice_friend_bob)
    assert STATEMENT_COL.has(str(FP64(f"{_alex}-{_age}-{_25}")))
    assert STATEMENT_COL.has(str(FP64(f"{_alice_friend_bob}-{_mentionedby}-{_alex}")))

    rdf_graph_2, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())
    assert len(rdf_graph_2) == len(rdf_graph) + 1

    statement = rdf_graph_2.value(predicate=RDF.type, object=RDF.Statement)
    assert (alice, friend, bob) in rdf_graph_2
    assert (statement, RDF.subject, alice) in rdf_graph_2
    assert (statement, RDF.predicate, friend) in rdf_graph_2
    assert (statement, RDF.object, bob) in rdf_graph_2
    assert (statement, mentionedby, alex) in rdf_graph_2
    assert (
        statement,
        adbrdf.adb_key_uri,
        Literal(_alice_friend_bob),
    ) in rdf_graph_2

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_12_1_RPT", get_rdf_graph("cases/12_1.ttl"))],
)
def test_rpt_case_12_1(name: str, rdf_graph: RDFGraph) -> None:
    num_triples = 2
    num_urirefs = 3
    num_bnodes = 0
    num_literals = 0

    mainpage = URIRef("http://example.com/mainPage")
    writer = URIRef("http://example.com/writer")
    alice = URIRef("http://example.com/alice")
    bobshomepage = URIRef("http://example.com/bobshomepage")

    _alice = adbrdf.rdf_id_to_adb_key(str(alice))
    _mainpage = adbrdf.rdf_id_to_adb_key(str(mainpage))
    _bobshomepage = adbrdf.rdf_id_to_adb_key(str(bobshomepage))
    _type = adbrdf.rdf_id_to_adb_key(str(RDF.type))
    _mainpage_writer_alice = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=RDF.type, object=RDF.Statement))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph,
        contextualize_graph=False,
        overwrite_graph=True,
        use_async=False,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == num_urirefs + num_bnodes + num_literals
    assert e_count == num_triples

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_alice)
    assert URIREF_COL.has(_mainpage)
    assert URIREF_COL.has(_bobshomepage)

    STATEMENT_COL = adb_graph.vertex_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(_mainpage_writer_alice)
    assert STATEMENT_COL.has(
        str(FP64(f"{_mainpage_writer_alice}-{_type}-{_bobshomepage}"))
    )

    rdf_graph_2, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())
    assert len(rdf_graph_2) == len(rdf_graph) + 1

    statement = rdf_graph_2.value(predicate=RDF.type, object=RDF.Statement)
    assert (mainpage, writer, alice) in rdf_graph_2
    assert (statement, RDF.subject, mainpage) in rdf_graph_2
    assert (statement, RDF.predicate, writer) in rdf_graph_2
    assert (statement, RDF.object, alice) in rdf_graph_2
    assert (statement, RDF.type, bobshomepage) in rdf_graph_2
    assert (
        statement,
        adbrdf.adb_key_uri,
        Literal(_mainpage_writer_alice),
    ) in rdf_graph_2

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_12_2_RPT", get_rdf_graph("cases/12_2.ttl"))],
)
def test_rpt_case_12_2(name: str, rdf_graph: RDFGraph) -> None:
    num_triples = 2
    num_urirefs = 3
    num_bnodes = 0
    num_literals = 0

    lara = URIRef("http://example.com/lara")
    writer = URIRef("http://example.com/writer")
    owner = URIRef("http://example.com/owner")
    journal = URIRef("http://example.com/journal")

    _lara = adbrdf.rdf_id_to_adb_key(str(lara))
    _writer = adbrdf.rdf_id_to_adb_key(str(writer))
    _owner = adbrdf.rdf_id_to_adb_key(str(owner))
    _journal = adbrdf.rdf_id_to_adb_key(str(journal))
    _lara_type_writer = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=RDF.type, object=RDF.Statement))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph,
        contextualize_graph=False,
        overwrite_graph=True,
        use_async=False,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == num_urirefs + num_bnodes + num_literals
    assert e_count == num_triples

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_lara)
    assert URIREF_COL.has(_writer)
    assert URIREF_COL.has(_journal)

    STATEMENT_COL = adb_graph.vertex_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(_lara_type_writer)
    assert STATEMENT_COL.has(str(FP64(f"{_lara_type_writer}-{_owner}-{_journal}")))

    rdf_graph_2, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())
    assert len(rdf_graph_2) == len(rdf_graph) + 1

    statement = rdf_graph_2.value(predicate=RDF.type, object=RDF.Statement)
    assert (lara, RDF.type, writer) in rdf_graph_2
    assert (statement, RDF.subject, lara) in rdf_graph_2
    assert (statement, RDF.predicate, RDF.type) in rdf_graph_2
    assert (statement, RDF.object, writer) in rdf_graph_2
    assert (statement, owner, journal) in rdf_graph_2
    assert (
        statement,
        adbrdf.adb_key_uri,
        Literal(_lara_type_writer),
    ) in rdf_graph_2

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_13_RPT", get_rdf_graph("cases/13.ttl"))],
)
def test_rpt_case_13(name: str, rdf_graph: RDFGraph) -> None:
    # Case 13 not yet supported
    pass


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_14_1_RPT", get_rdf_graph("cases/14_1.ttl"))],
)
def test_rpt_case_14_1(name: str, rdf_graph: RDFGraph) -> None:
    num_triples = 2
    num_urirefs = 1
    num_bnodes = 0
    num_literals = 2

    college_page = URIRef("http://example.com/college_page")
    subject = URIRef("http://example.com/subject")
    info_page = Literal("Info_Page")
    aau_page = Literal("aau_page")

    _college_page = adbrdf.rdf_id_to_adb_key(str(college_page))
    _subject = adbrdf.rdf_id_to_adb_key(str(subject))
    _info_page = adbrdf.rdf_id_to_adb_key(str(info_page))
    _aau_page = adbrdf.rdf_id_to_adb_key(str(aau_page))

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph,
        contextualize_graph=False,
        overwrite_graph=True,
        use_async=False,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == num_urirefs + num_bnodes + num_literals
    assert e_count == num_triples

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_college_page)

    LITERAL_COL = adb_graph.vertex_collection(f"{name}_Literal")
    assert LITERAL_COL.has(_info_page)
    assert LITERAL_COL.has(_aau_page)

    STATEMENT_COL = adb_graph.vertex_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(str(FP64(f"{_college_page}-{_subject}-{_info_page}")))
    assert STATEMENT_COL.has(str(FP64(f"{_college_page}-{_subject}-{_aau_page}")))

    rdf_graph_2, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())
    assert len(rdf_graph_2) == len(rdf_graph)
    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(outersect_graphs(rdf_graph_2, rdf_graph)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_14_2_RPT", get_rdf_graph("cases/14_2.ttl"))],
)
def test_rpt_case_14_2(name: str, rdf_graph: RDFGraph) -> None:
    num_triples = 3
    num_urirefs = 2
    num_bnodes = 0
    num_literals = 2

    mary = URIRef("http://example.com/Mary")
    likes = URIRef("http://example.com/likes")
    matt = URIRef("http://example.com/Matt")
    certainty = URIRef("http://example.com/certainty")
    certainty_val_1 = Literal(
        "0.5", datatype=URIRef("http://www.w3.org/2001/XMLSchema#decimal")
    )
    certainty_val_2 = Literal(1)

    _mary = adbrdf.rdf_id_to_adb_key(str(mary))
    _matt = adbrdf.rdf_id_to_adb_key(str(matt))
    _certainty = adbrdf.rdf_id_to_adb_key(str(certainty))
    _certainty_val_1 = adbrdf.rdf_id_to_adb_key(str(certainty_val_1))
    _certainty_val_2 = adbrdf.rdf_id_to_adb_key(str(certainty_val_2))
    _mary_likes_matt = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=RDF.type, object=RDF.Statement))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph,
        contextualize_graph=False,
        overwrite_graph=True,
        use_async=False,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == num_urirefs + num_bnodes + num_literals
    assert e_count == num_triples

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_mary)
    assert URIREF_COL.has(_matt)

    LITERAL_COL = adb_graph.vertex_collection(f"{name}_Literal")
    assert LITERAL_COL.has(_certainty_val_1)
    assert LITERAL_COL.has(_certainty_val_2)

    STATEMENT_COL = adb_graph.vertex_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(_mary_likes_matt)
    assert STATEMENT_COL.has(
        str(FP64(f"{_mary_likes_matt}-{_certainty}-{_certainty_val_1}"))
    )
    assert STATEMENT_COL.has(
        str(FP64(f"{_mary_likes_matt}-{_certainty}-{_certainty_val_2}"))
    )

    rdf_graph_2, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())
    assert len(rdf_graph_2) == len(rdf_graph) + 1

    statement = rdf_graph_2.value(predicate=RDF.type, object=RDF.Statement)
    assert (mary, likes, matt) in rdf_graph_2
    assert (statement, RDF.subject, mary) in rdf_graph_2
    assert (statement, RDF.predicate, likes) in rdf_graph_2
    assert (statement, RDF.object, matt) in rdf_graph_2
    assert (statement, certainty, certainty_val_1) in rdf_graph_2
    assert (statement, certainty, certainty_val_2) in rdf_graph_2
    assert (
        statement,
        adbrdf.adb_key_uri,
        Literal(_mary_likes_matt),
    ) in rdf_graph_2

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_15_RPT", get_rdf_graph("cases/15.ttl"))],
)
def test_rpt_case_15(name: str, rdf_graph: RDFGraph) -> None:
    num_triples = 3
    num_urirefs = 2
    num_bnodes = 0
    num_literals = 2

    mary = URIRef("http://example.com/Mary")
    likes = URIRef("http://example.com/likes")
    matt = URIRef("http://example.com/Matt")
    certainty = URIRef("http://example.com/certainty")
    source = URIRef("http://example.com/source")
    certainty_val = Literal(
        "0.5", datatype=URIRef("http://www.w3.org/2001/XMLSchema#decimal")
    )
    text = Literal("text")

    _mary = adbrdf.rdf_id_to_adb_key(str(mary))
    _matt = adbrdf.rdf_id_to_adb_key(str(matt))
    _certainty = adbrdf.rdf_id_to_adb_key(str(certainty))
    _source = adbrdf.rdf_id_to_adb_key(str(source))
    _certainty_val = adbrdf.rdf_id_to_adb_key(str(certainty_val))
    _text = adbrdf.rdf_id_to_adb_key(str(text))
    _mary_likes_matt = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=RDF.type, object=RDF.Statement))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph,
        contextualize_graph=False,
        overwrite_graph=True,
        use_async=False,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == num_urirefs + num_bnodes + num_literals
    assert e_count == num_triples

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_mary)
    assert URIREF_COL.has(_matt)

    LITERAL_COL = adb_graph.vertex_collection(f"{name}_Literal")
    assert LITERAL_COL.has(_certainty_val)
    assert LITERAL_COL.has(_text)

    STATEMENT_COL = adb_graph.vertex_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(_mary_likes_matt)
    assert STATEMENT_COL.has(
        str(FP64(f"{_mary_likes_matt}-{_certainty}-{_certainty_val}"))
    )
    assert STATEMENT_COL.has(str(FP64(f"{_mary_likes_matt}-{_source}-{_text}")))

    rdf_graph_2, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())
    assert len(rdf_graph_2) == len(rdf_graph) + 1

    statement = rdf_graph_2.value(predicate=RDF.type, object=RDF.Statement)
    assert (mary, likes, matt) in rdf_graph_2
    assert (statement, RDF.subject, mary) in rdf_graph_2
    assert (statement, RDF.predicate, likes) in rdf_graph_2
    assert (statement, RDF.object, matt) in rdf_graph_2
    assert (statement, certainty, certainty_val) in rdf_graph_2
    assert (statement, source, text) in rdf_graph_2
    assert (
        statement,
        adbrdf.adb_key_uri,
        Literal(_mary_likes_matt),
    ) in rdf_graph_2

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Meta_RPT", get_meta_graph())],
)
def test_rpt_meta(name: str, rdf_graph: RDFGraph) -> None:
    num_triples = META_GRAPH_SIZE
    num_urirefs = META_GRAPH_ALL_RESOURCES
    num_bnodes = 0
    num_literals = META_GRAPH_LITERAL_STATEMENTS

    adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph,
        contextualize_graph=False,
        overwrite_graph=True,
        use_async=False,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == num_urirefs + num_bnodes + num_literals
    assert e_count == num_triples

    rdf_graph_2, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    assert len(rdf_graph_2) == len(rdf_graph)
    assert num_urirefs + num_bnodes + num_literals == len(rdf_graph_2.all_nodes())
    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(outersect_graphs(rdf_graph_2, rdf_graph)) == 0

    db.delete_graph(name, drop_collections=True)

    num_triples = META_GRAPH_SIZE + META_GRAPH_CONTEXTUALIZE_STATEMENTS
    num_urirefs = META_GRAPH_ALL_RESOURCES
    num_bnodes = 0
    num_literals = META_GRAPH_LITERAL_STATEMENTS

    adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph,
        contextualize_graph=True,
        overwrite_graph=True,
        use_async=False,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == num_urirefs + num_bnodes + num_literals
    assert e_count == num_triples

    rdf_graph_3, _ = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    assert len(rdf_graph_3) == len(rdf_graph)
    assert num_urirefs + num_bnodes + num_literals == len(rdf_graph_3.all_nodes())
    assert len(outersect_graphs(rdf_graph, rdf_graph_3)) == 0
    assert len(outersect_graphs(rdf_graph_3, rdf_graph)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_1_PGT", get_rdf_graph("cases/1.ttl"))],
)
def test_pgt_case_1_0(name: str, rdf_graph: RDFGraph) -> None:
    size = len(rdf_graph)
    unique_nodes = 4
    identified_unique_nodes = 4
    non_literal_statements = 3
    contextualize_statements = 4

    # RDF to ArangoDB
    rdf_graph = adbrdf.load_meta_ontology(rdf_graph)
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph,
        overwrite_graph=True,
        contextualize_graph=True,
        use_async=False,
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
    assert adb_graph.edge_collection("type").has(
        str(FP64(f"{_alice}-{_type}-{_person}"))
    )
    assert adb_graph.edge_collection("type").has(str(FP64(f"{_bob}-{_type}-{_person}")))
    assert adb_graph.edge_collection("type").has(
        str(FP64(f"{_person}-{_type}-{_class}"))
    )
    assert adb_graph.has_edge_collection("meets")
    assert adb_graph.edge_collection("meets").has(
        str(FP64(f"{_alice}-{_meets}-{_bob}"))
    )

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
    } == {"Class", "Property", "Ontology", "Person"}

    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0
    diff = outersect_graphs(rdf_graph_2, rdf_graph)
    assert len(diff) == META_GRAPH_CONTEXTUALIZE_STATEMENTS + contextualize_statements
    predicates = {p for p in diff.predicates(unique=True)}
    assert predicates <= {RDF.type, RDFS.domain, RDFS.range}

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
        name, rdf_graph, overwrite_graph=True, contextualize_graph=True, use_async=False
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
    assert adb_graph.edge_collection("type").has(str(FP64(f"{_sam}-{_type}-{_person}")))
    assert adb_graph.edge_collection("type").has(str(FP64(f"{_lee}-{_type}-{_person}")))
    assert adb_graph.edge_collection("type").has(
        str(FP64(f"{_person}-{_type}-{_class}"))
    )
    assert adb_graph.has_edge_collection("mentor")
    assert adb_graph.edge_collection("mentor").has(
        str(FP64(f"{_sam}-{_mentor}-{_lee}"))
    )

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
    } == {"Class", "Property", "Ontology", "Person"}

    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0
    diff = outersect_graphs(rdf_graph_2, rdf_graph)
    assert len(diff) == META_GRAPH_CONTEXTUALIZE_STATEMENTS + contextualize_statements
    predicates = {p for p in diff.predicates(unique=True)}
    assert predicates <= {RDF.type, RDFS.domain, RDFS.range}

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
        name, rdf_graph, overwrite_graph=True, contextualize_graph=True, use_async=False
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
    assert adb_graph.edge_collection("type").has(
        str(FP64(f"{_mentorJoe}-{_type}-{_property}"))
    )
    assert adb_graph.has_edge_collection("mentorJoe")
    assert adb_graph.edge_collection("mentorJoe").has(
        str(FP64(f"{_martin}-{_mentorJoe}-{_joe}"))
    )

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
    } == {"Class", "Property", "Ontology"}

    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0
    diff = outersect_graphs(rdf_graph_2, rdf_graph)
    assert len(diff) == META_GRAPH_CONTEXTUALIZE_STATEMENTS + contextualize_statements
    predicates = {p for p in diff.predicates(unique=True)}
    assert predicates <= {RDF.type, RDFS.domain, RDFS.range}

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
        name, rdf_graph, overwrite_graph=True, contextualize_graph=True, use_async=False
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
        str(FP64(f"{_supervise}-{_subPropertyOf}-{_administer}"))
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
    } == {"Class", "Property", "Ontology", "Person"}

    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0
    diff = outersect_graphs(rdf_graph_2, rdf_graph)
    assert len(diff) == META_GRAPH_CONTEXTUALIZE_STATEMENTS + contextualize_statements
    predicates = {p for p in diff.predicates(unique=True)}
    assert predicates <= {RDF.type, RDFS.domain, RDFS.range}

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
        name, rdf_graph, overwrite_graph=True, contextualize_graph=True, use_async=False
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
    _relation = adbrdf.rdf_id_to_adb_key("http://example.com/relation")
    _friend = adbrdf.rdf_id_to_adb_key("http://example.com/friend")
    _tom = adbrdf.rdf_id_to_adb_key("http://example.com/Tom")
    _chris = adbrdf.rdf_id_to_adb_key("http://example.com/Chris")

    assert adb_graph.has_edge_collection("type")
    assert adb_graph.edge_collection("type").has(
        str(FP64(f"{_friend}-{_type}-{_relation}"))
    )
    assert adb_graph.edge_collection("type").has(
        str(FP64(f"{_relation}-{_type}-{_class}"))
    )

    assert adb_graph.has_edge_collection("friend")
    assert adb_graph.edge_collection("friend").has(
        str(FP64(f"{_tom}-{_friend}-{_chris}"))
    )

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
    assert (friend, RDF.type, relation) in rdf_graph_2
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
    } == {"Class", "Property", "Ontology"}

    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0
    diff = outersect_graphs(rdf_graph_2, rdf_graph)
    assert len(diff) == META_GRAPH_CONTEXTUALIZE_STATEMENTS + contextualize_statements
    predicates = {p for p in diff.predicates(unique=True)}
    assert predicates <= {RDF.type, RDFS.domain, RDFS.range}

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
        name, rdf_graph, overwrite_graph=True, contextualize_graph=True, use_async=False
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
    assert adb_graph.edge_collection("type").has(
        str(FP64(f"{_index}-{_type}-{_property}"))
    )

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
    } == {"Class", "Property", "Ontology"}

    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0
    diff = outersect_graphs(rdf_graph_2, rdf_graph)
    assert len(diff) == META_GRAPH_CONTEXTUALIZE_STATEMENTS + contextualize_statements
    predicates = {p for p in diff.predicates(unique=True)}
    assert predicates <= {RDF.type, RDFS.domain, RDFS.range}

    db.delete_graph(name, drop_collections=True)


# TODO - REVISIT
# NOTE: No current support for Literal datatype persistence in PGT Transformation
# i.e we lose the @en or @da language suffix
@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_3_2_PGT", get_rdf_graph("cases/3_2.ttl"))],
)
def test_pgt_case_3_2(name: str, rdf_graph: RDFGraph) -> None:
    unique_nodes = 2
    non_literal_statements = 0

    # RDF to ArangoDB
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph,
        overwrite_graph=True,
        contextualize_graph=False,
        use_async=False,
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
    title = URIRef("http://example.com/title")

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
        name,
        rdf_graph,
        overwrite_graph=True,
        contextualize_graph=False,
        use_async=False,
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
    contents = URIRef("http://example.com/contents")

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
        name,
        rdf_graph,
        overwrite_graph=True,
        contextualize_graph=False,
        use_async=False,
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

    bob = URIRef("http://example.com/bob")
    nationality = URIRef("http://example.com/nationality")
    country = URIRef("http://example.com/country")

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
    unique_nodes = 14
    identified_unique_nodes = 13
    non_literal_statements = 10
    contextualize_statements = 21
    datatype_statements = 1  # see ex:dateOfBirth statement
    rdf_graph_contexts = {
        "http://example.com/Graph1",
        "http://example.com/Graph2",
        f"file://{os.path.abspath(f'{PROJECT_DIR}/tests/data/rdf/cases/6.trig')}",
    }

    rdf_graph = adbrdf.load_meta_ontology(rdf_graph)
    assert {str(sg.identifier) for sg in rdf_graph.contexts()} == (
        META_GRAPH_CONTEXTS | rdf_graph_contexts
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name, rdf_graph, overwrite_graph=True, contextualize_graph=True, use_async=False
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
        str(FP64(f"{_monica}-{_hasSkill}-{_management}"))
    )
    assert edge["_sub_graph_uri"] == "http://example.com/Graph1"

    edge = adb_graph.edge_collection("type").get(
        str(FP64(f"{_monica}-{_type}-{_person}"))
    )
    assert edge["_sub_graph_uri"] == "http://example.com/Graph2"

    edge = adb_graph.edge_collection("type").get(
        str(FP64(f"{_monica}-{_type}-{_entity}"))
    )
    assert edge["_sub_graph_uri"] == "http://example.com/Graph1"

    assert adb_graph.edge_collection("subClassOf").has(
        str(FP64(f"{_person}-{_subClassOf}-{_entity}"))
    )

    # TODO: REVISIT
    # Is there a limit of 1 RDFS Domain per Predicate?
    for _from in [_hasSkill, _homepage, _name, _employer]:
        assert adb_graph.edge_collection("domain").has(
            str(FP64(f"{_from}-{_domain}-{_entity}"))
        )
        assert adb_graph.edge_collection("domain").has(
            str(FP64(f"{_from}-{_domain}-{_person}"))
        )

    print("\n")

    # ArangoDB to RDF
    rdf_graph_2, adb_mapping = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    person = URIRef("http://example.com/Person")
    monica = URIRef("http://example.com/Monica")
    monica_name = URIRef("http://example.com/name")
    monica_homepage = URIRef("http://example.com/homepage")
    dob = URIRef("http://example.com/dateOfBirth")

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
    assert (monica, dob, Literal("1963-03-22")) in rdf_graph_2

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
    } == {"Skill", "Person", "Website", "Ontology", "Property", "Class"}

    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == datatype_statements
    diff = outersect_graphs(rdf_graph_2, rdf_graph)
    assert (
        len(diff)
        == META_GRAPH_CONTEXTUALIZE_STATEMENTS
        + contextualize_statements
        + datatype_statements
    )

    # TODO - REVISIT
    # We lose the original ex:dateOfBirth statement in this transformation
    # because the original statement contains a datatype annotation (xsd:date)
    predicates = {p for p in diff.predicates(unique=True)}
    assert predicates <= {RDF.type, RDFS.domain, RDFS.range, dob}

    assert type(rdf_graph_2) == RDFConjunctiveGraph
    assert {str(sg.identifier) for sg in rdf_graph_2.contexts()} == (
        META_GRAPH_CONTEXTS | rdf_graph_contexts
    )

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
        name, rdf_graph, overwrite_graph=True, contextualize_graph=True, use_async=False
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
    assert adb_graph.edge_collection("type").has(
        str(FP64(f"{_alice}-{_type}-{_author}"))
    )
    assert adb_graph.edge_collection("type").has(
        str(FP64(f"{_alice}-{_type}-{_arson}"))
    )

    assert adb_graph.has_vertex_collection("Zenkey")
    assert adb_graph.has_vertex_collection("Human")
    assert not adb_graph.has_vertex_collection("Animal")
    assert not adb_graph.has_vertex_collection("LivingThing")
    assert adb_graph.edge_collection("type").has(
        str(FP64(f"{_charlie}-{_type}-{_livingthing}"))
    )
    assert adb_graph.edge_collection("type").has(
        str(FP64(f"{_charlie}-{_type}-{_animal}"))
    )
    assert adb_graph.edge_collection("type").has(
        str(FP64(f"{_charlie}-{_type}-{_zenkey}"))
    )
    assert adb_graph.edge_collection("type").has(
        str(FP64(f"{_marty}-{_type}-{_livingthing}"))
    )
    assert adb_graph.edge_collection("type").has(
        str(FP64(f"{_marty}-{_type}-{_animal}"))
    )
    assert adb_graph.edge_collection("type").has(
        str(FP64(f"{_marty}-{_type}-{_human}"))
    )

    assert adb_graph.has_vertex_collection("Artist")
    assert not adb_graph.has_vertex_collection("Singer")
    assert not adb_graph.has_vertex_collection("Writer")
    assert not adb_graph.has_vertex_collection("Guitarist")
    assert adb_graph.edge_collection("type").has(
        str(FP64(f"{_john}-{_type}-{_singer}"))
    )
    assert adb_graph.edge_collection("type").has(
        str(FP64(f"{_john}-{_type}-{_writer}"))
    )
    assert adb_graph.edge_collection("type").has(
        str(FP64(f"{_john}-{_type}-{_guitarist}"))
    )
    assert not adb_graph.edge_collection("type").has(
        str(FP64(f"{_john}-{_type}-{_artist}"))
    )

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
    } == {"Zenkey", "Arson", "Class", "Ontology", "Artist", "Property", "Human"}

    diff_1 = outersect_graphs(rdf_graph, rdf_graph_2)
    assert len(diff_1) == 1
    assert (john, adbrdf.adb_col_uri, Literal("Artist")) in diff_1

    diff_2 = outersect_graphs(rdf_graph_2, rdf_graph)
    assert len(diff_2) == META_GRAPH_CONTEXTUALIZE_STATEMENTS + contextualize_statements

    predicates = {p for p in diff_2.predicates(unique=True)}
    assert predicates <= {RDF.type, RDFS.domain, RDFS.range}

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Meta_PGT", get_meta_graph())],
)
def test_pgt_meta(name: str, rdf_graph: RDFConjunctiveGraph) -> None:
    assert {str(sg.identifier) for sg in rdf_graph.contexts()} == META_GRAPH_CONTEXTS

    adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph,
        contextualize_graph=True,
        overwrite_graph=True,
        use_async=False,
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
    } == {"Class", "Property", "Ontology"}

    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0
    diff = outersect_graphs(rdf_graph_2, rdf_graph)
    assert len(diff) == META_GRAPH_CONTEXTUALIZE_STATEMENTS
    assert {str(sg.identifier) for sg in rdf_graph_2.contexts()} == META_GRAPH_CONTEXTS

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Collection_PGT", get_rdf_graph("collection.ttl"))],
)
def test_pgt_collection(name: str, rdf_graph: RDFGraph) -> None:
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph,
        overwrite_graph=True,
        contextualize_graph=False,
        use_async=False,
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
    assert adb_graph.edge_collection("random").has(
        str(FP64(f"{_doc}-{_random}-{_mars}"))
    )

    print("\n")

    # ArangoDB to RDF
    rdf_graph_2, adb_mapping = adbrdf.arangodb_graph_to_rdf(
        name, type(rdf_graph)(), "collection"
    )

    assert len(rdf_graph_2) == 123
    assert len(adb_mapping) == 12

    doc = URIRef("http://example.com/Doc")
    planets = URIRef("http://example.com/planets")

    numbers = URIRef("http://example.com/numbers")
    random = URIRef("http://example.com/random")
    nested_container = URIRef("http://example.com/nested_container")

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
        name,
        rdf_graph,
        overwrite_graph=True,
        contextualize_graph=False,
        use_async=False,
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
    assert len(adb_mapping) == 10

    doc = URIRef("http://example.com/Doc")
    planets = URIRef("http://example.com/planets")
    numbers = URIRef("http://example.com/numbers")

    assert (doc, numbers, None) in rdf_graph_2
    assert (doc, planets, None) in rdf_graph_2

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name",
    [("TestGraph")],
)
def test_adb_doc_with_dict_property(name: str) -> None:
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


@pytest.mark.parametrize("name", [("GameOfThrones")])
def test_adb_native_graph(name: str) -> None:
    adb_graph = db.graph(name)
    rdf_graph, adb_mapping = adbrdf.arangodb_graph_to_rdf(
        name,
        RDFGraph(),
        list_conversion_mode="static",
        infer_type_from_adb_v_col=True,
        include_adb_key_statements=True,
    )

    doc_map: Dict[str, str] = {}
    adb_graph_namespace = f"{db._conn._url_prefixes[0]}/{name}#"

    doc: dict
    edge: dict
    for v_col in adb_graph.vertex_collections():
        v_col_uri = URIRef(f"{adb_graph_namespace}{v_col}")

        for doc in db.collection(v_col):
            doc_map[doc["_id"]] = doc["_key"]

            term = URIRef(f"{adb_graph_namespace}{doc['_key']}")
            assert (term, RDF.type, v_col_uri) in rdf_graph

            for k, _ in doc.items():
                if k not in ["_key", "_id", "_rev"]:
                    property = URIRef(f"{adb_graph_namespace}{k}")
                    assert (term, property, None) in rdf_graph

            assert (term, adbrdf.adb_col_uri, Literal(v_col)) in adb_mapping
            assert (term, adbrdf.adb_key_uri, Literal(doc["_key"])) in rdf_graph

    for e_d in adb_graph.edge_definitions():
        e_col = e_d["edge_collection"]
        e_col_uri = URIRef(f"{adb_graph_namespace}{e_col}")

        for doc in db.collection(e_col):
            subject = URIRef(f"{adb_graph_namespace}{doc_map[doc['_from']]}")
            object = URIRef(f"{adb_graph_namespace}{doc_map[doc['_to']]}")
            assert (subject, e_col_uri, object) in rdf_graph

            edge_has_metadata = False
            edge = URIRef(f"{adb_graph_namespace}{doc['_key']}")
            for k, _ in doc.items():
                if k not in ["_key", "_id", "_rev", "_from", "_to"]:
                    edge_has_metadata = True
                    property = URIRef(f"{adb_graph_namespace}{k}")
                    assert (edge, property, None) in rdf_graph

            if edge_has_metadata:
                assert (edge, RDF.type, RDF.Statement) in rdf_graph
                assert (edge, RDF.subject, subject) in rdf_graph
                assert (edge, RDF.predicate, e_col_uri) in rdf_graph
                assert (edge, RDF.object, object) in rdf_graph

                assert (edge, adbrdf.adb_key_uri, Literal(doc["_key"])) in rdf_graph

    ####################################################
    adbrdf.rdf_to_arangodb_by_rpt(
        name, rdf_graph, use_async=False, overwrite_graph=True
    )

    key_uri_triples = len({o for _, p, o in rdf_graph if p == adbrdf.adb_key_uri})
    rdf_star_triples = (
        len([1 for _, p, o in rdf_graph if (p, o) == (RDF.type, RDF.Statement)]) * 4
    )

    assert (
        db.collection(f"{name}_Statement").count()
        == len(rdf_graph) - key_uri_triples - rdf_star_triples
    )

    bnodes = {o for _, p, o in rdf_graph if type(o) is BNode}
    assert db.collection(f"{name}_BNode").count() == len(bnodes)

    lit = {o for _, p, o in rdf_graph if type(o) is Literal and p != adbrdf.adb_key_uri}
    assert db.collection(f"{name}_Literal").count() == len(lit)

    urirefs = set()
    for s, _, o in rdf_graph:
        if (s, RDF.type, RDF.Statement) in rdf_graph:
            continue

        if type(s) is URIRef:
            urirefs.add(s)
        if type(o) is URIRef:
            urirefs.add(o)

    assert db.collection(f"{name}_URIRef").count() == len(urirefs)

    ####################################################

    rdf_graph_2, _ = adbrdf.arangodb_graph_to_rdf(
        name,
        RDFGraph(),
        list_conversion_mode="static",
        include_adb_key_statements=True,
    )

    assert len(outersect_graphs(rdf_graph, rdf_graph_2)) == 0

    diff_1 = outersect_graphs(rdf_graph_2, rdf_graph)

    ####################################################

    adbrdf.rdf_to_arangodb_by_pgt(
        name, rdf_graph, overwrite_graph=True, adb_mapping=adb_mapping, use_async=False
    )

    # TODO: Add assertions

    ####################################################

    rdf_graph_3, _ = adbrdf.arangodb_graph_to_rdf(
        name,
        RDFGraph(),
        list_conversion_mode="static",
        include_adb_key_statements=True,
    )

    assert len(outersect_graphs(rdf_graph, rdf_graph_3)) == 0

    diff_2 = outersect_graphs(rdf_graph_3, rdf_graph)

    ####################################################

    diff_3 = outersect_graphs(rdf_graph_2, rdf_graph_3)
    diff_4 = outersect_graphs(rdf_graph_3, rdf_graph_2)

    for diff in [diff_1, diff_2, diff_3, diff_4]:
        predicates = {p for p in diff.predicates(unique=True)}
        assert predicates <= {adbrdf.adb_key_uri}

    ####################################################

    db.delete_graph(name, drop_collections=True)
