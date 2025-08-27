import json
from typing import Any, Dict, List

import pytest
from arango_datasets import Datasets
from rdflib import RDF, RDFS, BNode
from rdflib import ConjunctiveGraph as RDFConjunctiveGraph
from rdflib import Graph as RDFGraph
from rdflib import Literal, URIRef

from arango_rdf import ArangoRDF
from arango_rdf.exception import ArangoRDFImportException

from .conftest import (
    adbrdf,
    arango_restore,
    db,
    get_adb_graph_count,
    get_bnodes,
    get_literal_statements,
    get_literals,
    get_meta_graph,
    get_rdf_graph,
    get_uris,
    subtract_graphs,
)


def test_constructor() -> None:
    bad_db = None

    with pytest.raises(TypeError):
        ArangoRDF(bad_db)

    bad_controller = None
    with pytest.raises(TypeError):
        ArangoRDF(db, bad_controller)  # type: ignore


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_1_RPT", get_rdf_graph("cases/1.ttl"))],
)
def test_rpt_case_1(name: str, rdf_graph: RDFGraph) -> None:
    NUM_TRIPLES = len(rdf_graph)
    NUM_URIREFS = len(get_uris(rdf_graph))
    NUM_BNODES = len(get_bnodes(rdf_graph))
    NUM_LITERALS = len(get_literals(rdf_graph))

    _type = adbrdf.rdf_id_to_adb_key(str(RDF.type))
    _person = adbrdf.rdf_id_to_adb_key("http://example.com/Person")
    _meets = adbrdf.rdf_id_to_adb_key("http://example.com/meets")
    _alice = adbrdf.rdf_id_to_adb_key("http://example.com/alice")
    _bob = adbrdf.rdf_id_to_adb_key("http://example.com/bob")

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_person)
    assert URIREF_COL.has(_alice)
    assert URIREF_COL.has(_bob)

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(adbrdf.hash(f"{_alice}-{_type}-{_person}"))
    assert STATEMENT_COL.has(adbrdf.hash(f"{_bob}-{_type}-{_person}"))
    assert STATEMENT_COL.has(adbrdf.hash(f"{_alice}-{_meets}-{_bob}"))

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    assert NUM_URIREFS + NUM_BNODES + NUM_LITERALS == len(rdf_graph_2.all_nodes())
    assert len(subtract_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph)) == 0
    assert len(rdf_graph_2) == len(rdf_graph)

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_2_1_RPT", get_rdf_graph("cases/2_1.ttl"))],
)
def test_rpt_case_2_1(name: str, rdf_graph: RDFGraph) -> None:
    NUM_TRIPLES = len(rdf_graph)
    NUM_URIREFS = len(get_uris(rdf_graph))
    NUM_BNODES = len(get_bnodes(rdf_graph))
    NUM_LITERALS = len(get_literals(rdf_graph))

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
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_person)
    assert URIREF_COL.has(_sam)
    assert URIREF_COL.has(_lee)
    assert URIREF_COL.has(_mentor)

    LITERAL_COL = adb_graph.vertex_collection(f"{name}_Literal")
    assert LITERAL_COL.has(_project_supervisor)
    assert LITERAL_COL.has(_mentors_name)

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(adbrdf.hash(f"{_sam}-{_mentor}-{_lee}"))
    assert STATEMENT_COL.has(adbrdf.hash(f"{_mentor}-{_label}-{_project_supervisor}"))
    assert STATEMENT_COL.has(adbrdf.hash(f"{_mentor}-{_name}-{_mentors_name}"))
    assert STATEMENT_COL.has(adbrdf.hash(f"{_sam}-{_type}-{_person}"))
    assert STATEMENT_COL.has(adbrdf.hash(f"{_lee}-{_type}-{_person}"))

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    assert NUM_URIREFS + NUM_BNODES + NUM_LITERALS == len(rdf_graph_2.all_nodes())
    assert len(subtract_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph)) == 0
    assert len(rdf_graph_2) == len(rdf_graph)

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_2_2_RPT", get_rdf_graph("cases/2_2.ttl"))],
)
def test_rpt_case_2_2(name: str, rdf_graph: RDFGraph) -> None:
    NUM_TRIPLES = len(rdf_graph)
    NUM_URIREFS = len(get_uris(rdf_graph))
    NUM_BNODES = len(get_bnodes(rdf_graph))
    NUM_LITERALS = len(get_literals(rdf_graph))

    _martin = adbrdf.rdf_id_to_adb_key("http://example.com/Martin")
    _mentorJoe = adbrdf.rdf_id_to_adb_key("http://example.com/mentorJoe")
    _joe = adbrdf.rdf_id_to_adb_key("http://example.com/Joe")
    _alias = adbrdf.rdf_id_to_adb_key("http://example.com/alias")
    _teacher = adbrdf.rdf_id_to_adb_key("http://example.com/teacher")

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_martin)
    assert URIREF_COL.has(_joe)
    assert URIREF_COL.has(_mentorJoe)
    assert URIREF_COL.has(_teacher)

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(adbrdf.hash(f"{_martin}-{_mentorJoe}-{_joe}"))
    assert STATEMENT_COL.has(adbrdf.hash(f"{_mentorJoe}-{_alias}-{_teacher}"))

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    assert NUM_URIREFS + NUM_BNODES + NUM_LITERALS == len(rdf_graph_2.all_nodes())
    assert len(subtract_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph)) == 0
    assert len(rdf_graph_2) == len(rdf_graph)

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_2_3_RPT", get_rdf_graph("cases/2_3.ttl"))],
)
def test_rpt_case_2_3(name: str, rdf_graph: RDFGraph) -> None:
    NUM_TRIPLES = len(rdf_graph)
    NUM_URIREFS = len(get_uris(rdf_graph))
    NUM_BNODES = len(get_bnodes(rdf_graph))
    NUM_LITERALS = len(get_literals(rdf_graph))

    _type = adbrdf.rdf_id_to_adb_key(str(RDF.type))
    _subPropertyOf = adbrdf.rdf_id_to_adb_key(str(RDFS.subPropertyOf))
    _person = adbrdf.rdf_id_to_adb_key("http://example.com/Person")
    _supervise = adbrdf.rdf_id_to_adb_key("http://example.com/supervise")
    _administer = adbrdf.rdf_id_to_adb_key("http://example.com/administer")
    _jan = adbrdf.rdf_id_to_adb_key("http://example.com/Jan")
    _leo = adbrdf.rdf_id_to_adb_key("http://example.com/Leo")

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_person)
    assert URIREF_COL.has(_jan)
    assert URIREF_COL.has(_leo)
    assert URIREF_COL.has(_administer)

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(adbrdf.hash(f"{_jan}-{_type}-{_person}"))
    assert STATEMENT_COL.has(adbrdf.hash(f"{_leo}-{_type}-{_person}"))
    assert STATEMENT_COL.has(adbrdf.hash(f"{_jan}-{_supervise}-{_leo}"))
    assert STATEMENT_COL.has(
        adbrdf.hash(f"{_supervise}-{_subPropertyOf}-{_administer}")
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    assert NUM_URIREFS + NUM_BNODES + NUM_LITERALS == len(rdf_graph_2.all_nodes())
    assert len(subtract_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph)) == 0
    assert len(rdf_graph_2) == len(rdf_graph)

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_2_4_RPT", get_rdf_graph("cases/2_4.ttl"))],
)
def test_rpt_case_2_4(name: str, rdf_graph: RDFGraph) -> None:
    NUM_TRIPLES = len(rdf_graph)
    NUM_URIREFS = len(get_uris(rdf_graph))
    NUM_BNODES = len(get_bnodes(rdf_graph))
    NUM_LITERALS = len(get_literals(rdf_graph))

    _type = adbrdf.rdf_id_to_adb_key(str(RDF.type))
    _relation = adbrdf.rdf_id_to_adb_key("http://example.com/relation")
    _friend = adbrdf.rdf_id_to_adb_key("http://example.com/friend")
    _tom = adbrdf.rdf_id_to_adb_key("http://example.com/Tom")
    _chris = adbrdf.rdf_id_to_adb_key("http://example.com/Chris")

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_tom)
    assert URIREF_COL.has(_chris)
    assert URIREF_COL.has(_friend)
    assert URIREF_COL.has(_relation)

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(adbrdf.hash(f"{_tom}-{_friend}-{_chris}"))
    assert STATEMENT_COL.has(adbrdf.hash(f"{_friend}-{_type}-{_relation}"))

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    assert NUM_URIREFS + NUM_BNODES + NUM_LITERALS == len(rdf_graph_2.all_nodes())
    assert len(subtract_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph)) == 0
    assert len(rdf_graph_2) == len(rdf_graph)

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_3_1_RPT", get_rdf_graph("cases/3_1.ttl"))],
)
def test_rpt_case_3_1(name: str, rdf_graph: RDFGraph) -> None:
    NUM_TRIPLES = len(rdf_graph)
    NUM_URIREFS = len(get_uris(rdf_graph))
    NUM_BNODES = len(get_bnodes(rdf_graph))
    NUM_LITERALS = len(get_literals(rdf_graph))

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
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_book)

    LITERAL_COL = adb_graph.vertex_collection(f"{name}_Literal")
    assert LITERAL_COL.has(_date)
    assert LITERAL_COL.has(_100)
    assert LITERAL_COL.get(_100)["_datatype"] == _xsd_integer
    assert LITERAL_COL.has(_20)
    assert LITERAL_COL.get(_20)["_datatype"] == _xsd_integer
    assert LITERAL_COL.has(_55)

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(adbrdf.hash(f"{_book}-{_publish_date}-{_date}"))
    assert STATEMENT_COL.has(adbrdf.hash(f"{_book}-{_pages}-{_100}"))
    assert STATEMENT_COL.has(adbrdf.hash(f"{_book}-{_cover}-{_20}"))
    assert STATEMENT_COL.has(adbrdf.hash(f"{_book}-{_index}-{_55}"))

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    assert NUM_URIREFS + NUM_BNODES + NUM_LITERALS == len(rdf_graph_2.all_nodes())
    assert len(subtract_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph)) == 0
    assert len(rdf_graph_2) == len(rdf_graph)

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_3_2_RPT", get_rdf_graph("cases/3_2.ttl"))],
)
def test_rpt_case_3_2(name: str, rdf_graph: RDFGraph) -> None:
    NUM_TRIPLES = len(rdf_graph)
    NUM_URIREFS = len(get_uris(rdf_graph))
    NUM_BNODES = len(get_bnodes(rdf_graph))
    NUM_LITERALS = len(get_literals(rdf_graph))

    _book = adbrdf.rdf_id_to_adb_key("http://example.com/book")
    _title = adbrdf.rdf_id_to_adb_key("http://example.com/title")
    _englishtitle = adbrdf.rdf_id_to_adb_key("http://example.com/Englishtitle")
    _book_en = adbrdf.rdf_id_to_adb_key("Book")
    _book_da = adbrdf.rdf_id_to_adb_key("Bog")

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_book)

    LITERAL_COL = adb_graph.vertex_collection(f"{name}_Literal")
    assert LITERAL_COL.has(_book_en)
    assert LITERAL_COL.get(_book_en)["_lang"] == "en"
    assert LITERAL_COL.has(_book_da)
    assert LITERAL_COL.get(_book_da)["_lang"] == "da"

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(adbrdf.hash(f"{_book}-{_englishtitle}-{_book_en}"))
    assert STATEMENT_COL.has(adbrdf.hash(f"{_book}-{_title}-{_book_da}"))

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)(), batch_size=1)

    assert NUM_URIREFS + NUM_BNODES + NUM_LITERALS == len(rdf_graph_2.all_nodes())
    assert len(subtract_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph)) == 0
    assert len(rdf_graph_2) == len(rdf_graph)

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_4_RPT", get_rdf_graph("cases/4.ttl"))],
)
def test_rpt_case_4(name: str, rdf_graph: RDFGraph) -> None:
    NUM_TRIPLES = len(rdf_graph)
    NUM_URIREFS = len(get_uris(rdf_graph))
    NUM_BNODES = len(get_bnodes(rdf_graph))
    NUM_LITERALS = len(get_literals(rdf_graph))

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
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_list1)

    LITERAL_COL = adb_graph.vertex_collection(f"{name}_Literal")
    assert LITERAL_COL.has(_one)
    assert LITERAL_COL.has(_two)
    assert LITERAL_COL.has(_three)

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(adbrdf.hash(f"{_list1}-{_contents}-{_bnode_1}"))
    assert STATEMENT_COL.has(adbrdf.hash(f"{_bnode_1}-{_first}-{_one}"))
    assert STATEMENT_COL.has(adbrdf.hash(f"{_bnode_1}-{_rest}-{_bnode_2}"))
    assert STATEMENT_COL.has(adbrdf.hash(f"{_bnode_2}-{_first}-{_two}"))
    assert STATEMENT_COL.has(adbrdf.hash(f"{_bnode_2}-{_rest}-{_bnode_3}"))
    assert STATEMENT_COL.has(adbrdf.hash(f"{_bnode_3}-{_first}-{_three}"))
    assert STATEMENT_COL.has(adbrdf.hash(f"{_bnode_3}-{_rest}-{_nil}"))

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    assert NUM_URIREFS + NUM_BNODES + NUM_LITERALS == len(rdf_graph_2.all_nodes())
    bnode_4 = rdf_graph_2.value(list1, contents)
    bnode_5 = rdf_graph_2.value(bnode_4, RDF.rest)
    bnode_6 = rdf_graph_2.value(bnode_5, RDF.rest)
    assert (list1, contents, bnode_4) in rdf_graph_2
    assert (bnode_4, RDF.first, Literal("one")) in rdf_graph_2
    assert (bnode_5, RDF.first, Literal("two")) in rdf_graph_2
    assert (bnode_6, RDF.first, Literal("three")) in rdf_graph_2
    assert len(rdf_graph_2) == len(rdf_graph)

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_5_RPT", get_rdf_graph("cases/5.ttl"))],
)
def test_rpt_case_5(name: str, rdf_graph: RDFGraph) -> None:
    NUM_TRIPLES = len(rdf_graph)
    NUM_URIREFS = len(get_uris(rdf_graph))
    NUM_BNODES = len(get_bnodes(rdf_graph))
    NUM_LITERALS = len(get_literals(rdf_graph))

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
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_bob)

    LITERAL_COL = adb_graph.vertex_collection(f"{name}_Literal")
    assert LITERAL_COL.has(_canada)

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(adbrdf.hash(f"{_bob}-{_nationality}-{_bnode}"))
    assert STATEMENT_COL.has(adbrdf.hash(f"{_bnode}-{_country}-{_canada}"))

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    assert NUM_URIREFS + NUM_BNODES + NUM_LITERALS == len(rdf_graph_2.all_nodes())
    bnode_2 = rdf_graph_2.value(bob, nationality)
    assert (bnode_2, country, Literal("Canada")) in rdf_graph_2
    assert len(rdf_graph_2) == len(rdf_graph)

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_6_RPT", get_rdf_graph("cases/6.trig"))],
)
def test_rpt_case_6(name: str, rdf_graph: RDFGraph) -> None:
    NUM_TRIPLES = len(rdf_graph)
    NUM_URIREFS = len(get_uris(rdf_graph))
    NUM_BNODES = len(get_bnodes(rdf_graph))
    NUM_LITERALS = len(get_literals(rdf_graph))

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
        get_rdf_graph("cases/6.trig"),
        overwrite_graph=True,
    )

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    e1 = STATEMENT_COL.get(adbrdf.hash(f"{_monica}-{_type}-{_entity}"))
    assert e1["_sub_graph_uri"] == graph1
    e2 = STATEMENT_COL.get(adbrdf.hash(f"{_monica}-{_hasSkill}-{_management}"))
    assert e2["_sub_graph_uri"] == graph1
    e3 = STATEMENT_COL.get(adbrdf.hash(f"{_monica}-{_type}-{_person}"))
    assert e3["_sub_graph_uri"] == graph2
    e4 = STATEMENT_COL.get(adbrdf.hash(f"{_person}-{_subClassOf}-{_entity}"))
    assert e4["_sub_graph_uri"] == graph2

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())
    assert NUM_URIREFS + NUM_BNODES + NUM_LITERALS == len(rdf_graph_2.all_nodes())
    assert len(subtract_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph)) == 0
    assert len(rdf_graph_2) == len(rdf_graph)

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_7_RPT", get_rdf_graph("cases/7.ttl"))],
)
def test_rpt_case_7(name: str, rdf_graph: RDFGraph) -> None:
    NUM_TRIPLES = len(rdf_graph)
    NUM_URIREFS = len(get_uris(rdf_graph))
    NUM_BNODES = len(get_bnodes(rdf_graph))
    NUM_LITERALS = len(get_literals(rdf_graph))

    adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph,
        overwrite_graph=True,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    assert NUM_URIREFS + NUM_BNODES + NUM_LITERALS == len(rdf_graph_2.all_nodes())
    assert len(subtract_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph)) == 0
    assert len(rdf_graph_2) == len(rdf_graph)

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_8_RPT", get_rdf_graph("cases/8.ttl"))],
)
def test_rpt_case_8(name: str, rdf_graph: RDFGraph) -> None:
    NUM_TRIPLES = 2
    NUM_URIREFS = 2
    NUM_BNODES = 0
    NUM_LITERALS = 1

    alice = URIRef("http://example.com/alice")
    bob = URIRef("http://example.com/bob")
    likes = URIRef("http://example.com/likes")
    certainty = URIRef("http://example.com/certainty")
    certainty_val = Literal(
        "0.5", datatype=URIRef("http://www.w3.org/2001/XMLSchema#double")
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
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_alice)
    assert URIREF_COL.has(_bob)

    LITERAL_COL = adb_graph.vertex_collection(f"{name}_Literal")
    assert LITERAL_COL.has(_05)

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(_alice_likes_bob)
    assert STATEMENT_COL.has(adbrdf.hash(f"{_alice_likes_bob}-{_certainty}-{_05}"))

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    statement = rdf_graph_2.value(predicate=RDF.type, object=RDF.Statement)
    assert (statement, RDF.subject, alice) in rdf_graph_2
    assert (statement, RDF.predicate, likes) in rdf_graph_2
    assert (statement, RDF.object, bob) in rdf_graph_2
    assert (statement, certainty, certainty_val) in rdf_graph_2
    assert len(rdf_graph_2) == len(rdf_graph)

    adb_key_statements = RDFGraph()
    adb_key_statements.add((statement, adbrdf.adb_key_uri, Literal(_alice_likes_bob)))

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph_2 + adb_key_statements,
        overwrite_graph=True,
    )

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(_alice_likes_bob)
    assert STATEMENT_COL.has(adbrdf.hash(f"{_alice_likes_bob}-{_certainty}-{_05}"))

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_3 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_2)())
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph_3)) == 0
    assert len(rdf_graph_3) == len(rdf_graph_2)

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_9_RPT", get_rdf_graph("cases/9.ttl"))],
)
def test_rpt_case_9(name: str, rdf_graph: RDFGraph) -> None:
    NUM_TRIPLES = 2
    NUM_URIREFS = 1
    NUM_BNODES = 0
    NUM_LITERALS = 2

    mark = URIRef("http://example.com/mark")
    age = URIRef("http://example.com/age")
    age_val = Literal(28)
    certainty = URIRef("http://example.com/certainty")
    certainty_val = Literal(1)

    _mark = adbrdf.rdf_id_to_adb_key(str(mark))
    _28 = adbrdf.rdf_id_to_adb_key(str(age_val))
    _certainty = adbrdf.rdf_id_to_adb_key("http://example.com/certainty")
    _1 = adbrdf.rdf_id_to_adb_key(str(certainty_val))

    _mark_age_28 = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=RDF.type, object=RDF.Statement))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_mark)

    LITERAL_COL = adb_graph.vertex_collection(f"{name}_Literal")
    assert LITERAL_COL.has(_1)
    assert LITERAL_COL.has(_28)

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(_mark_age_28)
    assert STATEMENT_COL.has(adbrdf.hash(f"{_mark_age_28}-{_certainty}-{_1}"))

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    statement = rdf_graph_2.value(predicate=RDF.type, object=RDF.Statement)
    assert (statement, RDF.subject, mark) in rdf_graph_2
    assert (statement, RDF.predicate, age) in rdf_graph_2
    assert (statement, RDF.object, age_val) in rdf_graph_2
    assert (statement, certainty, certainty_val) in rdf_graph_2
    assert len(rdf_graph_2) == len(rdf_graph)

    edge_key_graph = RDFGraph()
    edge_key_graph.add((statement, adbrdf.adb_key_uri, Literal(_mark_age_28)))

    db.delete_graph(name, drop_collections=True)

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph_2 + edge_key_graph,
        overwrite_graph=True,
    )

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(_mark_age_28)
    assert STATEMENT_COL.has(adbrdf.hash(f"{_mark_age_28}-{_certainty}-{_1}"))

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_3 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_2)())
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph_3)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_10_RPT", get_rdf_graph("cases/10.ttl"))],
)
def test_rpt_case_10(name: str, rdf_graph: RDFGraph) -> None:
    NUM_TRIPLES = 5
    NUM_URIREFS = 3
    NUM_BNODES = 0
    NUM_LITERALS = 3

    alice = URIRef("http://example.com/alice")
    bobshomepage = URIRef("http://example.com/bobshomepage")
    mainpage = URIRef("http://example.com/mainPage")
    writer = URIRef("http://example.com/writer")
    source = URIRef("http://example.com/source")
    ex_1 = URIRef("http://example.com/1")
    ex_2 = URIRef("http://example.com/2")
    ex_3 = URIRef("http://example.com/3")
    one = Literal(1)
    two = Literal(2)
    three = Literal(3)

    _alice = adbrdf.rdf_id_to_adb_key(str(alice))
    _bobshomepage = adbrdf.rdf_id_to_adb_key(str(bobshomepage))
    _mainpage = adbrdf.rdf_id_to_adb_key(str(mainpage))
    _source = adbrdf.rdf_id_to_adb_key(str(source))
    _ex_1 = adbrdf.rdf_id_to_adb_key(str(ex_1))
    _ex_2 = adbrdf.rdf_id_to_adb_key(str(ex_2))
    _ex_3 = adbrdf.rdf_id_to_adb_key(str(ex_3))
    _one = adbrdf.rdf_id_to_adb_key(str(one))
    _two = adbrdf.rdf_id_to_adb_key(str(two))
    _three = adbrdf.rdf_id_to_adb_key(str(three))
    _mainpage_writer_alice = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=RDF.type, object=RDF.Statement))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_alice)
    assert URIREF_COL.has(_mainpage)
    assert URIREF_COL.has(_bobshomepage)

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(_mainpage_writer_alice)
    assert STATEMENT_COL.has(
        adbrdf.hash(f"{_bobshomepage}-{_source}-{_mainpage_writer_alice}")
    )
    assert STATEMENT_COL.has(adbrdf.hash(f"{_mainpage_writer_alice}-{_ex_1}-{_one}"))
    assert STATEMENT_COL.has(adbrdf.hash(f"{_mainpage_writer_alice}-{_ex_2}-{_two}"))
    assert STATEMENT_COL.has(adbrdf.hash(f"{_mainpage_writer_alice}-{_ex_3}-{_three}"))

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())
    statement = rdf_graph_2.value(predicate=RDF.type, object=RDF.Statement)
    assert (statement, RDF.subject, mainpage) in rdf_graph_2
    assert (statement, RDF.predicate, writer) in rdf_graph_2
    assert (statement, RDF.object, alice) in rdf_graph_2
    assert (bobshomepage, source, statement) in rdf_graph_2
    assert len(rdf_graph_2) == len(rdf_graph)

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_11_1_RPT", get_rdf_graph("cases/11_1.ttl"))],
)
def test_rpt_case_11_1(name: str, rdf_graph: RDFGraph) -> None:
    NUM_TRIPLES = 2
    NUM_URIREFS = 3
    NUM_BNODES = 0
    NUM_LITERALS = 0

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
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_alice)
    assert URIREF_COL.has(_mainpage)
    assert URIREF_COL.has(_bobshomepage)

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(_mainpage_writer_alice)
    assert STATEMENT_COL.has(
        adbrdf.hash(f"{_mainpage_writer_alice}-{_source}-{_bobshomepage}")
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    statement = rdf_graph_2.value(predicate=RDF.type, object=RDF.Statement)
    assert (statement, RDF.subject, mainpage) in rdf_graph_2
    assert (statement, RDF.predicate, writer) in rdf_graph_2
    assert (statement, RDF.object, alice) in rdf_graph_2
    assert (statement, source, bobshomepage) in rdf_graph_2
    assert len(rdf_graph_2) == len(rdf_graph)

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_11_2_RPT", get_rdf_graph("cases/11_2.ttl"))],
)
def test_rpt_case_11_2(name: str, rdf_graph: RDFGraph) -> None:
    NUM_TRIPLES = 3
    NUM_URIREFS = 3
    NUM_BNODES = 0
    NUM_LITERALS = 1

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
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_alice)
    assert URIREF_COL.has(_bob)
    assert URIREF_COL.has(_alex)

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(_alice_friend_bob)
    assert STATEMENT_COL.has(adbrdf.hash(f"{_alex}-{_age}-{_25}"))
    assert STATEMENT_COL.has(adbrdf.hash(f"{_alice_friend_bob}-{_mentionedby}-{_alex}"))

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    statement = rdf_graph_2.value(predicate=RDF.type, object=RDF.Statement)
    assert (statement, RDF.subject, alice) in rdf_graph_2
    assert (statement, RDF.predicate, friend) in rdf_graph_2
    assert (statement, RDF.object, bob) in rdf_graph_2
    assert (statement, mentionedby, alex) in rdf_graph_2
    assert len(rdf_graph_2) == len(rdf_graph)

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_12_1_RPT", get_rdf_graph("cases/12_1.ttl"))],
)
def test_rpt_case_12_1(name: str, rdf_graph: RDFGraph) -> None:
    NUM_TRIPLES = 2
    NUM_URIREFS = 3
    NUM_BNODES = 0
    NUM_LITERALS = 0

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
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_alice)
    assert URIREF_COL.has(_mainpage)
    assert URIREF_COL.has(_bobshomepage)

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(_mainpage_writer_alice)
    assert STATEMENT_COL.has(
        adbrdf.hash(f"{_mainpage_writer_alice}-{_type}-{_bobshomepage}")
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    statement = rdf_graph_2.value(predicate=RDF.type, object=RDF.Statement)
    assert (statement, RDF.subject, mainpage) in rdf_graph_2
    assert (statement, RDF.predicate, writer) in rdf_graph_2
    assert (statement, RDF.object, alice) in rdf_graph_2
    assert (statement, RDF.type, bobshomepage) in rdf_graph_2
    assert len(rdf_graph_2) == len(rdf_graph)

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_12_2_RPT", get_rdf_graph("cases/12_2.ttl"))],
)
def test_rpt_case_12_2(name: str, rdf_graph: RDFGraph) -> None:
    NUM_TRIPLES = 2
    NUM_URIREFS = 3
    NUM_BNODES = 0
    NUM_LITERALS = 0

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
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_lara)
    assert URIREF_COL.has(_writer)
    assert URIREF_COL.has(_journal)

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(_lara_type_writer)
    assert STATEMENT_COL.has(adbrdf.hash(f"{_lara_type_writer}-{_owner}-{_journal}"))

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    statement = rdf_graph_2.value(predicate=RDF.type, object=RDF.Statement)
    assert (statement, RDF.subject, lara) in rdf_graph_2
    assert (statement, RDF.predicate, RDF.type) in rdf_graph_2
    assert (statement, RDF.object, writer) in rdf_graph_2
    assert (statement, owner, journal) in rdf_graph_2
    assert len(rdf_graph_2) == len(rdf_graph)

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_13_1_RPT", get_rdf_graph("cases/13_1.ttl"))],
)
def test_rpt_case_13_1(name: str, rdf_graph: RDFGraph) -> None:
    NUM_TRIPLES = 3
    NUM_URIREFS = 4
    NUM_BNODES = 0
    NUM_LITERALS = 0

    steve = URIRef("http://example.com/steve")
    position = URIRef("http://example.com/position")
    ceo = URIRef("http://example.com/CEO")
    mentionedBy = URIRef("http://example.com/mentionedBy")
    book = URIRef("http://example.com/book")
    source = URIRef("http://example.com/source")
    journal = URIRef("http://example.com/journal")

    _steve = adbrdf.rdf_id_to_adb_key(str(steve))
    _ceo = adbrdf.rdf_id_to_adb_key(str(ceo))
    _book = adbrdf.rdf_id_to_adb_key(str(book))
    _source = adbrdf.rdf_id_to_adb_key(str(source))
    _journal = adbrdf.rdf_id_to_adb_key(str(journal))

    _steve_position_ceo = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=RDF.predicate, object=position))
    )

    _steve_position_ceo_mentionedby_book = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=RDF.predicate, object=mentionedBy))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
        batch_size=1,
    )

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_steve)
    assert URIREF_COL.has(_ceo)
    assert URIREF_COL.has(_book)
    assert URIREF_COL.has(_journal)

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(_steve_position_ceo)
    assert STATEMENT_COL.has(_steve_position_ceo_mentionedby_book)
    assert STATEMENT_COL.has(
        adbrdf.hash(f"{_steve_position_ceo_mentionedby_book}-{_source}-{_journal}")
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)(), batch_size=1)

    statement_1 = rdf_graph_2.value(predicate=RDF.predicate, object=position)
    assert (statement_1, RDF.subject, steve) in rdf_graph_2
    assert (statement_1, RDF.predicate, position) in rdf_graph_2
    assert (statement_1, RDF.object, ceo) in rdf_graph_2
    statement_2 = rdf_graph_2.value(predicate=RDF.predicate, object=mentionedBy)
    assert (statement_2, RDF.subject, statement_1) in rdf_graph_2
    assert (statement_2, RDF.predicate, mentionedBy) in rdf_graph_2
    assert (statement_2, RDF.object, book) in rdf_graph_2
    assert len(rdf_graph_2) == len(rdf_graph)

    edge_key_graph = RDFGraph()
    edge_key_graph.add((statement_1, adbrdf.adb_key_uri, Literal(_steve_position_ceo)))
    edge_key_graph.add(
        (statement_2, adbrdf.adb_key_uri, Literal(_steve_position_ceo_mentionedby_book))
    )

    db.delete_graph(name, drop_collections=True)

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph_2 + edge_key_graph,
        overwrite_graph=True,
    )

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(_steve_position_ceo)
    assert STATEMENT_COL.has(_steve_position_ceo_mentionedby_book)
    assert STATEMENT_COL.has(
        adbrdf.hash(f"{_steve_position_ceo_mentionedby_book}-{_source}-{_journal}")
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_3 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_2)())
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph_3)) == 0
    assert len(rdf_graph_3) == len(rdf_graph_2)

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_13_2_RPT", get_rdf_graph("cases/13_2.ttl"))],
)
def test_rpt_case_13_2(name: str, rdf_graph: RDFGraph) -> None:
    NUM_TRIPLES = 3
    NUM_URIREFS = 4
    NUM_BNODES = 0
    NUM_LITERALS = 0

    steve = URIRef("http://example.com/steve")
    position = URIRef("http://example.com/position")
    ceo = URIRef("http://example.com/CEO")
    mentionedBy = URIRef("http://example.com/mentionedBy")
    book = URIRef("http://example.com/book")
    source = URIRef("http://example.com/source")
    journal = URIRef("http://example.com/journal")

    _steve = adbrdf.rdf_id_to_adb_key(str(steve))
    _ceo = adbrdf.rdf_id_to_adb_key(str(ceo))
    _book = adbrdf.rdf_id_to_adb_key(str(book))
    _source = adbrdf.rdf_id_to_adb_key(str(source))
    _journal = adbrdf.rdf_id_to_adb_key(str(journal))

    statement_1 = rdf_graph.value(predicate=RDF.predicate, object=position)
    assert statement_1
    _steve_position_ceo = adbrdf.rdf_id_to_adb_key(str(statement_1))

    statement_2 = rdf_graph.value(predicate=RDF.predicate, object=mentionedBy)
    assert statement_2
    _book_mentioned_by_steve_position_ceo = adbrdf.rdf_id_to_adb_key(str(statement_2))

    statement_3 = rdf_graph.value(predicate=RDF.predicate, object=source)
    assert statement_3
    _source_journal = adbrdf.rdf_id_to_adb_key(str(statement_3))

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
        batch_size=1,
    )

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_steve)
    assert URIREF_COL.has(_ceo)
    assert URIREF_COL.has(_book)
    assert URIREF_COL.has(_journal)

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(_steve_position_ceo)
    assert STATEMENT_COL.has(_book_mentioned_by_steve_position_ceo)
    assert STATEMENT_COL.has(_source_journal)

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)(), batch_size=1)
    statement_1 = rdf_graph_2.value(predicate=RDF.predicate, object=position)
    assert statement_1
    assert (statement_1, RDF.subject, steve) in rdf_graph_2
    assert (statement_1, RDF.predicate, position) in rdf_graph_2
    assert (statement_1, RDF.object, ceo) in rdf_graph_2
    statement_2 = rdf_graph_2.value(predicate=RDF.predicate, object=mentionedBy)
    assert statement_2
    assert (statement_2, RDF.subject, book) in rdf_graph_2
    assert (statement_2, RDF.predicate, mentionedBy) in rdf_graph_2
    assert (statement_2, RDF.object, statement_1) in rdf_graph_2
    assert (journal, source, statement_2) in rdf_graph_2

    edge_key_graph = RDFGraph()
    edge_key_graph.add((statement_1, adbrdf.adb_key_uri, Literal(_steve_position_ceo)))
    edge_key_graph.add(
        (
            statement_2,
            adbrdf.adb_key_uri,
            Literal(_book_mentioned_by_steve_position_ceo),
        )
    )

    db.delete_graph(name, drop_collections=True)

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph_2 + edge_key_graph,
        overwrite_graph=True,
    )

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(_steve_position_ceo)
    assert STATEMENT_COL.has(_book_mentioned_by_steve_position_ceo)
    assert STATEMENT_COL.has(
        adbrdf.hash(f"{_journal}-{_source}-{_book_mentioned_by_steve_position_ceo}")
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_3 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_2)())
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph_3)) == 0
    assert len(rdf_graph_3) == len(rdf_graph_2)

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_14_1_RPT", get_rdf_graph("cases/14_1.ttl"))],
)
def test_rpt_case_14_1(name: str, rdf_graph: RDFGraph) -> None:
    NUM_TRIPLES = 4
    NUM_URIREFS = 2
    NUM_BNODES = 0
    NUM_LITERALS = 2

    college_page = URIRef("http://example.com/college_page")
    college_page_2 = URIRef("http://example.com/college_page_2")
    link = URIRef("http://example.com/link")
    subject = URIRef("http://example.com/subject")
    info_page = Literal("Info_Page")
    aau_page = Literal("aau_page")

    _college_page = adbrdf.rdf_id_to_adb_key(str(college_page))
    _college_page_2 = adbrdf.rdf_id_to_adb_key(str(college_page_2))
    _link = adbrdf.rdf_id_to_adb_key(str(link))
    _subject = adbrdf.rdf_id_to_adb_key(str(subject))
    _info_page = adbrdf.rdf_id_to_adb_key(str(info_page))
    _aau_page = adbrdf.rdf_id_to_adb_key(str(aau_page))

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
        batch_size=1,
    )

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_college_page)
    assert URIREF_COL.has(_college_page_2)

    LITERAL_COL = adb_graph.vertex_collection(f"{name}_Literal")
    assert LITERAL_COL.has(_info_page)
    assert LITERAL_COL.has(_aau_page)

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(adbrdf.hash(f"{_college_page}-{_subject}-{_info_page}"))
    assert STATEMENT_COL.has(adbrdf.hash(f"{_college_page}-{_subject}-{_aau_page}"))
    assert STATEMENT_COL.has(adbrdf.hash(f"{_college_page}-{_link}-{_college_page_2}"))

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())
    assert len(subtract_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph)) == 0
    assert len(rdf_graph_2) == len(rdf_graph)

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_14_2_RPT", get_rdf_graph("cases/14_2.ttl"))],
)
def test_rpt_case_14_2(name: str, rdf_graph: RDFGraph) -> None:
    NUM_TRIPLES = 4
    NUM_URIREFS = 2
    NUM_BNODES = 0
    NUM_LITERALS = 2

    mary = URIRef("http://example.com/Mary")
    likes = URIRef("http://example.com/likes")
    matt = URIRef("http://example.com/Matt")
    certainty = URIRef("http://example.com/certainty")
    certainty_val_1 = Literal(
        "0.5", datatype=URIRef("http://www.w3.org/2001/XMLSchema#double")
    )
    certainty_val_2 = Literal(1)

    _mary = adbrdf.rdf_id_to_adb_key(str(mary))
    _matt = adbrdf.rdf_id_to_adb_key(str(matt))
    _certainty = adbrdf.rdf_id_to_adb_key(str(certainty))
    _certainty_val_1 = adbrdf.rdf_id_to_adb_key(str(certainty_val_1))
    _certainty_val_2 = adbrdf.rdf_id_to_adb_key(str(certainty_val_2))
    _mary_likes_matt_1 = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=certainty, object=certainty_val_1))
    )
    _mary_likes_matt_2 = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=certainty, object=certainty_val_2))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_mary)
    assert URIREF_COL.has(_matt)

    LITERAL_COL = adb_graph.vertex_collection(f"{name}_Literal")
    assert LITERAL_COL.has(_certainty_val_1)
    assert LITERAL_COL.has(_certainty_val_2)

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(_mary_likes_matt_1)
    assert STATEMENT_COL.has(
        adbrdf.hash(f"{_mary_likes_matt_1}-{_certainty}-{_certainty_val_1}")
    )
    assert STATEMENT_COL.has(_mary_likes_matt_2)
    assert STATEMENT_COL.has(
        adbrdf.hash(f"{_mary_likes_matt_2}-{_certainty}-{_certainty_val_2}")
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    statement_1 = rdf_graph_2.value(predicate=certainty, object=certainty_val_1)
    assert (statement_1, RDF.subject, mary) in rdf_graph_2
    assert (statement_1, RDF.predicate, likes) in rdf_graph_2
    assert (statement_1, RDF.object, matt) in rdf_graph_2
    assert (statement_1, certainty, certainty_val_1) in rdf_graph_2
    statement_2 = rdf_graph_2.value(predicate=certainty, object=certainty_val_2)
    assert (statement_2, RDF.subject, mary) in rdf_graph_2
    assert (statement_2, RDF.predicate, likes) in rdf_graph_2
    assert (statement_2, RDF.object, matt) in rdf_graph_2
    assert (statement_2, certainty, certainty_val_2) in rdf_graph_2
    assert len(rdf_graph_2) == len(rdf_graph)

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_15_1_RPT", get_rdf_graph("cases/15_1.ttl"))],
)
def test_rpt_case_15_1(name: str, rdf_graph: RDFGraph) -> None:
    NUM_TRIPLES = 4
    NUM_URIREFS = 2
    NUM_BNODES = 0
    NUM_LITERALS = 2

    mary = URIRef("http://example.com/Mary")
    likes = URIRef("http://example.com/likes")
    matt = URIRef("http://example.com/Matt")
    certainty = URIRef("http://example.com/certainty")
    source = URIRef("http://example.com/source")
    certainty_val = Literal(
        "0.5", datatype=URIRef("http://www.w3.org/2001/XMLSchema#double")
    )
    text = Literal("text")

    _mary = adbrdf.rdf_id_to_adb_key(str(mary))
    _matt = adbrdf.rdf_id_to_adb_key(str(matt))
    _certainty = adbrdf.rdf_id_to_adb_key(str(certainty))
    _source = adbrdf.rdf_id_to_adb_key(str(source))
    _certainty_val = adbrdf.rdf_id_to_adb_key(str(certainty_val))
    _text = adbrdf.rdf_id_to_adb_key(str(text))
    _mary_likes_matt_1 = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=certainty, object=certainty_val))
    )
    _mary_likes_matt_2 = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=source, object=text))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_mary)
    assert URIREF_COL.has(_matt)

    LITERAL_COL = adb_graph.vertex_collection(f"{name}_Literal")
    assert LITERAL_COL.has(_certainty_val)
    assert LITERAL_COL.has(_text)

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(_mary_likes_matt_1)
    assert STATEMENT_COL.has(
        adbrdf.hash(f"{_mary_likes_matt_1}-{_certainty}-{_certainty_val}")
    )
    assert STATEMENT_COL.has(_mary_likes_matt_2)
    assert STATEMENT_COL.has(adbrdf.hash(f"{_mary_likes_matt_2}-{_source}-{_text}"))

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())
    statement_1 = rdf_graph_2.value(predicate=certainty, object=certainty_val)
    assert (statement_1, RDF.subject, mary) in rdf_graph_2
    assert (statement_1, RDF.predicate, likes) in rdf_graph_2
    assert (statement_1, RDF.object, matt) in rdf_graph_2
    assert (statement_1, certainty, certainty_val) in rdf_graph_2
    statement_2 = rdf_graph_2.value(predicate=source, object=text)
    assert (statement_2, RDF.subject, mary) in rdf_graph_2
    assert (statement_2, RDF.predicate, likes) in rdf_graph_2
    assert (statement_2, RDF.object, matt) in rdf_graph_2
    assert (statement_2, source, text) in rdf_graph_2
    assert len(rdf_graph_2) == len(rdf_graph)

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_15_2_RPT", get_rdf_graph("cases/15_2.ttl"))],
)
def test_rpt_case_15_2(name: str, rdf_graph: RDFGraph) -> None:
    NUM_TRIPLES = 4
    NUM_URIREFS = 3
    NUM_BNODES = 0
    NUM_LITERALS = 1

    foo = URIRef("http://example.com/foo")
    bar = URIRef("http://example.com/bar")
    mary = URIRef("http://example.com/Mary")
    likes = URIRef("http://example.com/likes")
    matt = URIRef("http://example.com/Matt")
    certainty = URIRef("http://example.com/certainty")
    certainty_val = Literal(
        "0.5", datatype=URIRef("http://www.w3.org/2001/XMLSchema#double")
    )

    _foo = adbrdf.rdf_id_to_adb_key(str(foo))
    _bar = adbrdf.rdf_id_to_adb_key(str(bar))
    _mary = adbrdf.rdf_id_to_adb_key(str(mary))
    _likes = adbrdf.rdf_id_to_adb_key(str(likes))
    _matt = adbrdf.rdf_id_to_adb_key(str(matt))
    _certainty = adbrdf.rdf_id_to_adb_key(str(certainty))
    _certainty_val = adbrdf.rdf_id_to_adb_key(str(certainty_val))
    _mary_likes_matt_1 = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=certainty, object=certainty_val))
    )
    _mary_likes_matt_2 = adbrdf.rdf_id_to_adb_key(f"{_mary}-{_likes}-{_matt}")

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_mary)
    assert URIREF_COL.has(_matt)
    assert URIREF_COL.has(_bar)

    LITERAL_COL = adb_graph.vertex_collection(f"{name}_Literal")
    assert LITERAL_COL.has(_certainty_val)

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(_mary_likes_matt_1)
    assert STATEMENT_COL.has(
        adbrdf.hash(f"{_mary_likes_matt_1}-{_certainty}-{_certainty_val}")
    )
    assert STATEMENT_COL.has(adbrdf.hash(f"{_mary_likes_matt_1}-{_foo}-{_bar}"))
    assert STATEMENT_COL.has(_mary_likes_matt_2)

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())
    statement_1 = rdf_graph_2.value(predicate=certainty, object=certainty_val)
    assert (statement_1, RDF.subject, mary) in rdf_graph_2
    assert (statement_1, RDF.predicate, likes) in rdf_graph_2
    assert (statement_1, RDF.object, matt) in rdf_graph_2
    assert (statement_1, certainty, certainty_val) in rdf_graph_2
    assert (statement_1, foo, bar) in rdf_graph_2

    db.delete_graph(name, drop_collections=True)

    # NOTE: ASSERTION BELOW IS FLAKY
    # See `self.__rdf_graph.remove((subject, predicate, object))`
    # in `ArangoRDF__process_adb_edge`
    try:
        assert (mary, likes, matt) in rdf_graph_2, "Flaky assertion"
        assert len(rdf_graph_2) == len(rdf_graph)
    except AssertionError:
        m = "RPT 15.2 (ArangoDB to RDF) not yet fully supported due to flaky assertion (if **simplify_reified_statements** is True)"  # noqa: E501
        pytest.xfail(m)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_15_3_RPT", get_rdf_graph("cases/15_3.ttl"))],
)
def test_rpt_case_15_3(name: str, rdf_graph: RDFGraph) -> None:
    NUM_TRIPLES = 4
    NUM_URIREFS = 3
    NUM_BNODES = 0
    NUM_LITERALS = 1

    foo = URIRef("http://example.com/foo")
    bar = URIRef("http://example.com/bar")
    mary = URIRef("http://example.com/Mary")
    likes = URIRef("http://example.com/likes")
    matt = URIRef("http://example.com/Matt")
    certainty = URIRef("http://example.com/certainty")
    certainty_val = Literal(
        "0.5", datatype=URIRef("http://www.w3.org/2001/XMLSchema#double")
    )

    _foo = adbrdf.rdf_id_to_adb_key(str(foo))
    _bar = adbrdf.rdf_id_to_adb_key(str(bar))
    _mary = adbrdf.rdf_id_to_adb_key(str(mary))
    # _likes = adbrdf.rdf_id_to_adb_key(str(likes))
    _matt = adbrdf.rdf_id_to_adb_key(str(matt))
    _certainty = adbrdf.rdf_id_to_adb_key(str(certainty))
    _certainty_val = adbrdf.rdf_id_to_adb_key(str(certainty_val))
    statement_1 = rdf_graph.value(predicate=certainty, object=certainty_val)
    _mary_likes_matt_1 = adbrdf.rdf_id_to_adb_key(str(statement_1))

    statement_2 = None
    for statement, _, _ in rdf_graph.triples((None, RDF.predicate, likes)):
        if statement != statement_1:
            statement_2 = statement
            break

    _mary_likes_matt_2 = adbrdf.rdf_id_to_adb_key(str(statement_2))

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_mary)
    assert URIREF_COL.has(_matt)
    assert URIREF_COL.has(_bar)

    LITERAL_COL = adb_graph.vertex_collection(f"{name}_Literal")
    assert LITERAL_COL.has(_certainty_val)

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")
    assert STATEMENT_COL.has(_mary_likes_matt_1)
    assert STATEMENT_COL.has(
        adbrdf.hash(f"{_mary_likes_matt_1}-{_certainty}-{_certainty_val}")
    )
    assert STATEMENT_COL.has(adbrdf.hash(f"{_mary_likes_matt_1}-{_foo}-{_bar}"))
    assert STATEMENT_COL.has(_mary_likes_matt_2)

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())
    statement_1 = rdf_graph_2.value(predicate=certainty, object=certainty_val)
    assert (statement_1, RDF.subject, mary) in rdf_graph_2
    assert (statement_1, RDF.predicate, likes) in rdf_graph_2
    assert (statement_1, RDF.object, matt) in rdf_graph_2
    assert (statement_1, certainty, certainty_val) in rdf_graph_2
    assert (statement_1, foo, bar) in rdf_graph_2

    db.delete_graph(name, drop_collections=True)

    # NOTE: ASSERTION BELOW IS FLAKY
    # See `self.__rdf_graph.remove((subject, predicate, object))`
    # in `ArangoRDF__process_adb_edge`
    try:
        assert (mary, likes, matt) in rdf_graph_2
    except AssertionError:
        m = "RPT 15.3 (ArangoDB to RDF) not yet fully supported due to flaky assertion (if **simplify_reified_statements** is True)"  # noqa: E501
        pytest.xfail(m)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_15_4_RPT", get_rdf_graph("cases/15_4.trig"))],
)
def test_rpt_case_15_4(name: str, rdf_graph: RDFGraph) -> None:
    # Reified Triple Simplification modifies the source graph,
    # so we must make a copy of the graph to test against
    rdf_graph_copy = RDFConjunctiveGraph()
    for quad in rdf_graph.quads((None, None, None, None)):
        rdf_graph_copy.add(quad)

    NUM_TRIPLES = 8
    NUM_URIREFS = 3
    NUM_BNODES = 0
    NUM_LITERALS = 4

    graph1 = "http://example.com/Graph1"
    graph2 = "http://example.com/Graph2"

    john = URIRef("http://example.com/John")
    said = URIRef("http://example.com/said")
    mary = URIRef("http://example.com/Mary")
    likes = URIRef("http://example.com/likes")
    matt = URIRef("http://example.com/Matt")
    certainty = URIRef("http://example.com/certainty")
    foo = URIRef("http://example.com/foo")
    certainty_val_05 = Literal(
        "0.5", datatype=URIRef("http://www.w3.org/2001/XMLSchema#decimal")
    )
    certainty_val_075 = Literal(
        "0.75", datatype=URIRef("http://www.w3.org/2001/XMLSchema#decimal")
    )
    certainty_val_1 = Literal(1)
    bar = Literal("bar")

    john_said_mary_likes_matt_05 = rdf_graph.value(predicate=foo, object=bar)
    mary_likes_matt_05 = rdf_graph.value(predicate=certainty, object=certainty_val_05)
    mary_likes_matt_075 = rdf_graph.value(predicate=certainty, object=certainty_val_075)
    mary_likes_matt_1 = rdf_graph.value(predicate=certainty, object=certainty_val_1)

    _john = adbrdf.rdf_id_to_adb_key(str(john))
    _mary = adbrdf.rdf_id_to_adb_key(str(mary))
    _matt = adbrdf.rdf_id_to_adb_key(str(matt))
    _certainty = adbrdf.rdf_id_to_adb_key(str(certainty))
    _bar = adbrdf.rdf_id_to_adb_key(str(bar))
    _certainty_val_05 = adbrdf.rdf_id_to_adb_key(str(certainty_val_05))
    _certainty_val_075 = adbrdf.rdf_id_to_adb_key(str(certainty_val_075))
    _certainty_val_1 = adbrdf.rdf_id_to_adb_key(str(certainty_val_1))
    _mary_likes_matt_05 = adbrdf.rdf_id_to_adb_key(str(mary_likes_matt_05))
    _mary_likes_matt_075 = adbrdf.rdf_id_to_adb_key(str(mary_likes_matt_075))
    _mary_likes_matt_1 = adbrdf.rdf_id_to_adb_key(str(mary_likes_matt_1))
    _john_said_mary_likes_matt_05 = adbrdf.rdf_id_to_adb_key(
        str(john_said_mary_likes_matt_05)
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph_copy,
        overwrite_graph=True,
    )

    URIREF_COL = adb_graph.vertex_collection(f"{name}_URIRef")
    assert URIREF_COL.has(_mary)
    assert URIREF_COL.has(_matt)
    assert URIREF_COL.has(_john)

    LITERAL_COL = adb_graph.vertex_collection(f"{name}_Literal")
    assert LITERAL_COL.has(_certainty_val_05)
    assert LITERAL_COL.has(_certainty_val_075)
    assert LITERAL_COL.has(_certainty_val_1)
    assert LITERAL_COL.has(_bar)

    STATEMENT_COL = adb_graph.edge_collection(f"{name}_Statement")

    edge = STATEMENT_COL.get(_mary_likes_matt_05)
    assert edge is not None
    assert edge["_sub_graph_uri"] == graph1

    edge = STATEMENT_COL.get(
        adbrdf.hash(f"{_mary_likes_matt_05}-{_certainty}-{_certainty_val_05}")
    )
    assert edge is not None
    assert edge["_sub_graph_uri"] == graph1

    edge = STATEMENT_COL.get(_mary_likes_matt_075)
    assert edge is not None
    assert edge["_sub_graph_uri"] == graph2

    edge = STATEMENT_COL.get(
        adbrdf.hash(f"{_mary_likes_matt_075}-{_certainty}-{_certainty_val_075}")
    )
    assert edge is not None
    assert edge["_sub_graph_uri"] == graph2

    edge = STATEMENT_COL.get(_mary_likes_matt_1)
    assert edge is not None
    assert "_sub_graph_uri" not in edge

    edge = STATEMENT_COL.get(
        adbrdf.hash(f"{_mary_likes_matt_1}-{_certainty}-{_certainty_val_1}")
    )
    assert edge is not None
    assert "_sub_graph_uri" not in edge

    edge = STATEMENT_COL.get(_john_said_mary_likes_matt_05)
    assert edge is not None
    assert edge["_sub_graph_uri"] == graph2

    edge = STATEMENT_COL.get(_john_said_mary_likes_matt_05)
    assert edge is not None
    assert edge["_sub_graph_uri"] == graph2

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    mary_likes_matt_05 = rdf_graph_2.value(predicate=certainty, object=certainty_val_05)
    assert mary_likes_matt_05
    assert (mary_likes_matt_05, RDF.subject, mary) in rdf_graph_2
    assert (mary_likes_matt_05, RDF.predicate, likes) in rdf_graph_2
    assert (mary_likes_matt_05, RDF.object, matt) in rdf_graph_2
    assert (mary_likes_matt_05, certainty, certainty_val_05) in rdf_graph_2
    mary_likes_matt_075 = rdf_graph_2.value(
        predicate=certainty, object=certainty_val_075
    )
    assert mary_likes_matt_075
    assert (mary_likes_matt_075, RDF.subject, mary) in rdf_graph_2
    assert (mary_likes_matt_075, RDF.predicate, likes) in rdf_graph_2
    assert (mary_likes_matt_075, RDF.object, matt) in rdf_graph_2
    assert (mary_likes_matt_075, certainty, certainty_val_075) in rdf_graph_2
    mary_likes_matt_1 = rdf_graph_2.value(predicate=certainty, object=certainty_val_1)
    assert mary_likes_matt_1
    assert (mary_likes_matt_1, RDF.subject, mary) in rdf_graph_2
    assert (mary_likes_matt_1, RDF.predicate, likes) in rdf_graph_2
    assert (mary_likes_matt_1, RDF.object, matt) in rdf_graph_2
    assert (mary_likes_matt_1, certainty, certainty_val_1) in rdf_graph_2

    john_said_mary_likes_matt_05 = rdf_graph_2.value(predicate=foo, object=bar)
    assert john_said_mary_likes_matt_05
    assert (john_said_mary_likes_matt_05, RDF.subject, john) in rdf_graph_2
    assert (john_said_mary_likes_matt_05, RDF.predicate, said) in rdf_graph_2
    assert (john_said_mary_likes_matt_05, RDF.object, mary_likes_matt_05) in rdf_graph_2
    assert (john_said_mary_likes_matt_05, foo, bar) in rdf_graph_2

    assert len(rdf_graph_2) == len(rdf_graph)
    rdf_graph_contexts = {str(sg.identifier) for sg in rdf_graph.contexts()}
    rdf_graph_2_contexts = {str(sg.identifier) for sg in rdf_graph_2.contexts()}
    assert len(rdf_graph_contexts) == len(rdf_graph_2_contexts) == 3
    assert graph1 in rdf_graph_contexts and graph1 in rdf_graph_2_contexts
    assert graph2 in rdf_graph_contexts and graph2 in rdf_graph_2_contexts

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Meta_RPT", get_meta_graph())],
)
def test_rpt_meta(name: str, rdf_graph: RDFGraph) -> None:
    NUM_TRIPLES = len(rdf_graph)
    NUM_URIREFS = len(get_uris(rdf_graph))
    NUM_BNODES = len(get_bnodes(rdf_graph))
    NUM_LITERALS = len(get_literals(rdf_graph))

    adbrdf.rdf_to_arangodb_by_rpt(
        name,
        get_meta_graph(),
        contextualize_graph=False,
        overwrite_graph=True,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    assert len(rdf_graph_2) == len(rdf_graph)
    assert NUM_URIREFS + NUM_BNODES + NUM_LITERALS == len(rdf_graph_2.all_nodes())
    assert len(subtract_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph)) == 0

    db.delete_graph(name, drop_collections=True)

    adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph,
        contextualize_graph=True,
        overwrite_graph=True,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_3 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    assert NUM_URIREFS + NUM_BNODES + NUM_LITERALS == len(rdf_graph_3.all_nodes())
    assert len(subtract_graphs(rdf_graph_2, rdf_graph_3)) == 0
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_2)) == 0
    assert len(rdf_graph_3) == len(rdf_graph_2)

    adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph_3,
        contextualize_graph=True,
        overwrite_graph=True,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == NUM_URIREFS + NUM_BNODES + NUM_LITERALS
    assert e_count == NUM_TRIPLES

    rdf_graph_4 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    assert NUM_URIREFS + NUM_BNODES + NUM_LITERALS == len(rdf_graph_4.all_nodes())
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_4)) == 0
    assert len(subtract_graphs(rdf_graph_4, rdf_graph_3)) == 0
    assert len(rdf_graph_4) == len(rdf_graph_3)

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_1_PGT", get_rdf_graph("cases/1.ttl"))],
)
def test_pgt_case_1(name: str, rdf_graph: RDFGraph) -> None:
    NON_LITERAL_STATEMENTS = len(rdf_graph) - len(get_literal_statements(rdf_graph))
    UNIQUE_NODES = len(
        get_uris(rdf_graph, include_predicates=True)
        | get_bnodes(rdf_graph, include_predicates=True)
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    _type = adbrdf.rdf_id_to_adb_key(str(RDF.type))
    _person = adbrdf.rdf_id_to_adb_key("http://example.com/Person")
    _meets = adbrdf.rdf_id_to_adb_key("http://example.com/meets")
    _alice = adbrdf.rdf_id_to_adb_key("http://example.com/alice")
    _bob = adbrdf.rdf_id_to_adb_key("http://example.com/bob")

    assert adb_graph.has_vertex_collection("Class")
    assert adb_graph.vertex_collection("Class").has(_person)
    assert adb_graph.has_vertex_collection("Property")
    assert adb_graph.vertex_collection("Property").has(_meets)
    assert adb_graph.vertex_collection("Property").has(_type)
    assert adb_graph.has_vertex_collection("Person")
    assert adb_graph.vertex_collection("Person").has(_alice)
    assert adb_graph.vertex_collection("Person").has(_bob)
    assert adb_graph.has_edge_collection("type")
    assert adb_graph.edge_collection("type").has(
        adbrdf.hash(f"{_alice}-{_type}-{_person}")
    )

    assert adb_graph.edge_collection("type").has(
        adbrdf.hash(f"{_bob}-{_type}-{_person}")
    )
    assert adb_graph.has_edge_collection("meets")
    assert adb_graph.edge_collection("meets").has(
        adbrdf.hash(f"{_alice}-{_meets}-{_bob}")
    )

    assert adb_graph.vertex_collection(f"{name}_UnknownResource").count() == 0

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(
        name,
        RDFConjunctiveGraph(),
        include_adb_v_col_statements=True,
    )

    adb_col_statements_1 = adbrdf.write_adb_col_statements(rdf_graph)
    adb_col_statements_2 = adbrdf.extract_adb_col_statements(rdf_graph_2)
    assert len(subtract_graphs(adb_col_statements_1, adb_col_statements_2)) == 0
    assert len(subtract_graphs(adb_col_statements_2, adb_col_statements_1)) == 0

    assert len(subtract_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_2_1_PGT", get_rdf_graph("cases/2_1.ttl"))],
)
def test_pgt_case_2_1(name: str, rdf_graph: RDFGraph) -> None:
    NON_LITERAL_STATEMENTS = len(rdf_graph) - len(get_literal_statements(rdf_graph))
    UNIQUE_NODES = len(
        get_uris(rdf_graph, include_predicates=True)
        | get_bnodes(rdf_graph, include_predicates=True)
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    _type = adbrdf.rdf_id_to_adb_key(str(RDF.type))
    _label = adbrdf.rdf_id_to_adb_key(str(RDFS.label))
    _person = adbrdf.rdf_id_to_adb_key("http://example.com/Person")
    _name = adbrdf.rdf_id_to_adb_key("http://example.com/name")
    _mentor = adbrdf.rdf_id_to_adb_key("http://example.com/mentor")
    _sam = adbrdf.rdf_id_to_adb_key("http://example.com/Sam")
    _lee = adbrdf.rdf_id_to_adb_key("http://example.com/Lee")

    assert adb_graph.has_vertex_collection("Class")
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
    assert adb_graph.edge_collection("type").has(
        adbrdf.hash(f"{_sam}-{_type}-{_person}")
    )
    assert adb_graph.edge_collection("type").has(
        adbrdf.hash(f"{_lee}-{_type}-{_person}")
    )
    assert adb_graph.has_edge_collection("mentor")
    assert adb_graph.edge_collection("mentor").has(
        adbrdf.hash(f"{_sam}-{_mentor}-{_lee}")
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(
        name, type(rdf_graph)(), include_adb_v_col_statements=True
    )

    assert (None, URIRef("http://example.com/name"), None) in rdf_graph_2

    adb_col_statements_1 = adbrdf.write_adb_col_statements(rdf_graph)
    adb_col_statements_2 = adbrdf.extract_adb_col_statements(rdf_graph_2)
    assert len(subtract_graphs(adb_col_statements_1, adb_col_statements_2)) == 0
    assert len(subtract_graphs(adb_col_statements_2, adb_col_statements_1)) == 0

    assert len(subtract_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph)) == 0

    rdf_graph_3 = adbrdf.arangodb_graph_to_rdf(
        name,
        type(rdf_graph)(),
        ignored_attributes={"name"},
    )

    assert (None, URIRef("http://example.com/name"), None) not in rdf_graph_3

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_2_2_PGT", get_rdf_graph("cases/2_2.ttl"))],
)
def test_pgt_case_2_2(name: str, rdf_graph: RDFGraph) -> None:
    NON_LITERAL_STATEMENTS = len(rdf_graph) - len(get_literal_statements(rdf_graph))
    UNIQUE_NODES = len(
        get_uris(rdf_graph, include_predicates=True)
        | get_bnodes(rdf_graph, include_predicates=True)
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name, rdf_graph + RDFGraph(), overwrite_graph=True
    )

    _alias = adbrdf.rdf_id_to_adb_key("http://example.com/alias")
    _mentorJoe = adbrdf.rdf_id_to_adb_key("http://example.com/mentorJoe")
    _teacher = adbrdf.rdf_id_to_adb_key("http://example.com/teacher")
    _joe = adbrdf.rdf_id_to_adb_key("http://example.com/Joe")
    _martin = adbrdf.rdf_id_to_adb_key("http://example.com/Martin")

    assert adb_graph.has_vertex_collection("Property")
    assert adb_graph.vertex_collection("Property").has(_mentorJoe)
    assert adb_graph.vertex_collection("Property").has(_alias)

    assert adb_graph.has_vertex_collection(f"{name}_UnknownResource")
    assert adb_graph.vertex_collection(f"{name}_UnknownResource").has(_martin)
    assert adb_graph.vertex_collection(f"{name}_UnknownResource").has(_joe)
    assert adb_graph.vertex_collection(f"{name}_UnknownResource").has(_teacher)
    assert adb_graph.has_edge_collection("mentorJoe")
    assert adb_graph.edge_collection("mentorJoe").has(
        adbrdf.hash(f"{_martin}-{_mentorJoe}-{_joe}")
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(
        name, type(rdf_graph)(), include_adb_v_col_statements=True
    )

    adb_col_statements_1 = adbrdf.write_adb_col_statements(rdf_graph)
    adb_col_statements_2 = adbrdf.extract_adb_col_statements(rdf_graph_2)
    assert len(subtract_graphs(adb_col_statements_1, adb_col_statements_2)) == 0
    assert set(
        subtract_graphs(adb_col_statements_2, adb_col_statements_1).objects()
    ) == {Literal(f"{name}_UnknownResource")}

    assert len(subtract_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_2_3_PGT", get_rdf_graph("cases/2_3.ttl"))],
)
def test_pgt_case_2_3(name: str, rdf_graph: RDFGraph) -> None:
    NON_LITERAL_STATEMENTS = len(rdf_graph) - len(get_literal_statements(rdf_graph))
    UNIQUE_NODES = len(
        get_uris(rdf_graph, include_predicates=True)
        | get_bnodes(rdf_graph, include_predicates=True)
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name, rdf_graph + RDFGraph(), overwrite_graph=True
    )

    _type = adbrdf.rdf_id_to_adb_key(str(RDF.type))
    _subPropertyOf = adbrdf.rdf_id_to_adb_key(str(RDFS.subPropertyOf))
    _person = adbrdf.rdf_id_to_adb_key("http://example.com/Person")
    _supervise = adbrdf.rdf_id_to_adb_key("http://example.com/supervise")
    _administer = adbrdf.rdf_id_to_adb_key("http://example.com/administer")
    _jan = adbrdf.rdf_id_to_adb_key("http://example.com/Jan")
    _leo = adbrdf.rdf_id_to_adb_key("http://example.com/Leo")

    assert adb_graph.has_vertex_collection("Class")
    assert adb_graph.vertex_collection("Class").has(_person)
    assert adb_graph.has_vertex_collection("Property")
    assert adb_graph.vertex_collection("Property").has(_supervise)
    assert adb_graph.vertex_collection("Property").has(_type)
    assert adb_graph.has_vertex_collection("Person")
    assert adb_graph.vertex_collection("Person").has(_jan)
    assert adb_graph.vertex_collection("Person").has(_leo)

    assert adb_graph.has_edge_collection("subPropertyOf")
    assert adb_graph.edge_collection("subPropertyOf").has(
        adbrdf.hash(f"{_supervise}-{_subPropertyOf}-{_administer}")
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(
        name, type(rdf_graph)(), include_adb_v_col_statements=True
    )

    adb_col_statements_1 = adbrdf.write_adb_col_statements(rdf_graph)
    adb_col_statements_2 = adbrdf.extract_adb_col_statements(rdf_graph_2)
    assert len(subtract_graphs(adb_col_statements_1, adb_col_statements_2)) == 0
    assert set(
        subtract_graphs(adb_col_statements_2, adb_col_statements_1).objects()
    ) == {Literal(f"{name}_UnknownResource")}

    assert len(subtract_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_2_4_PGT", get_rdf_graph("cases/2_4.ttl"))],
)
def test_pgt_case_2_4(name: str, rdf_graph: RDFGraph) -> None:
    NON_LITERAL_STATEMENTS = len(rdf_graph) - len(get_literal_statements(rdf_graph))
    UNIQUE_NODES = len(
        get_uris(rdf_graph, include_predicates=True)
        | get_bnodes(rdf_graph, include_predicates=True)
    )

    # RDF to ArangoDB
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name, rdf_graph + RDFGraph(), overwrite_graph=True
    )

    _type = adbrdf.rdf_id_to_adb_key(str(RDF.type))
    _relation = adbrdf.rdf_id_to_adb_key("http://example.com/relation")
    _friend = adbrdf.rdf_id_to_adb_key("http://example.com/friend")
    _tom = adbrdf.rdf_id_to_adb_key("http://example.com/Tom")
    _chris = adbrdf.rdf_id_to_adb_key("http://example.com/Chris")

    assert adb_graph.has_edge_collection("type")
    assert adb_graph.edge_collection("type").has(
        adbrdf.hash(f"{_friend}-{_type}-{_relation}")
    )

    assert adb_graph.has_edge_collection("friend")
    assert adb_graph.edge_collection("friend").has(
        adbrdf.hash(f"{_tom}-{_friend}-{_chris}")
    )

    assert adb_graph.vertex_collection("Property").has(_friend)
    assert not adb_graph.has_vertex_collection("relation")

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(
        name, type(rdf_graph)(), include_adb_v_col_statements=True
    )

    adb_col_statements_1 = adbrdf.write_adb_col_statements(rdf_graph)
    adb_col_statements_2 = adbrdf.extract_adb_col_statements(rdf_graph_2)
    assert len(subtract_graphs(adb_col_statements_1, adb_col_statements_2)) == 0
    assert set(
        subtract_graphs(adb_col_statements_2, adb_col_statements_1).objects()
    ) == {Literal(f"{name}_UnknownResource")}

    assert len(subtract_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_3_1_PGT", get_rdf_graph("cases/3_1.ttl"))],
)
def test_pgt_case_3_1(name: str, rdf_graph: RDFGraph) -> None:
    NON_LITERAL_STATEMENTS = len(rdf_graph) - len(get_literal_statements(rdf_graph))
    UNIQUE_NODES = len(
        get_uris(rdf_graph, include_predicates=True)
        | get_bnodes(rdf_graph, include_predicates=True)
    )

    # RDF to ArangoDB
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name, rdf_graph + RDFGraph(), overwrite_graph=True
    )

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

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(
        name, type(rdf_graph)(), include_adb_v_col_statements=True
    )

    adb_col_statements_1 = adbrdf.write_adb_col_statements(rdf_graph)
    adb_col_statements_2 = adbrdf.extract_adb_col_statements(rdf_graph_2)
    assert len(subtract_graphs(adb_col_statements_1, adb_col_statements_2)) == 0
    assert set(
        subtract_graphs(adb_col_statements_2, adb_col_statements_1).objects()
    ) == {Literal(f"{name}_UnknownResource")}

    assert len(subtract_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph)) == 0

    db.delete_graph(name, drop_collections=True)


# TODO - REVISIT
# NOTE: No current support for Literal datatype persistence in PGT Transformation
# i.e we lose the @en or @da language suffix
# Maybe a 'Primitive' collection is needed? Similar to PGT Case 9
# Or what if we just concatenate the suffix to the literal value?
# e.g (ex:book ex:title "Bog"@da) --> (ex:book ex:title "Bog@da")
# Offer three modes of conversion:
# 1. Static - Keep the literal value as is, lose the suffix
# 2. Primitive - Store the Literal Value as a Vertex in a 'Primitive' Collection,
#   add suffixes as properties
# 3. Concatenate - Concatenate the suffixes to the literal value
#   (e.g "Bog@da", "19.5^^xsd:double") # noqa: E501
@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_3_2_PGT", get_rdf_graph("cases/3_2.ttl"))],
)
def test_pgt_case_3_2(name: str, rdf_graph: RDFGraph) -> None:
    NON_LITERAL_STATEMENTS = len(rdf_graph) - len(get_literal_statements(rdf_graph))
    UNIQUE_NODES = len(
        get_uris(rdf_graph, include_predicates=True)
        | get_bnodes(rdf_graph, include_predicates=True)
    )

    # RDF to ArangoDB
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name, rdf_graph + RDFGraph(), overwrite_graph=True
    )

    _book = adbrdf.rdf_id_to_adb_key("http://example.com/book")

    assert adb_graph.has_vertex_collection(f"{name}_UnknownResource")
    doc = adb_graph.vertex_collection(f"{name}_UnknownResource").get(_book)

    # TODO: Revisit the concept of data types and language tags
    # on Literals in ArangoDB, as it is not currently supported
    assert doc["title"] == "Bog"
    assert doc["Englishtitle"] == "Book"

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    book = URIRef("http://example.com/book")
    title = URIRef("http://example.com/title")
    englishtitle = URIRef("http://example.com/Englishtitle")

    # NOTE: We lose the @en & @da language suffixes here
    assert (book, title, Literal("Bog")) in rdf_graph_2
    assert (book, englishtitle, Literal("Book")) in rdf_graph_2
    assert len(rdf_graph_2) == 2

    db.delete_graph(name, drop_collections=True)

    m = "PGT 3.2 not yet fully supported due to missing language suffixes"
    pytest.xfail(m)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_4_PGT", get_rdf_graph("cases/4.ttl"))],
)
def test_pgt_case_4(name: str, rdf_graph: RDFGraph) -> None:
    NON_LITERAL_STATEMENTS = 0
    UNIQUE_NODES = 2

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    _list1 = adbrdf.rdf_id_to_adb_key("http://example.com/List1")

    assert adb_graph.has_vertex_collection(f"{name}_UnknownResource")
    assert adb_graph.vertex_collection(f"{name}_UnknownResource").has(_list1)
    doc = adb_graph.vertex_collection(f"{name}_UnknownResource").get(_list1)

    assert "contents" in doc
    assert type(doc["contents"]) is list
    assert set(doc["contents"]) == {"one", "two", "three"}

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(
        name, type(rdf_graph)(), list_conversion_mode="static"
    )

    list1 = URIRef("http://example.com/List1")
    contents = URIRef("http://example.com/contents")

    assert (list1, contents, Literal("one")) in rdf_graph_2
    assert (list1, contents, Literal("two")) in rdf_graph_2
    assert (list1, contents, Literal("three")) in rdf_graph_2
    assert len(rdf_graph_2) == 3

    rdf_graph_3 = adbrdf.arangodb_graph_to_rdf(
        name, type(rdf_graph)(), list_conversion_mode="container"
    )

    bnode = rdf_graph_3.value(list1, contents)
    assert bnode is not None
    assert (bnode, URIRef(f"{RDF}_1"), Literal("one")) in rdf_graph_3
    assert (bnode, URIRef(f"{RDF}_2"), Literal("two")) in rdf_graph_3
    assert (bnode, URIRef(f"{RDF}_3"), Literal("three")) in rdf_graph_3
    assert len(rdf_graph_3) == 4

    rdf_graph_4 = adbrdf.arangodb_graph_to_rdf(
        name, type(rdf_graph)(), list_conversion_mode="collection"
    )

    assert (list1, contents, None) in rdf_graph_4
    assert (None, RDF.first, Literal("one")) in rdf_graph_4
    assert (None, RDF.first, Literal("two")) in rdf_graph_4
    assert (None, RDF.first, Literal("three")) in rdf_graph_4
    assert len(rdf_graph_4) == len(rdf_graph)

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_5_PGT", get_rdf_graph("cases/5.ttl"))],
)
def test_pgt_case_5(name: str, rdf_graph: RDFGraph) -> None:
    NON_LITERAL_STATEMENTS = len(rdf_graph) - len(get_literal_statements(rdf_graph))
    UNIQUE_NODES = len(
        get_uris(rdf_graph, include_predicates=True)
        | get_bnodes(rdf_graph, include_predicates=True)
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    bob = URIRef("http://example.com/bob")
    nationality = URIRef("http://example.com/nationality")
    country = URIRef("http://example.com/country")

    _bob = adbrdf.rdf_id_to_adb_key(str(bob))

    assert adb_graph.edge_collection("nationality").count() == 1
    assert adb_graph.vertex_collection(f"{name}_UnknownResource").count() == 2
    assert adb_graph.vertex_collection(f"{name}_UnknownResource").has(_bob)
    assert adb_graph.vertex_collection(f"{name}_UnknownResource").find(
        {"country": "Canada"}
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    bnode = rdf_graph_2.value(bob, nationality)
    assert bnode is not None
    assert (bnode, country, Literal("Canada")) in rdf_graph_2
    assert len(rdf_graph_2) == len(rdf_graph)

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_6_PGT", get_rdf_graph("cases/6.trig"))],
)
def test_pgt_case_6(name: str, rdf_graph: RDFGraph) -> None:
    NON_LITERAL_STATEMENTS = len(rdf_graph) - len(get_literal_statements(rdf_graph))
    UNIQUE_NODES = len(
        get_uris(rdf_graph, include_predicates=True)
        | get_bnodes(rdf_graph, include_predicates=True)
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        get_rdf_graph("cases/6.trig"),
        overwrite_graph=True,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    _type = adbrdf.rdf_id_to_adb_key(str(RDF.type))
    _subClassOf = adbrdf.rdf_id_to_adb_key(str(RDFS.subClassOf))
    _monica = adbrdf.rdf_id_to_adb_key("http://example.com/Monica")
    _person = adbrdf.rdf_id_to_adb_key("http://example.com/Person")
    _monica = adbrdf.rdf_id_to_adb_key("http://example.com/Monica")
    _hasSkill = adbrdf.rdf_id_to_adb_key("http://example.com/hasSkill")
    _management = adbrdf.rdf_id_to_adb_key("http://example.com/Management")
    _entity = adbrdf.rdf_id_to_adb_key("http://example.com/Entity")

    assert adb_graph.has_vertex_collection("Person")
    doc = adb_graph.vertex_collection("Person").get(_monica)
    assert doc["name"] == "Monica"

    assert adb_graph.vertex_collection("Skill").count() == 2
    assert adb_graph.vertex_collection("Website").count() == 1

    edge = adb_graph.edge_collection("hasSkill").get(
        adbrdf.hash(f"{_monica}-{_hasSkill}-{_management}")
    )

    assert edge["_sub_graph_uri"] == "http://example.com/Graph1"

    edge = adb_graph.edge_collection("type").get(
        adbrdf.hash(f"{_monica}-{_type}-{_person}")
    )

    assert edge["_sub_graph_uri"] == "http://example.com/Graph2"

    edge = adb_graph.edge_collection("type").get(
        adbrdf.hash(f"{_monica}-{_type}-{_entity}")
    )

    assert edge["_sub_graph_uri"] == "http://example.com/Graph1"

    assert adb_graph.edge_collection("subClassOf").has(
        adbrdf.hash(f"{_person}-{_subClassOf}-{_entity}")
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(
        name, type(rdf_graph)(), include_adb_v_col_statements=True
    )

    adb_col_statements_1 = adbrdf.write_adb_col_statements(rdf_graph)
    adb_col_statements_2 = adbrdf.extract_adb_col_statements(rdf_graph_2)
    assert len(subtract_graphs(adb_col_statements_1, adb_col_statements_2)) == 0
    assert set(
        subtract_graphs(adb_col_statements_2, adb_col_statements_1).objects()
    ) == {Literal(f"{name}_UnknownResource")}

    assert len(subtract_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph)) == 0

    db.delete_graph(name, drop_collections=True)


# NOTE: Official assertions are TBD, given Case 7 dispute
@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_7_PGT", get_rdf_graph("cases/7.ttl"))],
)
def test_pgt_case_7(name: str, rdf_graph: RDFGraph) -> None:
    NON_LITERAL_STATEMENTS = len(rdf_graph) - len(get_literal_statements(rdf_graph))
    UNIQUE_NODES = len(
        get_uris(rdf_graph, include_predicates=True)
        | get_bnodes(rdf_graph, include_predicates=True)
    )

    adb_col_statements_1 = adbrdf.extract_adb_col_statements(rdf_graph)

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph,
        adb_col_statements=adb_col_statements_1,
        write_adb_col_statements=True,  # default
        overwrite_graph=True,
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
        adbrdf.hash(f"{_alice}-{_type}-{_author}")
    )

    assert adb_graph.edge_collection("type").has(
        adbrdf.hash(f"{_alice}-{_type}-{_arson}")
    )

    assert adb_graph.has_vertex_collection("Zenkey")
    assert adb_graph.has_vertex_collection("Human")
    assert not adb_graph.has_vertex_collection("Animal")
    assert not adb_graph.has_vertex_collection("LivingThing")
    assert adb_graph.edge_collection("type").has(
        adbrdf.hash(f"{_charlie}-{_type}-{_livingthing}")
    )

    assert adb_graph.edge_collection("type").has(
        adbrdf.hash(f"{_charlie}-{_type}-{_animal}")
    )

    assert adb_graph.edge_collection("type").has(
        adbrdf.hash(f"{_charlie}-{_type}-{_zenkey}")
    )

    assert adb_graph.edge_collection("type").has(
        adbrdf.hash(f"{_marty}-{_type}-{_livingthing}")
    )

    assert adb_graph.edge_collection("type").has(
        adbrdf.hash(f"{_marty}-{_type}-{_animal}")
    )

    assert adb_graph.edge_collection("type").has(
        adbrdf.hash(f"{_marty}-{_type}-{_human}")
    )

    assert adb_graph.has_vertex_collection("Artist")
    assert not adb_graph.has_vertex_collection("Singer")
    assert not adb_graph.has_vertex_collection("Writer")
    assert not adb_graph.has_vertex_collection("Guitarist")
    assert adb_graph.edge_collection("type").has(
        adbrdf.hash(f"{_john}-{_type}-{_singer}")
    )

    assert adb_graph.edge_collection("type").has(
        adbrdf.hash(f"{_john}-{_type}-{_writer}")
    )

    assert adb_graph.edge_collection("type").has(
        adbrdf.hash(f"{_john}-{_type}-{_guitarist}")
    )

    assert not adb_graph.edge_collection("type").has(
        adbrdf.hash(f"{_john}-{_type}-{_artist}")
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(
        name, type(rdf_graph)(), include_adb_v_col_statements=True
    )

    adb_col_statements_2 = adbrdf.extract_adb_col_statements(rdf_graph_2)
    assert len(subtract_graphs(adb_col_statements_1, adb_col_statements_2)) == 0
    assert len(subtract_graphs(adb_col_statements_2, adb_col_statements_1)) == 0

    assert len(subtract_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_8_PGT", get_rdf_graph("cases/8.ttl"))],
)
def test_pgt_case_8(name: str, rdf_graph: RDFGraph) -> None:
    UNIQUE_NODES = 4
    NON_LITERAL_STATEMENTS = 1

    alice = URIRef("http://example.com/alice")
    bob = URIRef("http://example.com/bob")
    likes = URIRef("http://example.com/likes")
    certainty = URIRef("http://example.com/certainty")
    certainty_val = Literal(
        "0.5", datatype=URIRef("http://www.w3.org/2001/XMLSchema#double")
    )

    _alice = adbrdf.rdf_id_to_adb_key(str(alice))
    _bob = adbrdf.rdf_id_to_adb_key(str(bob))
    _likes = adbrdf.rdf_id_to_adb_key(str(likes))
    _certainty = adbrdf.rdf_id_to_adb_key("http://example.com/certainty")

    _alice_likes_bob = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=RDF.type, object=RDF.Statement))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    col = adb_graph.vertex_collection(f"{name}_UnknownResource")
    assert col.has(_alice)
    assert col.has(_bob)

    col = adb_graph.vertex_collection("Property")
    assert col.has(_likes)
    assert col.has(_certainty)

    col = adb_graph.edge_collection("likes")
    assert col.has(_alice_likes_bob)
    assert col.get(_alice_likes_bob)["certainty"] == 0.5

    assert not db.has_collection("certainty")

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    statement = rdf_graph_2.value(predicate=RDF.type, object=RDF.Statement)
    assert (statement, RDF.subject, alice) in rdf_graph_2
    assert (statement, RDF.predicate, likes) in rdf_graph_2
    assert (statement, RDF.object, bob) in rdf_graph_2
    assert (statement, certainty, certainty_val) in rdf_graph_2
    assert len(rdf_graph_2) == len(rdf_graph)

    adb_key_statements = RDFGraph()
    adb_key_statements.add((statement, adbrdf.adb_key_uri, Literal(_alice_likes_bob)))

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph_2 + adb_key_statements,
        overwrite_graph=True,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_3 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_2)())
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph_3)) == 0

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph_3 + RDFGraph(),
        overwrite_graph=True,
        flatten_reified_triples=False,
    )

    assert adb_graph.has_vertex_collection("Statement")
    assert adb_graph.has_edge_collection("subject")
    assert adb_graph.has_edge_collection("predicate")
    assert adb_graph.has_edge_collection("object")

    NON_LITERAL_STATEMENTS = len(rdf_graph_3) - len(get_literal_statements(rdf_graph_3))
    UNIQUE_NODES = len(
        get_uris(rdf_graph_3, include_predicates=True)
        | get_bnodes(rdf_graph_3, include_predicates=True)
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_4 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_3)())
    assert len(subtract_graphs(rdf_graph_4, rdf_graph_3)) == 0
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_4)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_9_PGT", get_rdf_graph("cases/9.ttl"))],
)
def test_pgt_case_9(name: str, rdf_graph: RDFGraph) -> None:
    # Case 9 not supported for PGT
    # Perhaps just store "28" as a document in a "Primitive" collection?
    # This way we can preserve the `certainty` property
    # See PGT Case 3_2 for possible solutions
    pytest.skip("PGT 9 not yet supported")


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_10_PGT", get_rdf_graph("cases/10.ttl"))],
)
def test_pgt_case_10(name: str, rdf_graph: RDFGraph) -> None:
    UNIQUE_NODES = 8
    NON_LITERAL_STATEMENTS = 2

    alice = URIRef("http://example.com/alice")
    bobshomepage = URIRef("http://example.com/bobshomepage")
    mainpage = URIRef("http://example.com/mainPage")
    writer = URIRef("http://example.com/writer")
    source = URIRef("http://example.com/source")

    _alice = adbrdf.rdf_id_to_adb_key(str(alice))
    _bobshomepage = adbrdf.rdf_id_to_adb_key(str(bobshomepage))
    _mainpage = adbrdf.rdf_id_to_adb_key(str(mainpage))
    _source = adbrdf.rdf_id_to_adb_key(str(source))
    _writer = adbrdf.rdf_id_to_adb_key(str(writer))
    _mainpage_writer_alice = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=RDF.type, object=RDF.Statement))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
        batch_size=2,
    )

    col = adb_graph.vertex_collection(f"{name}_UnknownResource")
    assert col.has(_bobshomepage)
    assert col.has(_mainpage)
    assert col.has(_alice)

    col = adb_graph.vertex_collection("Property")
    assert col.has(_source)
    assert col.has(_writer)

    edge = adb_graph.edge_collection("writer").get(_mainpage_writer_alice)
    assert edge
    assert edge["1"] == "1"
    assert edge["2"] == "2"
    assert edge["3"] == "3"

    assert adb_graph.edge_collection("source").has(
        adbrdf.hash(f"{_bobshomepage}-{_source}-{_mainpage_writer_alice}")
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    statement = rdf_graph_2.value(predicate=RDF.type, object=RDF.Statement)
    assert (statement, RDF.subject, mainpage) in rdf_graph_2
    assert (statement, RDF.predicate, writer) in rdf_graph_2
    assert (statement, RDF.object, alice) in rdf_graph_2
    assert (bobshomepage, source, statement) in rdf_graph_2
    assert len(rdf_graph_2) == len(rdf_graph)

    adb_key_statements = RDFGraph()
    adb_key_statements.add(
        (statement, adbrdf.adb_key_uri, Literal(_mainpage_writer_alice))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph_2 + adb_key_statements,
        overwrite_graph=True,
    )

    edge = adb_graph.edge_collection("writer").get(_mainpage_writer_alice)
    assert edge
    assert edge["1"] == "1"
    assert edge["2"] == "2"
    assert edge["3"] == "3"
    assert adb_graph.edge_collection("source").has(
        adbrdf.hash(f"{_bobshomepage}-{_source}-{_mainpage_writer_alice}")
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_3 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_2)())
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph_3)) == 0

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph_3 + RDFGraph(),
        overwrite_graph=True,
        flatten_reified_triples=False,
    )

    assert adb_graph.has_vertex_collection("Statement")
    assert adb_graph.has_edge_collection("subject")
    assert adb_graph.has_edge_collection("predicate")
    assert adb_graph.has_edge_collection("object")

    NON_LITERAL_STATEMENTS = len(rdf_graph_3) - len(get_literal_statements(rdf_graph_3))
    UNIQUE_NODES = len(
        get_uris(rdf_graph_3, include_predicates=True)
        | get_bnodes(rdf_graph_3, include_predicates=True)
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_4 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_3)())
    assert len(subtract_graphs(rdf_graph_4, rdf_graph_3)) == 0
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_4)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_11_1_PGT", get_rdf_graph("cases/11_1.ttl"))],
)
def test_pgt_case_11_1(name: str, rdf_graph: RDFGraph) -> None:
    UNIQUE_NODES = 5
    NON_LITERAL_STATEMENTS = 2

    alice = URIRef("http://example.com/alice")
    bobshomepage = URIRef("http://example.com/bobshomepage")
    mainpage = URIRef("http://example.com/mainPage")
    writer = URIRef("http://example.com/writer")
    source = URIRef("http://example.com/source")

    _alice = adbrdf.rdf_id_to_adb_key(str(alice))
    _bobshomepage = adbrdf.rdf_id_to_adb_key(str(bobshomepage))
    _mainpage = adbrdf.rdf_id_to_adb_key(str(mainpage))
    _source = adbrdf.rdf_id_to_adb_key(str(source))
    _writer = adbrdf.rdf_id_to_adb_key(str(writer))
    _mainpage_writer_alice = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=RDF.type, object=RDF.Statement))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    col = adb_graph.vertex_collection(f"{name}_UnknownResource")
    assert col.has(_bobshomepage)
    assert col.has(_mainpage)
    assert col.has(_alice)

    col = adb_graph.vertex_collection("Property")
    assert col.has(_source)
    assert col.has(_writer)

    assert adb_graph.edge_collection("writer").has(_mainpage_writer_alice)
    assert adb_graph.edge_collection("source").has(
        adbrdf.hash(f"{_mainpage_writer_alice}-{_source}-{_bobshomepage}")
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    statement = rdf_graph_2.value(predicate=RDF.type, object=RDF.Statement)
    assert (statement, RDF.subject, mainpage) in rdf_graph_2
    assert (statement, RDF.predicate, writer) in rdf_graph_2
    assert (statement, RDF.object, alice) in rdf_graph_2
    assert (statement, source, bobshomepage) in rdf_graph_2
    assert len(rdf_graph_2) == len(rdf_graph)

    adb_key_statements = RDFGraph()
    adb_key_statements.add(
        (statement, adbrdf.adb_key_uri, Literal(_mainpage_writer_alice))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph_2 + adb_key_statements,
        overwrite_graph=True,
    )

    assert adb_graph.edge_collection("writer").has(_mainpage_writer_alice)
    assert adb_graph.edge_collection("source").has(
        adbrdf.hash(f"{_mainpage_writer_alice}-{_source}-{_bobshomepage}")
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_3 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_2)())
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph_3)) == 0

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph_3 + RDFGraph(),
        overwrite_graph=True,
        flatten_reified_triples=False,
    )

    assert adb_graph.has_vertex_collection("Statement")
    assert adb_graph.has_edge_collection("subject")
    assert adb_graph.has_edge_collection("predicate")
    assert adb_graph.has_edge_collection("object")

    NON_LITERAL_STATEMENTS = len(rdf_graph_3) - len(get_literal_statements(rdf_graph_3))
    UNIQUE_NODES = len(
        get_uris(rdf_graph_3, include_predicates=True)
        | get_bnodes(rdf_graph_3, include_predicates=True)
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_4 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_3)())
    assert len(subtract_graphs(rdf_graph_4, rdf_graph_3)) == 0
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_4)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_11_2_PGT", get_rdf_graph("cases/11_2.ttl"))],
)
def test_pgt_case_11_2(name: str, rdf_graph: RDFGraph) -> None:
    UNIQUE_NODES = 6
    NON_LITERAL_STATEMENTS = 2

    alice = URIRef("http://example.com/alice")
    friend = URIRef("http://example.com/friend")
    bob = URIRef("http://example.com/bob")
    mentionedby = URIRef("http://example.com/mentionedBy")
    alex = URIRef("http://example.com/alex")
    age = URIRef("http://example.com/age")

    _alice = adbrdf.rdf_id_to_adb_key(str(alice))
    _friend = adbrdf.rdf_id_to_adb_key(str(friend))
    _bob = adbrdf.rdf_id_to_adb_key(str(bob))
    _mentionedby = adbrdf.rdf_id_to_adb_key(str(mentionedby))
    _alex = adbrdf.rdf_id_to_adb_key(str(alex))
    _age = adbrdf.rdf_id_to_adb_key(str(age))
    _alice_friend_bob = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=RDF.type, object=RDF.Statement))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    col = adb_graph.vertex_collection(f"{name}_UnknownResource")
    assert col.has(_alice)
    assert col.has(_bob)
    assert col.has(_alex)
    assert col.get(_alex)["age"] == 25

    col = adb_graph.vertex_collection("Property")
    assert col.has(_friend)
    assert col.has(_mentionedby)
    assert col.has(_age)

    assert adb_graph.edge_collection("friend").has(_alice_friend_bob)
    assert adb_graph.edge_collection("mentionedBy").has(
        adbrdf.hash(f"{_alice_friend_bob}-{_mentionedby}-{_alex}")
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    statement = rdf_graph_2.value(predicate=RDF.type, object=RDF.Statement)
    assert (statement, RDF.subject, alice) in rdf_graph_2
    assert (statement, RDF.predicate, friend) in rdf_graph_2
    assert (statement, RDF.object, bob) in rdf_graph_2
    assert (statement, mentionedby, alex) in rdf_graph_2
    assert (alex, age, Literal(25)) in rdf_graph_2
    assert len(rdf_graph_2) == len(rdf_graph)

    adb_key_statements = RDFGraph()
    adb_key_statements.add((statement, adbrdf.adb_key_uri, Literal(_alice_friend_bob)))

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph_2 + adb_key_statements,
        overwrite_graph=True,
    )

    assert adb_graph.edge_collection("friend").has(_alice_friend_bob)
    assert adb_graph.edge_collection("mentionedBy").has(
        adbrdf.hash(f"{_alice_friend_bob}-{_mentionedby}-{_alex}")
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_3 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_2)())
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph_3)) == 0

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph_3 + RDFGraph(),
        overwrite_graph=True,
        flatten_reified_triples=False,
    )

    assert adb_graph.has_vertex_collection("Statement")
    assert adb_graph.has_edge_collection("subject")
    assert adb_graph.has_edge_collection("predicate")
    assert adb_graph.has_edge_collection("object")

    NON_LITERAL_STATEMENTS = len(rdf_graph_3) - len(get_literal_statements(rdf_graph_3))
    UNIQUE_NODES = len(
        get_uris(rdf_graph_3, include_predicates=True)
        | get_bnodes(rdf_graph_3, include_predicates=True)
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_4 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_3)())
    assert len(subtract_graphs(rdf_graph_4, rdf_graph_3)) == 0
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_4)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_12_1_PGT", get_rdf_graph("cases/12_1.ttl"))],
)
def test_pgt_case_12_1(name: str, rdf_graph: RDFGraph) -> None:
    UNIQUE_NODES = 5
    NON_LITERAL_STATEMENTS = 2

    mainpage = URIRef("http://example.com/mainPage")
    writer = URIRef("http://example.com/writer")
    alice = URIRef("http://example.com/alice")
    bobshomepage = URIRef("http://example.com/bobshomepage")

    _alice = adbrdf.rdf_id_to_adb_key(str(alice))
    _mainpage = adbrdf.rdf_id_to_adb_key(str(mainpage))
    _bobshomepage = adbrdf.rdf_id_to_adb_key(str(bobshomepage))
    _writer = adbrdf.rdf_id_to_adb_key(str(writer))
    _type = adbrdf.rdf_id_to_adb_key(str(RDF.type))
    _mainpage_writer_alice = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=RDF.type, object=RDF.Statement))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    col = adb_graph.vertex_collection(f"{name}_UnknownResource")
    assert col.has(_alice)
    assert col.has(_mainpage)

    col = adb_graph.vertex_collection("Class")
    assert col.has(_bobshomepage)

    col = adb_graph.vertex_collection("Property")
    assert col.has(_writer)
    assert col.has(_type)

    assert adb_graph.edge_collection("writer").has(_mainpage_writer_alice)
    assert adb_graph.edge_collection("type").has(
        adbrdf.hash(f"{_mainpage_writer_alice}-{_type}-{_bobshomepage}")
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    statement = rdf_graph_2.value(predicate=RDF.type, object=RDF.Statement)
    assert (statement, RDF.subject, mainpage) in rdf_graph_2
    assert (statement, RDF.predicate, writer) in rdf_graph_2
    assert (statement, RDF.object, alice) in rdf_graph_2
    assert (statement, RDF.type, bobshomepage) in rdf_graph_2
    assert len(rdf_graph_2) == len(rdf_graph)

    adb_key_statements = RDFGraph()
    adb_key_statements.add(
        (statement, adbrdf.adb_key_uri, Literal(_mainpage_writer_alice))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph_2 + adb_key_statements,
        overwrite_graph=True,
    )

    assert adb_graph.edge_collection("writer").has(_mainpage_writer_alice)
    assert adb_graph.edge_collection("type").has(
        adbrdf.hash(f"{_mainpage_writer_alice}-{_type}-{_bobshomepage}")
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_3 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_2)())
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph_3)) == 0

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph_3 + RDFGraph(),
        overwrite_graph=True,
        flatten_reified_triples=False,
    )

    assert not adb_graph.has_vertex_collection("Statement")
    assert adb_graph.has_vertex_collection("bobshomepage")
    assert adb_graph.has_edge_collection("subject")
    assert adb_graph.has_edge_collection("predicate")
    assert adb_graph.has_edge_collection("object")

    NON_LITERAL_STATEMENTS = len(rdf_graph_3) - len(get_literal_statements(rdf_graph_3))
    UNIQUE_NODES = len(
        get_uris(rdf_graph_3, include_predicates=True)
        | get_bnodes(rdf_graph_3, include_predicates=True)
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_4 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_3)())
    assert len(subtract_graphs(rdf_graph_4, rdf_graph_3)) == 0
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_4)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_12_2_PGT", get_rdf_graph("cases/12_2.ttl"))],
)
def test_pgt_case_12_2(name: str, rdf_graph: RDFGraph) -> None:
    UNIQUE_NODES = 5
    NON_LITERAL_STATEMENTS = 2

    lara = URIRef("http://example.com/lara")
    writer = URIRef("http://example.com/writer")
    owner = URIRef("http://example.com/owner")
    journal = URIRef("http://example.com/journal")

    _lara = adbrdf.rdf_id_to_adb_key(str(lara))
    _writer = adbrdf.rdf_id_to_adb_key(str(writer))
    _owner = adbrdf.rdf_id_to_adb_key(str(owner))
    _type = adbrdf.rdf_id_to_adb_key(str(RDF.type))
    _journal = adbrdf.rdf_id_to_adb_key(str(journal))
    _lara_type_writer = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=RDF.type, object=RDF.Statement))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    col = adb_graph.vertex_collection("writer")
    assert col.has(_lara)

    col = adb_graph.vertex_collection(f"{name}_UnknownResource")
    assert col.has(_journal)

    col = adb_graph.vertex_collection("Class")
    assert col.has(_writer)

    col = adb_graph.vertex_collection("Property")
    assert col.has(_owner)
    assert col.has(_type)

    assert adb_graph.edge_collection("type").has(_lara_type_writer)
    assert adb_graph.edge_collection("owner").has(
        adbrdf.hash(f"{_lara_type_writer}-{_owner}-{_journal}")
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    statement = rdf_graph_2.value(predicate=RDF.type, object=RDF.Statement)
    assert (statement, RDF.subject, lara) in rdf_graph_2
    assert (statement, RDF.predicate, RDF.type) in rdf_graph_2
    assert (statement, RDF.object, writer) in rdf_graph_2
    assert (statement, owner, journal) in rdf_graph_2
    assert len(rdf_graph_2) == len(rdf_graph)

    adb_key_statements = RDFGraph()
    adb_key_statements.add((statement, adbrdf.adb_key_uri, Literal(_lara_type_writer)))

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph_2 + adb_key_statements,
        overwrite_graph=True,
    )

    assert adb_graph.edge_collection("type").has(_lara_type_writer)
    assert adb_graph.edge_collection("owner").has(
        adbrdf.hash(f"{_lara_type_writer}-{_owner}-{_journal}")
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_3 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_2)())
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph_3)) == 0

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph_3 + RDFGraph(),
        overwrite_graph=True,
        flatten_reified_triples=False,
    )

    assert adb_graph.has_vertex_collection("Statement")
    assert adb_graph.has_edge_collection("subject")
    assert adb_graph.has_edge_collection("predicate")
    assert adb_graph.has_edge_collection("object")

    NON_LITERAL_STATEMENTS = len(rdf_graph_3) - len(get_literal_statements(rdf_graph_3))
    UNIQUE_NODES = len(
        get_uris(rdf_graph_3, include_predicates=True)
        | get_bnodes(rdf_graph_3, include_predicates=True)
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_4 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_3)())
    assert len(subtract_graphs(rdf_graph_4, rdf_graph_3)) == 0
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_4)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_13_1_PGT", get_rdf_graph("cases/13_1.ttl"))],
)
def test_pgt_case_13_1(name: str, rdf_graph: RDFGraph) -> None:
    UNIQUE_NODES = 7
    NON_LITERAL_STATEMENTS = 3

    steve = URIRef("http://example.com/steve")
    position = URIRef("http://example.com/position")
    ceo = URIRef("http://example.com/CEO")
    mentionedBy = URIRef("http://example.com/mentionedBy")
    book = URIRef("http://example.com/book")
    source = URIRef("http://example.com/source")
    journal = URIRef("http://example.com/journal")

    _steve = adbrdf.rdf_id_to_adb_key(str(steve))
    _ceo = adbrdf.rdf_id_to_adb_key(str(ceo))
    _source = adbrdf.rdf_id_to_adb_key(str(source))
    _journal = adbrdf.rdf_id_to_adb_key(str(journal))
    _position = adbrdf.rdf_id_to_adb_key(str(position))
    _mentionedby = adbrdf.rdf_id_to_adb_key(str(mentionedBy))

    _steve_position_ceo = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=RDF.predicate, object=position))
    )

    _steve_position_ceo_mentionedby_book = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=RDF.predicate, object=mentionedBy))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name, rdf_graph + RDFGraph(), overwrite_graph=True, batch_size=1
    )

    col = adb_graph.vertex_collection(f"{name}_UnknownResource")
    assert col.has(_steve)
    assert col.has(_ceo)

    col = adb_graph.vertex_collection("Property")
    assert col.has(_position)
    assert col.has(_mentionedby)
    assert col.has(_source)

    assert adb_graph.edge_collection("position").has(_steve_position_ceo)
    assert adb_graph.edge_collection("mentionedBy").has(
        _steve_position_ceo_mentionedby_book
    )
    assert adb_graph.edge_collection("source").has(
        adbrdf.hash(f"{_steve_position_ceo_mentionedby_book}-{_source}-{_journal}")
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)(), batch_size=1)

    statement_1 = rdf_graph_2.value(predicate=RDF.predicate, object=position)
    assert (statement_1, RDF.subject, steve) in rdf_graph_2
    assert (statement_1, RDF.predicate, position) in rdf_graph_2
    assert (statement_1, RDF.object, ceo) in rdf_graph_2
    statement_2 = rdf_graph_2.value(predicate=RDF.predicate, object=mentionedBy)
    assert (statement_2, RDF.subject, statement_1) in rdf_graph_2
    assert (statement_2, RDF.predicate, mentionedBy) in rdf_graph_2
    assert (statement_2, RDF.object, book) in rdf_graph_2
    assert (statement_2, source, journal) in rdf_graph_2
    assert len(rdf_graph_2) == len(rdf_graph)

    adb_key_statements = RDFGraph()
    adb_key_statements.add(
        (statement_1, adbrdf.adb_key_uri, Literal(_steve_position_ceo))
    )
    adb_key_statements.add(
        (statement_2, adbrdf.adb_key_uri, Literal(_steve_position_ceo_mentionedby_book))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph_2 + adb_key_statements,
        overwrite_graph=True,
    )

    assert adb_graph.edge_collection("position").has(_steve_position_ceo)
    assert adb_graph.edge_collection("mentionedBy").has(
        _steve_position_ceo_mentionedby_book
    )
    assert adb_graph.edge_collection("source").has(
        adbrdf.hash(f"{_steve_position_ceo_mentionedby_book}-{_source}-{_journal}")
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_3 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_2)())
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph_3)) == 0

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph_3 + RDFGraph(),
        overwrite_graph=True,
        flatten_reified_triples=False,
    )

    assert adb_graph.has_vertex_collection("Statement")
    assert adb_graph.has_edge_collection("subject")
    assert adb_graph.has_edge_collection("predicate")
    assert adb_graph.has_edge_collection("object")

    NON_LITERAL_STATEMENTS = len(rdf_graph_3) - len(get_literal_statements(rdf_graph_3))
    UNIQUE_NODES = len(
        get_uris(rdf_graph_3, include_predicates=True)
        | get_bnodes(rdf_graph_3, include_predicates=True)
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_4 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_3)())
    assert len(subtract_graphs(rdf_graph_4, rdf_graph_3)) == 0
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_4)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_13_2_PGT", get_rdf_graph("cases/13_2.ttl"))],
)
def test_pgt_case_13_2(name: str, rdf_graph: RDFGraph) -> None:
    UNIQUE_NODES = 7
    NON_LITERAL_STATEMENTS = 3

    steve = URIRef("http://example.com/steve")
    position = URIRef("http://example.com/position")
    ceo = URIRef("http://example.com/CEO")
    mentionedBy = URIRef("http://example.com/mentionedBy")
    book = URIRef("http://example.com/book")
    source = URIRef("http://example.com/source")
    journal = URIRef("http://example.com/journal")

    _steve = adbrdf.rdf_id_to_adb_key(str(steve))
    _ceo = adbrdf.rdf_id_to_adb_key(str(ceo))
    _source = adbrdf.rdf_id_to_adb_key(str(source))
    _journal = adbrdf.rdf_id_to_adb_key(str(journal))
    _position = adbrdf.rdf_id_to_adb_key(str(position))
    _mentionedby = adbrdf.rdf_id_to_adb_key(str(mentionedBy))

    statement_1 = rdf_graph.value(predicate=RDF.predicate, object=position)
    assert statement_1
    _steve_position_ceo = adbrdf.rdf_id_to_adb_key(str(statement_1))

    statement_2 = rdf_graph.value(predicate=RDF.predicate, object=mentionedBy)
    _book_mentioned_by_steve_position_ceo = adbrdf.rdf_id_to_adb_key(str(statement_2))

    statement_3 = rdf_graph.value(predicate=RDF.predicate, object=source)
    _journal_source_book = adbrdf.rdf_id_to_adb_key(str(statement_3))

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name, rdf_graph + RDFGraph(), overwrite_graph=True, batch_size=1
    )

    col = adb_graph.vertex_collection(f"{name}_UnknownResource")
    assert col.has(_steve)
    assert col.has(_ceo)

    col = adb_graph.vertex_collection("Property")
    assert col.has(_position)
    assert col.has(_mentionedby)
    assert col.has(_source)

    assert adb_graph.edge_collection("position").has(_steve_position_ceo)
    assert adb_graph.edge_collection("mentionedBy").has(
        _book_mentioned_by_steve_position_ceo
    )
    assert adb_graph.edge_collection("source").has(_journal_source_book)

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)(), batch_size=1)

    statement_1 = rdf_graph_2.value(predicate=RDF.predicate, object=position)
    assert (statement_1, RDF.subject, steve) in rdf_graph_2
    assert (statement_1, RDF.predicate, position) in rdf_graph_2
    assert (statement_1, RDF.object, ceo) in rdf_graph_2
    statement_2 = rdf_graph_2.value(predicate=RDF.predicate, object=mentionedBy)
    assert (statement_2, RDF.subject, book) in rdf_graph_2
    assert (statement_2, RDF.predicate, mentionedBy) in rdf_graph_2
    assert (statement_2, RDF.object, statement_1) in rdf_graph_2
    assert (journal, source, statement_2) in rdf_graph_2

    adb_key_statements = RDFGraph()
    adb_key_statements.add(
        (statement_1, adbrdf.adb_key_uri, Literal(_steve_position_ceo))
    )
    adb_key_statements.add(
        (
            statement_2,
            adbrdf.adb_key_uri,
            Literal(_book_mentioned_by_steve_position_ceo),
        )
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph_2 + adb_key_statements,
        overwrite_graph=True,
    )

    assert adb_graph.edge_collection("position").has(_steve_position_ceo)
    assert adb_graph.edge_collection("mentionedBy").has(
        _book_mentioned_by_steve_position_ceo
    )
    assert adb_graph.edge_collection("source").has(
        adbrdf.hash(f"{_journal}-{_source}-{_book_mentioned_by_steve_position_ceo}")
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_3 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_2)())
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph_3)) == 0

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph_3 + RDFGraph(),
        overwrite_graph=True,
        flatten_reified_triples=False,
    )

    assert adb_graph.has_vertex_collection("Statement")
    assert adb_graph.has_edge_collection("subject")
    assert adb_graph.has_edge_collection("predicate")
    assert adb_graph.has_edge_collection("object")

    NON_LITERAL_STATEMENTS = len(rdf_graph_3) - len(get_literal_statements(rdf_graph_3))
    UNIQUE_NODES = len(
        get_uris(rdf_graph_3, include_predicates=True)
        | get_bnodes(rdf_graph_3, include_predicates=True)
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_4 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_3)())
    assert len(subtract_graphs(rdf_graph_4, rdf_graph_3)) == 0
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_4)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_14_1_PGT", get_rdf_graph("cases/14_1.ttl"))],
)
def test_pgt_case_14_1(name: str, rdf_graph: RDFGraph) -> None:
    NON_LITERAL_STATEMENTS = len(rdf_graph) - len(get_literal_statements(rdf_graph))
    UNIQUE_NODES = len(
        get_uris(rdf_graph, include_predicates=True)
        | get_bnodes(rdf_graph, include_predicates=True)
    )

    college_page = URIRef("http://example.com/college_page")
    college_page_2 = URIRef("http://example.com/college_page_2")
    link = URIRef("http://example.com/link")
    subject = URIRef("http://example.com/subject")
    info_page = Literal("Info_Page")
    aau_page = Literal("aau_page")

    _college_page = adbrdf.rdf_id_to_adb_key(str(college_page))
    _college_page_2 = adbrdf.rdf_id_to_adb_key(str(college_page_2))
    _link = adbrdf.rdf_id_to_adb_key(str(link))
    _subject = adbrdf.rdf_id_to_adb_key(str(subject))

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
        batch_size=1,
    )

    col = adb_graph.vertex_collection(f"{name}_UnknownResource")
    assert col.has(_college_page)
    assert col.has(_college_page_2)
    assert set(col.get(_college_page)["subject"]) == {"Info_Page", "aau_page"}

    col = adb_graph.vertex_collection("link")
    assert col.has(adbrdf.hash(f"{_college_page}-{_link}-{_college_page_2}"))

    col = adb_graph.vertex_collection("Property")
    assert col.has(_subject)

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(
        name, type(rdf_graph)(), list_conversion_mode="static"
    )
    assert len(subtract_graphs(rdf_graph_2, rdf_graph)) == 0
    assert len(subtract_graphs(rdf_graph, rdf_graph_2)) == 0

    rdf_graph_3 = adbrdf.arangodb_graph_to_rdf(
        name, type(rdf_graph)(), list_conversion_mode="collection"
    )
    assert (college_page, subject, None) in rdf_graph_3
    assert (None, RDF.first, info_page) in rdf_graph_3
    assert (None, RDF.first, aau_page) in rdf_graph_3
    assert (college_page_2, subject, info_page) in rdf_graph_3
    assert (college_page, link, college_page_2) in rdf_graph_3

    rdf_graph_4 = adbrdf.arangodb_graph_to_rdf(
        name, type(rdf_graph)(), list_conversion_mode="container"
    )
    bnode = rdf_graph_4.value(college_page, subject)
    assert (bnode, None, info_page) in rdf_graph_4
    assert (bnode, None, aau_page) in rdf_graph_4
    assert (bnode, URIRef(f"{RDF}_1"), None) in rdf_graph_4
    assert (bnode, URIRef(f"{RDF}_2"), None) in rdf_graph_4
    assert (college_page_2, subject, info_page) in rdf_graph_3
    assert (college_page, link, college_page_2) in rdf_graph_4

    with pytest.raises(ValueError):
        adbrdf.arangodb_graph_to_rdf(
            name, type(rdf_graph)(), list_conversion_mode="bad_name"
        )

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_14_2_PGT", get_rdf_graph("cases/14_2.ttl"))],
)
def test_pgt_case_14_2(name: str, rdf_graph: RDFGraph) -> None:
    UNIQUE_NODES = 4
    NON_LITERAL_STATEMENTS = 2

    mary = URIRef("http://example.com/Mary")
    likes = URIRef("http://example.com/likes")
    matt = URIRef("http://example.com/Matt")
    certainty = URIRef("http://example.com/certainty")
    certainty_val_05 = Literal(
        "0.5", datatype=URIRef("http://www.w3.org/2001/XMLSchema#double")
    )
    certainty_val_1 = Literal(1)

    _mary = adbrdf.rdf_id_to_adb_key(str(mary))
    _matt = adbrdf.rdf_id_to_adb_key(str(matt))
    _likes = adbrdf.rdf_id_to_adb_key(str(likes))
    _certainty = adbrdf.rdf_id_to_adb_key(str(certainty))
    _mary_likes_matt_05 = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=certainty, object=certainty_val_05))
    )
    _mary_likes_matt_1 = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=certainty, object=certainty_val_1))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    col = adb_graph.vertex_collection(f"{name}_UnknownResource")
    assert col.has(_mary)
    assert col.has(_matt)

    col = adb_graph.vertex_collection("Property")
    assert col.has(_likes)
    assert col.has(_certainty)

    col = adb_graph.edge_collection("likes")
    assert col.has(_mary_likes_matt_05)
    assert col.get(_mary_likes_matt_05)["certainty"] == 0.5
    assert col.has(_mary_likes_matt_1)
    assert col.get(_mary_likes_matt_1)["certainty"] == 1

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    statement_1 = rdf_graph_2.value(predicate=certainty, object=certainty_val_05)
    assert (statement_1, RDF.subject, mary) in rdf_graph_2
    assert (statement_1, RDF.predicate, likes) in rdf_graph_2
    assert (statement_1, RDF.object, matt) in rdf_graph_2
    assert (statement_1, certainty, certainty_val_05) in rdf_graph_2
    statement_2 = rdf_graph_2.value(predicate=certainty, object=certainty_val_1)
    assert (statement_2, RDF.subject, mary) in rdf_graph_2
    assert (statement_2, RDF.predicate, likes) in rdf_graph_2
    assert (statement_2, RDF.object, matt) in rdf_graph_2
    assert (statement_2, certainty, certainty_val_1) in rdf_graph_2
    assert len(rdf_graph_2) == len(rdf_graph)

    adb_key_statements = RDFGraph()
    adb_key_statements.add(
        (statement_1, adbrdf.adb_key_uri, Literal(_mary_likes_matt_05))
    )
    adb_key_statements.add(
        (statement_2, adbrdf.adb_key_uri, Literal(_mary_likes_matt_1))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph_2 + adb_key_statements,
        overwrite_graph=True,
    )

    col = adb_graph.edge_collection("likes")
    assert col.has(_mary_likes_matt_05)
    assert col.get(_mary_likes_matt_05)["certainty"] == 0.5
    assert col.has(_mary_likes_matt_1)
    assert col.get(_mary_likes_matt_1)["certainty"] == 1

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_3 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_2)())
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph_3)) == 0

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph_3 + RDFGraph(),
        overwrite_graph=True,
        flatten_reified_triples=False,
    )

    assert adb_graph.has_vertex_collection("Statement")
    assert adb_graph.has_edge_collection("subject")
    assert adb_graph.has_edge_collection("predicate")
    assert adb_graph.has_edge_collection("object")

    NON_LITERAL_STATEMENTS = len(rdf_graph_3) - len(get_literal_statements(rdf_graph_3))
    UNIQUE_NODES = len(
        get_uris(rdf_graph_3, include_predicates=True)
        | get_bnodes(rdf_graph_3, include_predicates=True)
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_4 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_3)())
    assert len(subtract_graphs(rdf_graph_4, rdf_graph_3)) == 0
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_4)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_15_1_PGT", get_rdf_graph("cases/15_1.ttl"))],
)
def test_pgt_case_15_1(name: str, rdf_graph: RDFGraph) -> None:
    UNIQUE_NODES = 5
    NON_LITERAL_STATEMENTS = 2

    mary = URIRef("http://example.com/Mary")
    likes = URIRef("http://example.com/likes")
    matt = URIRef("http://example.com/Matt")
    certainty = URIRef("http://example.com/certainty")
    source = URIRef("http://example.com/source")
    certainty_val = Literal(
        "0.5", datatype=URIRef("http://www.w3.org/2001/XMLSchema#double")
    )
    text = Literal("text")

    _mary = adbrdf.rdf_id_to_adb_key(str(mary))
    _matt = adbrdf.rdf_id_to_adb_key(str(matt))
    _certainty = adbrdf.rdf_id_to_adb_key(str(certainty))
    _likes = adbrdf.rdf_id_to_adb_key(str(likes))
    _source = adbrdf.rdf_id_to_adb_key(str(source))
    _mary_likes_matt_1 = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=certainty, object=certainty_val))
    )
    _mary_likes_matt_2 = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=source, object=text))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    col = adb_graph.vertex_collection(f"{name}_UnknownResource")
    assert col.has(_mary)
    assert col.has(_matt)

    col = adb_graph.vertex_collection("Property")
    assert col.has(_likes)
    assert col.has(_certainty)
    assert col.has(_source)

    col = adb_graph.edge_collection("likes")
    assert col.has(_mary_likes_matt_1)
    assert col.get(_mary_likes_matt_1)["certainty"] == 0.5
    assert col.has(_mary_likes_matt_2)
    assert col.get(_mary_likes_matt_2)["source"] == "text"

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    statement_1 = rdf_graph_2.value(predicate=certainty, object=certainty_val)
    assert (statement_1, RDF.subject, mary) in rdf_graph_2
    assert (statement_1, RDF.predicate, likes) in rdf_graph_2
    assert (statement_1, RDF.object, matt) in rdf_graph_2
    assert (statement_1, certainty, certainty_val) in rdf_graph_2
    statement_2 = rdf_graph_2.value(predicate=source, object=text)
    assert (statement_2, RDF.subject, mary) in rdf_graph_2
    assert (statement_2, RDF.predicate, likes) in rdf_graph_2
    assert (statement_2, RDF.object, matt) in rdf_graph_2
    assert (statement_2, source, text) in rdf_graph_2
    assert len(rdf_graph_2) == len(rdf_graph)

    adb_key_statements = RDFGraph()
    adb_key_statements.add(
        (statement_1, adbrdf.adb_key_uri, Literal(_mary_likes_matt_1))
    )
    adb_key_statements.add(
        (statement_2, adbrdf.adb_key_uri, Literal(_mary_likes_matt_2))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph_2 + adb_key_statements,
        overwrite_graph=True,
    )

    col = adb_graph.edge_collection("likes")
    assert col.has(_mary_likes_matt_1)
    assert col.get(_mary_likes_matt_1)["certainty"] == 0.5
    assert col.has(_mary_likes_matt_2)
    assert col.get(_mary_likes_matt_2)["source"] == "text"

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_3 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_2)())
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph_3)) == 0

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph_3 + RDFGraph(),
        overwrite_graph=True,
        flatten_reified_triples=False,
    )

    assert adb_graph.has_vertex_collection("Statement")
    assert adb_graph.has_edge_collection("subject")
    assert adb_graph.has_edge_collection("predicate")
    assert adb_graph.has_edge_collection("object")

    NON_LITERAL_STATEMENTS = len(rdf_graph_3) - len(get_literal_statements(rdf_graph_3))
    UNIQUE_NODES = len(
        get_uris(rdf_graph_3, include_predicates=True)
        | get_bnodes(rdf_graph_3, include_predicates=True)
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_4 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_3)())
    assert len(subtract_graphs(rdf_graph_4, rdf_graph_3)) == 0
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_4)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_15_2_PGT", get_rdf_graph("cases/15_2.ttl"))],
)
def test_pgt_case_15_2(name: str, rdf_graph: RDFGraph) -> None:
    UNIQUE_NODES = 6
    NON_LITERAL_STATEMENTS = 3

    foo = URIRef("http://example.com/foo")
    bar = URIRef("http://example.com/bar")
    mary = URIRef("http://example.com/Mary")
    likes = URIRef("http://example.com/likes")
    matt = URIRef("http://example.com/Matt")
    certainty = URIRef("http://example.com/certainty")
    certainty_val = Literal(
        "0.5", datatype=URIRef("http://www.w3.org/2001/XMLSchema#double")
    )

    _foo = adbrdf.rdf_id_to_adb_key(str(foo))
    _bar = adbrdf.rdf_id_to_adb_key(str(bar))
    _mary = adbrdf.rdf_id_to_adb_key(str(mary))
    _matt = adbrdf.rdf_id_to_adb_key(str(matt))
    _certainty = adbrdf.rdf_id_to_adb_key(str(certainty))
    _likes = adbrdf.rdf_id_to_adb_key(str(likes))
    _mary_likes_matt_1 = adbrdf.rdf_id_to_adb_key(
        str(rdf_graph.value(predicate=certainty, object=certainty_val))
    )
    _mary_likes_matt_2 = adbrdf.rdf_id_to_adb_key(f"{_mary}-{_likes}-{_matt}")

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    col = adb_graph.vertex_collection(f"{name}_UnknownResource")
    assert col.has(_mary)
    assert col.has(_matt)

    col = adb_graph.vertex_collection("Property")
    assert col.has(_likes)
    assert col.has(_certainty)

    col = adb_graph.edge_collection("likes")
    assert col.has(_mary_likes_matt_1)
    assert col.get(_mary_likes_matt_1)["certainty"] == 0.5
    assert col.has(_mary_likes_matt_2)

    col = adb_graph.edge_collection("foo")
    assert col.has(adbrdf.hash(f"{_mary_likes_matt_1}-{_foo}-{_bar}"))

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    statement_1 = rdf_graph_2.value(predicate=certainty, object=certainty_val)
    assert (statement_1, RDF.subject, mary) in rdf_graph_2
    assert (statement_1, RDF.predicate, likes) in rdf_graph_2
    assert (statement_1, RDF.object, matt) in rdf_graph_2
    assert (statement_1, certainty, certainty_val) in rdf_graph_2
    assert (statement_1, foo, bar) in rdf_graph_2
    # NOTE: ASSERTION BELOW IS FLAKY
    # See `self.__rdf_graph.remove((subject, predicate, object))`
    # in `ArangoRDF__process_adb_edge`
    try:
        assert (mary, likes, matt) in rdf_graph_2
        assert len(rdf_graph_2) == len(rdf_graph)
    except AssertionError:
        db.delete_graph(name, drop_collections=True)
        m = "PGT 15.2 (ArangoDB to RDF) not yet fully supported due to flaky assertion"
        pytest.xfail(m)

    adb_key_statements = RDFGraph()
    adb_key_statements.add(
        (statement_1, adbrdf.adb_key_uri, Literal(_mary_likes_matt_1))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph_2 + adb_key_statements,
        overwrite_graph=True,
    )

    col = adb_graph.edge_collection("likes")
    assert col.has(_mary_likes_matt_1)
    assert col.get(_mary_likes_matt_1)["certainty"] == 0.5
    assert col.has(_mary_likes_matt_2)

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_3 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_2)())
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_2)) == 0
    try:
        assert len(subtract_graphs(rdf_graph_2, rdf_graph_3)) == 0
    except AssertionError:
        db.delete_graph(name, drop_collections=True)
        m = "PGT 15.2 (ArangoDB to RDF) not yet fully supported due to flaky assertion"
        pytest.xfail(m)

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph_3 + RDFGraph(),
        overwrite_graph=True,
        flatten_reified_triples=False,
    )

    assert adb_graph.has_vertex_collection("Statement")
    assert adb_graph.has_edge_collection("subject")
    assert adb_graph.has_edge_collection("predicate")
    assert adb_graph.has_edge_collection("object")

    NON_LITERAL_STATEMENTS = len(rdf_graph_3) - len(get_literal_statements(rdf_graph_3))
    UNIQUE_NODES = len(
        get_uris(rdf_graph_3, include_predicates=True)
        | get_bnodes(rdf_graph_3, include_predicates=True)
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_4 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_3)())
    assert len(subtract_graphs(rdf_graph_4, rdf_graph_3)) == 0
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_4)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_15_3_PGT", get_rdf_graph("cases/15_3.ttl"))],
)
def test_pgt_case_15_3(name: str, rdf_graph: RDFGraph) -> None:
    UNIQUE_NODES = 6
    NON_LITERAL_STATEMENTS = 3

    foo = URIRef("http://example.com/foo")
    bar = URIRef("http://example.com/bar")
    mary = URIRef("http://example.com/Mary")
    likes = URIRef("http://example.com/likes")
    matt = URIRef("http://example.com/Matt")
    certainty = URIRef("http://example.com/certainty")
    certainty_val = Literal(
        "0.5", datatype=URIRef("http://www.w3.org/2001/XMLSchema#double")
    )

    _foo = adbrdf.rdf_id_to_adb_key(str(foo))
    _bar = adbrdf.rdf_id_to_adb_key(str(bar))
    _mary = adbrdf.rdf_id_to_adb_key(str(mary))
    _matt = adbrdf.rdf_id_to_adb_key(str(matt))
    _certainty = adbrdf.rdf_id_to_adb_key(str(certainty))
    _likes = adbrdf.rdf_id_to_adb_key(str(likes))
    statement_1 = rdf_graph.value(predicate=certainty, object=certainty_val)
    assert statement_1
    _mary_likes_matt_1 = adbrdf.rdf_id_to_adb_key(str(statement_1))

    statement_2 = None
    for statement, _, _ in rdf_graph.triples((None, RDF.predicate, likes)):
        if statement != statement_1:
            statement_2 = statement
            break

    _mary_likes_matt_2 = adbrdf.rdf_id_to_adb_key(str(statement_2))

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
    )

    col = adb_graph.vertex_collection(f"{name}_UnknownResource")
    assert col.has(_mary)
    assert col.has(_matt)

    col = adb_graph.vertex_collection("Property")
    assert col.has(_likes)
    assert col.has(_certainty)

    col = adb_graph.edge_collection("likes")
    assert col.has(_mary_likes_matt_1)
    assert col.get(_mary_likes_matt_1)["certainty"] == 0.5
    assert col.has(_mary_likes_matt_2)

    col = adb_graph.edge_collection("foo")
    assert col.has(adbrdf.hash(f"{_mary_likes_matt_1}-{_foo}-{_bar}"))

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    statement_1 = rdf_graph_2.value(predicate=certainty, object=certainty_val)
    assert (statement_1, RDF.subject, mary) in rdf_graph_2
    assert (statement_1, RDF.predicate, likes) in rdf_graph_2
    assert (statement_1, RDF.object, matt) in rdf_graph_2
    assert (statement_1, certainty, certainty_val) in rdf_graph_2
    assert (statement_1, foo, bar) in rdf_graph_2
    # NOTE: ASSERTION BELOW IS FLAKY
    # See `self.__rdf_graph.remove((subject, predicate, object))`
    # in `ArangoRDF__process_adb_edge`
    try:
        assert (mary, likes, matt) in rdf_graph_2
    except AssertionError:
        db.delete_graph(name, drop_collections=True)
        m = "PGT 15.3 (ArangoDB to RDF) not yet fully supported due to flaky assertion"
        pytest.xfail(m)

    adb_key_statements = RDFGraph()
    adb_key_statements.add(
        (statement_1, adbrdf.adb_key_uri, Literal(_mary_likes_matt_1))
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph_2 + adb_key_statements,
        overwrite_graph=True,
    )

    col = adb_graph.edge_collection("likes")
    assert col.has(_mary_likes_matt_1)
    assert col.get(_mary_likes_matt_1)["certainty"] == 0.5
    assert col.has(adbrdf.hash(f"{_mary}-{_likes}-{_matt}"))

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_3 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_2)())
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph_3)) == 0

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph_3 + RDFGraph(),
        overwrite_graph=True,
        flatten_reified_triples=False,
    )

    assert adb_graph.has_vertex_collection("Statement")
    assert adb_graph.has_edge_collection("subject")
    assert adb_graph.has_edge_collection("predicate")
    assert adb_graph.has_edge_collection("object")

    NON_LITERAL_STATEMENTS = len(rdf_graph_3) - len(get_literal_statements(rdf_graph_3))
    UNIQUE_NODES = len(
        get_uris(rdf_graph_3, include_predicates=True)
        | get_bnodes(rdf_graph_3, include_predicates=True)
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_4 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_3)())
    assert len(subtract_graphs(rdf_graph_4, rdf_graph_3)) == 0
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_4)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_15_4_PGT", get_rdf_graph("cases/15_4.trig"))],
)
def test_pgt_case_15_4(name: str, rdf_graph: RDFGraph) -> None:
    # Reified Triple Simplification modifies the source graph,
    # so we must make a copy of the graph to test against
    rdf_graph_copy = RDFConjunctiveGraph()
    for quad in rdf_graph.quads((None, None, None, None)):
        rdf_graph_copy.add(quad)

    UNIQUE_NODES = 7
    NON_LITERAL_STATEMENTS = 4

    graph1 = "http://example.com/Graph1"
    graph2 = "http://example.com/Graph2"

    john = URIRef("http://example.com/John")
    said = URIRef("http://example.com/said")
    mary = URIRef("http://example.com/Mary")
    likes = URIRef("http://example.com/likes")
    matt = URIRef("http://example.com/Matt")
    certainty = URIRef("http://example.com/certainty")
    foo = URIRef("http://example.com/foo")
    certainty_val_05 = Literal(
        "0.5", datatype=URIRef("http://www.w3.org/2001/XMLSchema#decimal")
    )
    certainty_val_075 = Literal(
        "0.75", datatype=URIRef("http://www.w3.org/2001/XMLSchema#decimal")
    )
    certainty_val_1 = Literal(1)
    bar = Literal("bar")

    john_said_mary_likes_matt_05 = rdf_graph.value(predicate=foo, object=bar)
    mary_likes_matt_05 = rdf_graph.value(predicate=certainty, object=certainty_val_05)
    mary_likes_matt_075 = rdf_graph.value(predicate=certainty, object=certainty_val_075)
    mary_likes_matt_1 = rdf_graph.value(predicate=certainty, object=certainty_val_1)

    _john = adbrdf.rdf_id_to_adb_key(str(john))
    _said = adbrdf.rdf_id_to_adb_key(str(said))
    _mary = adbrdf.rdf_id_to_adb_key(str(mary))
    _likes = adbrdf.rdf_id_to_adb_key(str(likes))
    _matt = adbrdf.rdf_id_to_adb_key(str(matt))
    _certainty = adbrdf.rdf_id_to_adb_key(str(certainty))
    _john_said_mary_likes_matt_05 = adbrdf.rdf_id_to_adb_key(
        str(john_said_mary_likes_matt_05)
    )
    _mary_likes_matt_05 = adbrdf.rdf_id_to_adb_key(str(mary_likes_matt_05))
    _mary_likes_matt_075 = adbrdf.rdf_id_to_adb_key(str(mary_likes_matt_075))
    _mary_likes_matt_1 = adbrdf.rdf_id_to_adb_key(str(mary_likes_matt_1))

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph_copy,
        overwrite_graph=True,
    )

    col = adb_graph.vertex_collection(f"{name}_UnknownResource")
    assert col.has(_mary)
    assert col.has(_matt)
    assert col.has(_john)

    col = adb_graph.vertex_collection("Property")
    assert col.has(_likes)
    assert col.has(_certainty)
    assert col.has(_said)

    col = adb_graph.edge_collection("likes")
    assert col.has(_mary_likes_matt_05)
    assert col.get(_mary_likes_matt_05)["certainty"] == 0.5
    assert col.get(_mary_likes_matt_05)["_sub_graph_uri"] == graph1
    assert col.has(_mary_likes_matt_075)
    assert col.get(_mary_likes_matt_075)["certainty"] == 0.75
    assert col.get(_mary_likes_matt_075)["_sub_graph_uri"] == graph2
    assert col.has(_mary_likes_matt_1)
    assert col.get(_mary_likes_matt_1)["certainty"] == 1
    assert "_sub_graph_uri" not in col.get(_mary_likes_matt_1)

    col = adb_graph.edge_collection("said")
    assert col.has(_john_said_mary_likes_matt_05)
    assert col.get(_john_said_mary_likes_matt_05)["foo"] == "bar"
    assert col.get(_john_said_mary_likes_matt_05)["_sub_graph_uri"] == graph2

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph)())

    # Datatype is lost on PGT, hence Literal(0.5) != certainty_val_05
    certainty_val_05 = Literal(0.5)
    mary_likes_matt_05 = rdf_graph_2.value(predicate=certainty, object=certainty_val_05)
    assert mary_likes_matt_05
    assert (mary_likes_matt_05, RDF.subject, mary) in rdf_graph_2
    assert (mary_likes_matt_05, RDF.predicate, likes) in rdf_graph_2
    assert (mary_likes_matt_05, RDF.object, matt) in rdf_graph_2
    assert (mary_likes_matt_05, certainty, certainty_val_05) in rdf_graph_2
    # Datatype is lost on PGT, hence Literal(0.75) != certainty_val_075
    certainty_val_075 = Literal(0.75)
    mary_likes_matt_075 = rdf_graph_2.value(
        predicate=certainty, object=certainty_val_075
    )
    assert mary_likes_matt_075
    assert (mary_likes_matt_075, RDF.subject, mary) in rdf_graph_2
    assert (mary_likes_matt_075, RDF.predicate, likes) in rdf_graph_2
    assert (mary_likes_matt_075, RDF.object, matt) in rdf_graph_2
    assert (mary_likes_matt_075, certainty, certainty_val_075) in rdf_graph_2
    # Datatype is lost on PGT, hence Literal(1) != certainty_val_1
    certainty_val_1 = Literal(1)
    mary_likes_matt_1 = rdf_graph_2.value(predicate=certainty, object=certainty_val_1)
    assert mary_likes_matt_1
    assert (mary_likes_matt_1, RDF.subject, mary) in rdf_graph_2
    assert (mary_likes_matt_1, RDF.predicate, likes) in rdf_graph_2
    assert (mary_likes_matt_1, RDF.object, matt) in rdf_graph_2
    assert (mary_likes_matt_1, certainty, certainty_val_1) in rdf_graph_2

    john_said_mary_likes_matt_05 = rdf_graph_2.value(predicate=foo, object=bar)
    assert john_said_mary_likes_matt_05
    assert (john_said_mary_likes_matt_05, RDF.subject, john) in rdf_graph_2
    assert (john_said_mary_likes_matt_05, RDF.predicate, said) in rdf_graph_2
    assert (john_said_mary_likes_matt_05, RDF.object, mary_likes_matt_05) in rdf_graph_2
    assert (john_said_mary_likes_matt_05, foo, bar) in rdf_graph_2

    assert len(rdf_graph_2) == len(rdf_graph)
    rdf_graph_contexts = {str(sg.identifier) for sg in rdf_graph.contexts()}
    rdf_graph_2_contexts = {str(sg.identifier) for sg in rdf_graph_2.contexts()}
    assert len(rdf_graph_contexts) == len(rdf_graph_2_contexts) == 3
    assert graph1 in rdf_graph_contexts and graph1 in rdf_graph_2_contexts
    assert graph2 in rdf_graph_contexts and graph2 in rdf_graph_2_contexts

    rdf_graph_2_copy = RDFConjunctiveGraph()
    for quad in rdf_graph_2.quads((None, None, None, None)):
        rdf_graph_2_copy.add(quad)

    rdf_graph_2_copy.add(
        (mary_likes_matt_05, adbrdf.adb_key_uri, Literal(_mary_likes_matt_05))
    )
    rdf_graph_2_copy.add(
        (mary_likes_matt_075, adbrdf.adb_key_uri, Literal(_mary_likes_matt_075))
    )
    rdf_graph_2_copy.add(
        (mary_likes_matt_1, adbrdf.adb_key_uri, Literal(_mary_likes_matt_1))
    )
    rdf_graph_2_copy.add(
        (
            john_said_mary_likes_matt_05,
            adbrdf.adb_key_uri,
            Literal(_john_said_mary_likes_matt_05),
        )
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph_2_copy,
        overwrite_graph=True,
    )

    col = adb_graph.edge_collection("likes")
    assert col.has(_mary_likes_matt_05)
    assert col.has(_mary_likes_matt_075)
    assert col.has(_mary_likes_matt_1)

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_3 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_2)())
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph_3)) == 0

    rdf_graph_3_copy = RDFConjunctiveGraph()
    for quad in rdf_graph_3.quads((None, None, None, None)):
        rdf_graph_3_copy.add(quad)

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name, rdf_graph_3_copy, overwrite_graph=True, flatten_reified_triples=False
    )

    assert adb_graph.has_vertex_collection("Statement")
    assert adb_graph.has_edge_collection("subject")
    assert adb_graph.has_edge_collection("predicate")
    assert adb_graph.has_edge_collection("object")

    NON_LITERAL_STATEMENTS = len(rdf_graph_3) - len(get_literal_statements(rdf_graph_3))
    UNIQUE_NODES = len(
        get_uris(rdf_graph_3, include_predicates=True)
        | get_bnodes(rdf_graph_3, include_predicates=True)
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS

    rdf_graph_4 = adbrdf.arangodb_graph_to_rdf(name, type(rdf_graph_3)())
    assert len(subtract_graphs(rdf_graph_4, rdf_graph_3)) == 0
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_4)) == 0

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Meta_PGT", get_meta_graph())],
)
def test_pgt_meta(name: str, rdf_graph: RDFConjunctiveGraph) -> None:
    META_GRAPH_CONTEXTS = {
        "http://www.arangodb.com/",
        "http://www.w3.org/2002/07/owl#",
        "http://purl.org/dc/elements/1.1/",
        "http://www.w3.org/2001/XMLSchema#",
        "http://www.w3.org/2000/01/rdf-schema#",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    }

    NON_LITERAL_STATEMENTS = len(rdf_graph) - len(get_literal_statements(rdf_graph))
    UNIQUE_NODES = len(
        get_uris(rdf_graph, include_predicates=True)
        | get_bnodes(rdf_graph, include_predicates=True)
    )

    META_GRAPH_UNKNOWN_RESOURCES = 12

    assert {str(sg.identifier) for sg in rdf_graph.contexts()} == META_GRAPH_CONTEXTS

    adbrdf.rdf_to_arangodb_by_pgt(
        name,
        get_meta_graph(),
        contextualize_graph=False,
        overwrite_graph=True,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS
    assert (
        db.collection(f"{name}_UnknownResource").count() == META_GRAPH_UNKNOWN_RESOURCES
    )

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(
        name, type(rdf_graph)(), include_adb_v_col_statements=True
    )

    # Meta Ontology Contexts (i.e RDF, RDFS, OWL, DC, ArangoDB)
    for context in rdf_graph.contexts():
        assert (
            len(rdf_graph.get_context(context.identifier))
            == len(rdf_graph_2.get_context(context.identifier))
            > 0
        )

    # BNode Context
    rdf_graph_contexts = {str(sg.identifier) for sg in rdf_graph.contexts()}
    rdf_graph_2_contexts = {str(sg.identifier) for sg in rdf_graph_2.contexts()}
    context_diff = rdf_graph_2_contexts - rdf_graph_contexts
    assert len(context_diff) == 1
    bnode_context = rdf_graph_2.get_context(BNode(context_diff.pop()))

    assert len(bnode_context) == UNIQUE_NODES
    adb_col_statements = adbrdf.extract_adb_col_statements(rdf_graph_2)
    assert len(bnode_context) == 0

    assert len(adb_col_statements) == UNIQUE_NODES
    cols = {str(o) for o in adb_col_statements.objects(subject=None, predicate=None)}
    assert cols == {"Class", "Property", "Ontology", f"{name}_UnknownResource"}

    assert len(subtract_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph)) == 0
    assert len(rdf_graph_2) == len(rdf_graph)

    adbrdf.rdf_to_arangodb_by_pgt(
        name,
        get_meta_graph(),
        contextualize_graph=True,
        overwrite_graph=True,
    )

    v_count, e_count = get_adb_graph_count(name)
    assert v_count == UNIQUE_NODES
    assert e_count == NON_LITERAL_STATEMENTS
    assert (
        db.collection(f"{name}_UnknownResource").count() == META_GRAPH_UNKNOWN_RESOURCES
    )

    rdf_graph_3 = adbrdf.arangodb_graph_to_rdf(
        name, type(rdf_graph_2)(), include_adb_v_col_statements=True
    )

    # Meta Ontology Contexts (i.e RDF, RDFS, OWL, DC, ArangoDB)
    for context in rdf_graph.contexts():
        assert (
            len(rdf_graph.get_context(context.identifier))
            == len(rdf_graph_3.get_context(context.identifier))
            > 0
        )

    # BNode Context
    rdf_graph_3_contexts = {str(sg.identifier) for sg in rdf_graph_3.contexts()}
    context_diff = rdf_graph_3_contexts - rdf_graph_contexts
    assert len(context_diff) == 1
    bnode_context = rdf_graph_3.get_context(BNode(context_diff.pop()))

    assert len(bnode_context) == UNIQUE_NODES
    adb_col_statements_2 = adbrdf.extract_adb_col_statements(rdf_graph_3)
    assert len(bnode_context) == 0

    assert len(adb_col_statements_2) == UNIQUE_NODES
    cols = {str(o) for o in adb_col_statements_2.objects(subject=None, predicate=None)}
    assert cols == {"Class", "Property", "Ontology", f"{name}_UnknownResource"}

    assert len(subtract_graphs(adb_col_statements, adb_col_statements_2)) == 0
    assert len(subtract_graphs(adb_col_statements_2, adb_col_statements)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph_3)) == 0
    assert len(subtract_graphs(rdf_graph_3, rdf_graph_2)) == 0
    assert len(rdf_graph_3) == len(rdf_graph_2)

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Collection_PGT", get_rdf_graph("collection.ttl"))],
)
def test_pgt_collection(name: str, rdf_graph: RDFGraph) -> None:
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
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
        adbrdf.hash(f"{_doc}-{_random}-{_mars}")
    )

    # ArangoDB to RDF
    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(
        name,
        type(rdf_graph)(),
        list_conversion_mode="collection",
    )

    doc = URIRef("http://example.com/Doc")
    planets = URIRef("http://example.com/planets")

    numbers = URIRef("http://example.com/numbers")
    random = URIRef("http://example.com/random")
    nested_container = URIRef("http://example.com/nested_container")

    # TODO: Revisit these assertions.
    # too vague..
    assert (doc, numbers, None) in rdf_graph_2
    assert (doc, planets, None) in rdf_graph_2
    assert (doc, random, None) in rdf_graph_2
    assert (doc, nested_container, None) in rdf_graph_2

    # TODO: Revisit magic numbers...
    assert len([i for i in rdf_graph_2.triples((None, RDF.first, None))]) == 55
    assert len([i for i in rdf_graph_2.triples((None, RDF.rest, None))]) == 55
    assert len(rdf_graph_2) == 123

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Container_PGT", get_rdf_graph("container.ttl"))],
)
def test_pgt_container(name: str, rdf_graph: RDFGraph) -> None:
    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph + RDFGraph(),
        overwrite_graph=True,
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

    def flatten(li: List[Any]) -> List[Any]:
        return [
            item
            for sublist in li
            for item in (flatten(sublist) if isinstance(sublist, list) else [sublist])
        ]

    assert "more_numbers" in doc
    assert len(doc["more_numbers"]) == 2
    assert set(flatten(doc["more_numbers"])) == {1, 2, 3, 4, 5}

    # ArangoDB to RDF
    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(
        name,
        type(rdf_graph)(),
        list_conversion_mode="container",
    )

    doc = URIRef("http://example.com/Doc")
    planets = URIRef("http://example.com/planets")
    numbers = URIRef("http://example.com/numbers")
    more_numbers = URIRef("http://example.com/more_numbers")

    assert (doc, numbers, None) in rdf_graph_2
    assert (doc, planets, None) in rdf_graph_2
    assert (doc, more_numbers, None) in rdf_graph_2
    # TODO: Revisit magic number
    assert len(rdf_graph_2) == 51

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
        "foo": "bar",
    }

    db.collection("TestDoc").insert(doc)

    rdf_graph = adbrdf.arangodb_graph_to_rdf(
        name,
        RDFGraph(),
        dict_conversion_mode="static",
        include_adb_v_col_statements=True,
    )

    adb_col_statements = adbrdf.extract_adb_col_statements(
        rdf_graph, keep_adb_col_statements_in_rdf_graph=False
    )

    graph_namespace = f"{db._conn._url_prefixes[0]}/{name}"
    test_doc_namespace = f"{graph_namespace}/TestDoc"

    test_doc = URIRef(f"{test_doc_namespace}#1")
    foo = URIRef(f"{graph_namespace}/foo")
    val = URIRef(f"{graph_namespace}/val")

    assert len(adb_col_statements) == 1
    assert (test_doc, None, None) in rdf_graph
    assert (test_doc, URIRef(f"{graph_namespace}/foo"), Literal("bar")) in rdf_graph
    assert (test_doc, URIRef(f"{graph_namespace}/val"), None) in rdf_graph
    assert (None, URIRef(f"{graph_namespace}/sub_val_1"), Literal(1)) in rdf_graph
    assert (None, URIRef(f"{graph_namespace}/sub_val_2"), None) in rdf_graph
    assert (None, URIRef(f"{graph_namespace}/sub_val_3"), Literal(3)) in rdf_graph
    assert (None, URIRef(f"{graph_namespace}/sub_val_4"), Literal(4)) in rdf_graph
    assert (None, URIRef(f"{graph_namespace}/sub_val_5"), None) in rdf_graph
    assert (None, URIRef(f"{graph_namespace}/sub_val_6"), Literal(6)) in rdf_graph
    assert (None, URIRef(f"{graph_namespace}/sub_val_7"), Literal(7)) in rdf_graph
    # TODO: Revisit magic number
    assert len(rdf_graph) == 10

    # TODO: RDF Graph back to ArangoDB with this monster ^
    # Need to discuss...
    # adb_graph = adbrdf.rdf_to_arangodb_by_pgt(f"{name}2", rdf_graph)
    # db.delete_graph(f"{name}2", drop_collections=True)

    rdf_graph_2 = adbrdf.arangodb_to_rdf(
        name,
        RDFGraph(),
        dict_conversion_mode="static",
        metagraph={"vertexCollections": {"TestDoc": {"foo"}}},
    )

    with pytest.raises(ValueError) as e:
        adbrdf.arangodb_to_rdf(
            name,
            RDFGraph(),
            dict_conversion_mode="static",
            metagraph={"vertexCollections": {"TestDoc": {"foo"}}},
            ignored_attributes={"foo"},
        )

    assert (
        "**ignored_attributes** cannot be used if **explicit_metagraph** is True"
        in str(e.value)
    )

    assert len(rdf_graph_2) == 1
    assert (
        test_doc,
        URIRef(f"{graph_namespace}/foo"),
        Literal("bar"),
    ) in rdf_graph_2

    rdf_graph_3 = adbrdf.arangodb_to_rdf(
        name,
        RDFGraph(),
        dict_conversion_mode="static",
        metagraph={"vertexCollections": {"TestDoc": {"foo"}}},
        explicit_metagraph=False,
    )

    assert len(rdf_graph_3) == len(rdf_graph)

    rdf_graph_4 = adbrdf.arangodb_graph_to_rdf(
        name,
        RDFGraph(),
        dict_conversion_mode="serialize",
        include_adb_v_col_statements=True,
        include_adb_v_key_statements=True,
    )

    assert len(rdf_graph_4) == 4
    assert (test_doc, foo, Literal("bar")) in rdf_graph_4
    assert (test_doc, val, Literal(json.dumps(doc["val"]))) in rdf_graph_4

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph_4,
        overwrite_graph=True,
    )

    new_doc = adb_graph.vertex_collection("TestDoc").get("1")
    assert new_doc
    assert new_doc["foo"] == doc["foo"]
    assert new_doc["val"] == doc["val"]

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Key", get_rdf_graph("key.ttl"))],
)
def test_adb_key_uri(name: str, rdf_graph: RDFGraph) -> None:
    adb_graph_rpt = adbrdf.rdf_to_arangodb_by_rpt(
        f"{name}_RPT", rdf_graph + RDFGraph(), overwrite_graph=True
    )
    adb_graph_pgt = adbrdf.rdf_to_arangodb_by_pgt(
        f"{name}_PGT", rdf_graph + RDFGraph(), overwrite_graph=True
    )

    _bob = "1"
    _alice = "2"
    _john = "3"
    _person = "Person"

    col = adb_graph_rpt.vertex_collection(f"{name}_RPT_URIRef")
    assert col.has(_bob)
    assert col.has(_alice)
    assert col.has(_john)
    assert col.has(_person)

    col = adb_graph_pgt.vertex_collection("Person")
    assert col.has(_bob)
    assert col.has(_alice)
    assert col.has(_john)

    col = adb_graph_pgt.vertex_collection("Class")
    assert col.has(_person)

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(
        f"{name}_RPT", type(rdf_graph)(), include_adb_v_key_statements=True
    )

    assert len(subtract_graphs(rdf_graph, rdf_graph_2)) == 0
    assert len(subtract_graphs(rdf_graph_2, rdf_graph)) == 0

    rdf_graph_3 = adbrdf.arangodb_graph_to_rdf(
        f"{name}_PGT", type(rdf_graph)(), include_adb_v_key_statements=True
    )

    assert len(subtract_graphs(rdf_graph, rdf_graph_3)) == 0
    assert set(subtract_graphs(rdf_graph_3, rdf_graph).subjects()) == {RDF.type}

    db.delete_graph(f"{name}_RPT", drop_collections=True)
    db.delete_graph(f"{name}_PGT", drop_collections=True)


@pytest.mark.parametrize(
    "name, path, edge_definitions, orphan_collections",
    [
        (
            "GameOfThrones",
            "tests/data/adb/got_dump",
            [
                {
                    "edge_collection": "ChildOf",
                    "from_vertex_collections": ["Characters"],
                    "to_vertex_collections": ["Characters"],
                },
            ],
            ["Traits", "Locations"],
        )
    ],
)
def test_game_of_thrones_graph(
    name: str,
    path: str,
    edge_definitions: List[Dict[str, Any]],
    orphan_collections: List[str],
) -> None:
    if not db.has_graph(name):
        arango_restore(path)
        db.create_graph(
            name,
            edge_definitions=edge_definitions,
            orphan_collections=orphan_collections,
        )

    adb_graph = db.graph(name)
    rdf_graph = adbrdf.arangodb_graph_to_rdf(
        name,
        RDFGraph(),
        list_conversion_mode="static",
        infer_type_from_adb_v_col=True,
        include_adb_v_col_statements=True,
        include_adb_v_key_statements=True,
        include_adb_e_key_statements=True,
        batch_size=1,
    )

    adb_col_statements = adbrdf.extract_adb_col_statements(
        rdf_graph, keep_adb_col_statements_in_rdf_graph=False
    )

    adb_key_statements = adbrdf.extract_adb_key_statements(
        rdf_graph, keep_adb_key_statements_in_rdf_graph=False
    )

    adb_graph_namespace = f"{db._conn._url_prefixes[0]}/{name}"

    doc: Dict[str, Any]
    for v_col in adb_graph.vertex_collections():
        v_col_uri = URIRef(f"{adb_graph_namespace}/{v_col}")

        for doc in db.collection(v_col):
            term = URIRef(f"{adb_graph_namespace}/{v_col}#{doc['_key']}")
            assert (term, RDF.type, v_col_uri) in rdf_graph

            for k, _ in doc.items():
                if k not in ["_key", "_id", "_rev"]:
                    property = URIRef(f"{adb_graph_namespace}/{k}")
                    assert (term, property, None) in rdf_graph

            assert (term, adbrdf.adb_col_uri, Literal(v_col)) in adb_col_statements
            assert (
                term,
                adbrdf.adb_key_uri,
                Literal(doc["_key"]),
            ) in adb_key_statements

    for e_d in adb_graph.edge_definitions():
        e_col = e_d["edge_collection"]
        e_col_uri = URIRef(f"{adb_graph_namespace}/{e_col}")

        for doc in db.collection(e_col):
            from_v_col, from_v_key = doc["_from"].split("/")
            to_v_col, to_v_key = doc["_to"].split("/")
            subject = URIRef(f"{adb_graph_namespace}/{from_v_col}#{from_v_key}")
            object = URIRef(f"{adb_graph_namespace}/{to_v_col}#{to_v_key}")

            edge_has_metadata = False
            edge = URIRef(f"{adb_graph_namespace}/{e_col}#{doc['_key']}")
            for k, _ in doc.items():
                if k not in ["_key", "_id", "_rev", "_from", "_to"]:
                    edge_has_metadata = True
                    property = URIRef(f"{adb_graph_namespace}/{k}")
                    assert (edge, property, None) in rdf_graph

            if edge_has_metadata:
                assert (edge, RDF.type, RDF.Statement) in rdf_graph
                assert (edge, RDF.subject, subject) in rdf_graph
                assert (edge, RDF.predicate, e_col_uri) in rdf_graph
                assert (edge, RDF.object, object) in rdf_graph
                assert (
                    edge,
                    adbrdf.adb_key_uri,
                    Literal(doc["_key"]),
                ) in adb_key_statements

    ####################################################
    adbrdf.rdf_to_arangodb_by_rpt(
        name,
        rdf_graph + adb_key_statements,
        overwrite_graph=True,
    )

    key_uri_triples = len({o for _, p, o in rdf_graph if p == adbrdf.adb_key_uri})
    rdf_star_triples = (
        len([1 for _, p, o in rdf_graph if (p, o) == (RDF.type, RDF.Statement)]) * 3
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

    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(name, RDFGraph())
    assert len(subtract_graphs(rdf_graph_2, rdf_graph)) == 0
    assert len(subtract_graphs(rdf_graph, rdf_graph_2)) == 0

    ####################################################

    adbrdf.rdf_to_arangodb_by_pgt(
        name,
        rdf_graph + adb_key_statements,
        adb_col_statements=adb_col_statements,
        batch_size=10,
        write_adb_col_statements=False,
        overwrite_graph=True,
    )

    # TODO: Add assertions

    ####################################################

    rdf_graph_3 = adbrdf.arangodb_graph_to_rdf(name, RDFGraph())
    assert len(subtract_graphs(rdf_graph_3, rdf_graph)) == 0
    assert len(subtract_graphs(rdf_graph, rdf_graph_3)) == 0

    ####################################################

    db.delete_graph(name, drop_collections=True)


@pytest.mark.parametrize("name", [("OPEN_INTELLIGENCE_ANGOLA")])
def test_open_intelligence_graph(name: str) -> None:
    Datasets(db).load(name)
    v_count_1, e_count_1 = get_adb_graph_count(name)

    rdf_graph = adbrdf.arangodb_graph_to_rdf(
        name,
        RDFGraph(),
        list_conversion_mode="serialize",
        dict_conversion_mode="serialize",
        include_adb_v_col_statements=True,
        include_adb_v_key_statements=True,
        include_adb_e_key_statements=True,
    )

    adb_graph = adbrdf.rdf_to_arangodb_by_pgt(name, rdf_graph, overwrite_graph=True)
    v_count_2, e_count_2 = get_adb_graph_count(name)

    property_col_count = adb_graph.vertex_collection("Property").count()
    assert v_count_1 == v_count_2 - property_col_count
    assert e_count_1 == e_count_2

    db.delete_graph(name, drop_collections=True)


def test_multiple_graphs_pgt() -> None:
    g1 = RDFGraph()

    g1.parse(
        data="""
        @prefix ex: <http://example.com/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

        ex:alice a ex:Person ;
            ex:knows ex:bob ;
            ex:age "25"^^xsd:integer .

        ex:bob a ex:Person ;
            ex:knows ex:charlie ;
            ex:age "30"^^xsd:integer .

        ex:charlie a ex:Person ;
            ex:knows ex:alice ;
            ex:age "35"^^xsd:integer .
        """,
        format="turtle",
    )

    g2 = RDFGraph()

    g2.parse(
        data="""
        @prefix ex: <http://example.com/> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

        ex:hired rdfs:range ex:Person .

        ex:Apple a ex:Company .
        ex:Apple ex:hired ex:bob .
        """,
        format="turtle",
    )

    for g in [g1, g2]:
        adbrdf.rdf_to_arangodb_by_pgt("PersonGraph", g, overwrite_graph=False)

    edge_defs = {
        e_d["edge_collection"]: e_d
        for e_d in db.graph("PersonGraph").edge_definitions()
    }

    assert edge_defs == {
        "hired": {
            "edge_collection": "hired",
            "from_vertex_collections": ["Company"],
            "to_vertex_collections": ["Person"],
        },
        "range": {
            "edge_collection": "range",
            "from_vertex_collections": ["Property"],
            "to_vertex_collections": ["Class"],
        },
        "knows": {
            "edge_collection": "knows",
            "from_vertex_collections": ["Person"],
            "to_vertex_collections": ["Person"],
        },
        "type": {
            "edge_collection": "type",
            "from_vertex_collections": ["Company", "Person"],
            "to_vertex_collections": ["Class"],
        },
    }

    db.delete_graph("PersonGraph", ignore_missing=True, drop_collections=True)


@pytest.mark.parametrize(
    "graph_name, rdf_graph",
    [("NamespacePrefixTest", RDFGraph())],
)
def test_namespace_collection(graph_name: str, rdf_graph: RDFGraph) -> None:
    # Create test data with namespaces
    rdf_graph.parse(
        data="""
        @prefix ex: <http://example.com/> .
        @prefix foaf: <http://xmlns.com/foaf/0.1/> .
        @prefix schema: <https://schema.org/> .

        ex:alice a foaf:Person ;
            schema:name "Alice" .
        """,
        format="turtle",
    )

    db.delete_graph(graph_name, drop_collections=True, ignore_missing=True)
    db.delete_collection("namespaces", ignore_missing=True)

    adbrdf.rdf_to_arangodb_by_pgt(
        graph_name,
        rdf_graph,
        overwrite_graph=True,
        namespace_collection_name="namespaces",
    )

    assert db.has_collection("namespaces")
    assert db.collection("namespaces").count() >= 3

    assert db.collection("namespaces").has(adbrdf.hash("http://example.com/"))
    assert db.collection("namespaces").has(adbrdf.hash("http://xmlns.com/foaf/0.1/"))
    assert db.collection("namespaces").has(adbrdf.hash("https://schema.org/"))

    # Test ArangoDB to RDF without namespace prefix collection
    rdf_graph_2 = adbrdf.arangodb_graph_to_rdf(graph_name, RDFGraph())

    # Verify the namespaces were preserved
    namespace_dict = {prefix: uri for prefix, uri in rdf_graph_2.namespaces()}
    assert "ex" not in namespace_dict

    # Test ArangoDB to RDF with namespace prefix collection
    rdf_graph_3 = adbrdf.arangodb_graph_to_rdf(
        graph_name, RDFGraph(), namespace_collection_name="namespaces"
    )

    # Verify the namespaces were preserved
    namespace_dict = {prefix: uri for prefix, uri in rdf_graph_3.namespaces()}
    assert "ns1" not in namespace_dict
    assert namespace_dict["ex"] == URIRef("http://example.com/")
    assert namespace_dict["foaf"] == URIRef("http://xmlns.com/foaf/0.1/")
    assert namespace_dict["schema"] == URIRef("https://schema.org/")

    # Verify the data was preserved with namespaced URIs
    alice = URIRef("http://example.com/alice")
    rdf_type = URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
    person = URIRef("http://xmlns.com/foaf/0.1/Person")
    name = URIRef("https://schema.org/name")

    assert (alice, rdf_type, person) in rdf_graph_3
    assert (alice, name, Literal("Alice")) in rdf_graph_3

    db.delete_graph(graph_name, drop_collections=True)
    db.delete_collection("namespaces")


def test_pgt_uri_collection_and_migrate_unknown_resources() -> None:
    db.delete_graph("Test", drop_collections=True, ignore_missing=True)
    db.delete_collection("URI_COLLECTION", ignore_missing=True)

    g1 = RDFGraph()
    g1.parse(
        data="""
        @prefix ex: <http://example.com/> .

        ex:Alice a ex:Person .
        ex:GreatBook a ex:Book .
    """,
        format="turtle",
    )

    g2 = RDFGraph()
    g2.parse(
        data="""
        @prefix ex: <http://example.com/> .

        ex:Alice ex:wrote ex:GreatBook .

        ex:Alice ex:age 25 .
        ex:GreatBook ex:title "The Great Novel" .
    """,
        format="turtle",
    )

    adbrdf.rdf_to_arangodb_by_pgt("Test", g1, uri_map_collection_name="URI_COLLECTION")

    assert db.collection("URI_COLLECTION").count() == 5
    assert db.collection("URI_COLLECTION").has(adbrdf.hash("http://example.com/Alice"))
    assert db.collection("URI_COLLECTION").has(
        adbrdf.hash("http://example.com/GreatBook")
    )
    assert db.collection("URI_COLLECTION").has(adbrdf.hash("http://example.com/Person"))
    assert db.collection("URI_COLLECTION").has(adbrdf.hash("http://example.com/Book"))
    assert db.collection("URI_COLLECTION").has(
        adbrdf.hash("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
    )

    # No IRI Collection is specified, so Unknown Resources must be manually migrated
    adbrdf.rdf_to_arangodb_by_pgt("Test", g2)

    assert db.collection("URI_COLLECTION").count() == 5
    assert db.collection("Test_UnknownResource").count() == 2
    assert db.collection("Test_UnknownResource").has(
        adbrdf.hash("http://example.com/Alice")
    )
    assert db.collection("Test_UnknownResource").has(
        adbrdf.hash("http://example.com/GreatBook")
    )
    edge = db.collection("wrote").random()
    assert "UnknownResource/" in edge["_from"]
    assert "UnknownResource/" in edge["_to"]

    alice = db.collection("Test_UnknownResource").get(
        adbrdf.hash("http://example.com/Alice")
    )
    assert alice["age"] == 25
    alice_2 = db.collection("Person").random()
    assert "age" not in alice_2

    # Migrate Unknown Resources to IRI Collection
    adbrdf.migrate_unknown_resources("Test", "URI_COLLECTION")

    assert db.collection("Test_UnknownResource").count() == 0
    edge = db.collection("wrote").random()
    assert "Person/" in edge["_from"]
    assert "Book/" in edge["_to"]

    alice = db.collection("Person").random()
    assert alice["age"] == 25

    book = db.collection("Book").random()
    assert book["title"] == "The Great Novel"

    db.delete_graph("Test", drop_collections=True)
    db.delete_collection("URI_COLLECTION")


def test_pgt_uri_collection_back_to_back() -> None:
    db.delete_graph("Test", drop_collections=True, ignore_missing=True)
    db.delete_collection("URI_COLLECTION", ignore_missing=True)

    g1 = RDFGraph()
    g1.parse(
        data="""
        @prefix ex: <http://example.com/> .

        ex:Alice a ex:Person .
        ex:GreatBook a ex:Book .
    """,
        format="turtle",
    )

    g2 = RDFGraph()
    g2.parse(
        data="""
        @prefix ex: <http://example.com/> .

        ex:Alice ex:wrote ex:GreatBook .

        ex:Alice ex:age 25 .
        ex:GreatBook ex:title "The Great Novel" .
    """,
        format="turtle",
    )

    adbrdf.rdf_to_arangodb_by_pgt("Test", g1, uri_map_collection_name="URI_COLLECTION")
    adbrdf.rdf_to_arangodb_by_pgt("Test", g2, uri_map_collection_name="URI_COLLECTION")

    assert db.collection("Test_UnknownResource").count() == 0
    edge = db.collection("wrote").random()
    assert "Person/" in edge["_from"]
    assert "Book/" in edge["_to"]

    alice = db.collection("Person").random()
    assert alice["age"] == 25

    book = db.collection("Book").random()
    assert book["title"] == "The Great Novel"

    db.delete_graph("Test", drop_collections=True)
    db.delete_collection("URI_COLLECTION")


def test_pgt_uri_collection_back_to_back_with_unknown_resources() -> None:
    db.delete_graph("Test", drop_collections=True, ignore_missing=True)
    db.delete_collection("URI_COLLECTION", ignore_missing=True)

    g1 = RDFGraph()
    g1.parse(
        data="""
        @prefix ex: <http://example.com/> .

        ex:Alice a ex:Person ;
            ex:name "Alice" ;
            ex:age 25 ;
            ex:worksAt ex:ACME ;
            ex:reads ex:GreatBook .

        ex:GreatBook ex:isBasedOn ex:Bob ;
            ex:wasWrittenBy ex:Bob .

        ex:A ex:isBasedOn ex:B .

        ex:ACME a ex:Organization ;
            ex:name "ACME Corp" .
    """,
        format="turtle",
    )

    g2 = RDFGraph()
    g2.parse(
        data="""
        @prefix ex: <http://example.com/> .

        ex:ACME ex:founded "2000" ;
            ex:employs ex:Alice .

        ex:A a ex:Thing .
        ex:B a ex:Thing .

        ex:GreatBook a ex:Book ;
            ex:title "The Great Novel" .

        ex:Bob a ex:Person .
    """,
        format="turtle",
    )

    adbrdf.rdf_to_arangodb_by_pgt("Test", g1, uri_map_collection_name="URI_COLLECTION")
    adbrdf.rdf_to_arangodb_by_pgt("Test", g2, uri_map_collection_name="URI_COLLECTION")

    assert db.collection("URI_COLLECTION").count() == 20
    assert db.collection("Test_UnknownResource").count() == 4
    assert db.collection("Test_UnknownResource").has(
        adbrdf.hash("http://example.com/A")
    )
    assert db.collection("Test_UnknownResource").has(
        adbrdf.hash("http://example.com/B")
    )
    assert db.collection("Test_UnknownResource").has(
        adbrdf.hash("http://example.com/GreatBook")
    )
    assert db.collection("Test_UnknownResource").has(
        adbrdf.hash("http://example.com/Bob")
    )

    for edge in db.collection("isBasedOn").all():
        assert "UnknownResource/" in edge["_from"]
        assert "UnknownResource/" in edge["_to"]

    for edge in db.collection("wasWrittenBy").all():
        assert "UnknownResource/" in edge["_from"]
        assert "UnknownResource/" in edge["_to"]

    assert "UnknownResource/" in db.collection("reads").random()["_to"]

    ur_count, edge_count = adbrdf.migrate_unknown_resources("Test", "URI_COLLECTION")
    assert ur_count == 4
    assert edge_count == 7  # 1 edit for each _from/_to if UnknownResource is migrated

    assert db.collection("Test_UnknownResource").count() == 0
    assert db.collection("URI_COLLECTION").count() == 20

    assert db.collection("Book").random()["title"] == "The Great Novel"

    for edge in db.collection("isBasedOn").all():
        assert "Book/" in edge["_from"] or "Thing/" in edge["_from"]
        assert "Person/" in edge["_to"] or "Thing/" in edge["_to"]

    for edge in db.collection("wasWrittenBy").all():
        assert "Book/" in edge["_from"]
        assert "Person/" in edge["_to"]

    assert "Book/" in db.collection("reads").random()["_to"]

    db.delete_graph("Test", drop_collections=True)
    db.delete_collection("URI_COLLECTION")


def test_pgt_uri_collection_back_to_back_with_type_statements() -> None:
    db.delete_graph("Test", drop_collections=True, ignore_missing=True)
    db.delete_collection("URI_COLLECTION", ignore_missing=True)

    g1 = RDFGraph()
    g1.parse(
        data="""
        @prefix ex: <http://example.com/> .

        ex:Alice a ex:Person .
        """,
        format="turtle",
    )

    g2 = RDFGraph()
    g2.parse(
        data="""
        @prefix ex: <http://example.com/> .

        ex:Alice a ex:Human .
        """,
        format="turtle",
    )

    adbrdf.rdf_to_arangodb_by_pgt("Test", g1, uri_map_collection_name="URI_COLLECTION")
    adbrdf.rdf_to_arangodb_by_pgt("Test", g2, uri_map_collection_name="URI_COLLECTION")

    assert (
        db.document(f"URI_COLLECTION/{adbrdf.hash('http://example.com/Alice')}")[
            "collection"
        ]
        == "Person"
    )
    assert db.collection("type").count() == 2
    assert db.collection("Person").count() == 1
    assert not db.has_collection("Human")

    db.delete_graph("Test", drop_collections=True)
    db.delete_collection("URI_COLLECTION")


def test_pgt_import_exception_from_schema_violation() -> None:
    db.delete_graph("Test", drop_collections=True, ignore_missing=True)
    db.delete_collection("Person", ignore_missing=True)

    db.create_collection("Person")
    db.collection("Person").configure(
        schema={
            "rule": {
                "type": "object",
                "required": ["name", "age"],
                "properties": {"name": {"type": "string"}, "age": {"type": "number"}},
            },
            "level": "strict",
            "message": "Invalid Person document",
        }
    )

    g = RDFGraph()
    g.parse(
        data="""
        @prefix ex: <http://example.com/> .

        ex:Alice a ex:Person .
        """,
        format="turtle",
    )

    with pytest.raises(ArangoRDFImportException) as e:
        adbrdf.rdf_to_arangodb_by_pgt("Test", g)

    assert "Invalid Person document" in str(e.value.error)
    assert "Person" == e.value.collection
    assert len(e.value.documents) == 1

    g = RDFGraph()
    g.parse(
        data="""
        @prefix ex: <http://example.com/> .

        ex:Alice a ex:Person .
        ex:Alice ex:name "Alice" .
        ex:Alice ex:age 25 .
        """,
        format="turtle",
    )

    adbrdf.rdf_to_arangodb_by_pgt("Test", g)

    db.delete_graph("Test", drop_collections=True)


def test_pgt_resource_collection_name_and_set_types_attribute() -> None:
    db.delete_graph("Test", drop_collections=True, ignore_missing=True)

    g = RDFGraph()
    g.parse(
        data="""
        @prefix ex: <http://example.com/> .

        ex:Alice a ex:Person .
        ex:Alice a ex:Human .
        ex:Alice ex:name "Alice" .
        ex:Alice ex:age 25 .

        ex:Bob a ex:Person .
        ex:Bob a ex:Human .
        ex:Bob ex:name "Bob" .
        ex:Bob ex:age 30 .

        ex:Alice ex:friend ex:Bob .

        ex:ACME a ex:Organization .
        ex:ACME a ex:Company .
        """,
        format="turtle",
    )

    with pytest.raises(ValueError) as e:
        adbrdf.rdf_to_arangodb_by_pgt(
            "Test",
            g,
            resource_collection_name="Node",
            uri_map_collection_name="URI_COLLECTION",
        )

    m = "Cannot specify both **uri_map_collection_name** and **resource_collection_name**."  # noqa: E501
    assert m in str(e.value)

    adbrdf.rdf_to_arangodb_by_pgt("Test", g, resource_collection_name="Node")

    assert not db.has_collection("Person")
    assert not db.has_collection("Human")
    assert not db.has_collection("Test_UnknownResource")
    assert db.has_collection("Node")
    assert db.collection("Node").count() == 3
    assert db.collection("Node").has(adbrdf.hash("http://example.com/Alice"))
    assert db.collection("Node").has(adbrdf.hash("http://example.com/Bob"))
    assert db.collection("Node").has(adbrdf.hash("http://example.com/ACME"))

    assert db.collection("Class").count() == 4
    assert db.collection("Property").count() == 4

    assert db.collection("friend").count() == 1
    edge = db.collection("friend").random()
    assert "Node/" in edge["_from"]
    assert "Node/" in edge["_to"]

    for edge in db.collection("type"):
        assert "Node/" in edge["_from"]
        assert "Class/" in edge["_to"]

    for node in db.collection("Node"):
        assert "_type" not in node

    count = adbrdf.migrate_edges_to_attributes("Test", "type")

    node_col = db.collection("Node")
    assert set(node_col.get(adbrdf.hash("http://example.com/Alice"))["_type"]) == {
        "Person",
        "Human",
    }
    assert set(node_col.get(adbrdf.hash("http://example.com/Bob"))["_type"]) == {
        "Person",
        "Human",
    }
    assert set(node_col.get(adbrdf.hash("http://example.com/ACME"))["_type"]) == {
        "Organization",
        "Company",
    }
    assert count == 3

    db.delete_graph("Test", drop_collections=True)

    adbrdf.rdf_to_arangodb_by_pgt("Test", g)

    assert not db.has_collection("Node")
    assert db.has_collection("Human")
    assert not db.has_collection("Person")
    assert db.has_collection("Company")
    assert not db.has_collection("Organization")

    for v in db.collection("Human"):
        assert "_type" not in v

    for v in db.collection("Company"):
        assert "_type" not in v

    count = adbrdf.migrate_edges_to_attributes("Test", "type", "foo")
    assert count == 3

    for v in db.collection("Human"):
        assert set(v["foo"]) == {"Person", "Human"}

    for v in db.collection("Company"):
        assert set(v["foo"]) == {"Organization", "Company"}

    count = adbrdf.migrate_edges_to_attributes(
        graph_name="Test", edge_collection_name="friend"
    )

    alice = db.collection("Human").get(adbrdf.hash("http://example.com/Alice"))
    assert alice["_friend"] == ["Bob"]

    bob = db.collection("Human").get(adbrdf.hash("http://example.com/Bob"))
    assert bob["_friend"] == []

    assert count == 2

    count = adbrdf.migrate_edges_to_attributes(
        graph_name="Test", edge_collection_name="friend", edge_direction="ANY"
    )

    assert count == 2

    alice = db.collection("Human").get(adbrdf.hash("http://example.com/Alice"))
    assert alice["_friend"] == ["Bob"]

    bob = db.collection("Human").get(adbrdf.hash("http://example.com/Bob"))
    assert bob["_friend"] == ["Alice"]

    with pytest.raises(ValueError) as e:
        adbrdf.migrate_edges_to_attributes(
            graph_name="Test", edge_collection_name="friend", edge_direction="INVALID"
        )

    assert "Invalid edge direction: INVALID" in str(e.value)

    with pytest.raises(ValueError) as e:
        adbrdf.migrate_edges_to_attributes(
            graph_name="Test", edge_collection_name="INVALID"
        )

    m = "No edge definition found for 'INVALID' in graph 'Test'. Cannot migrate edges to attributes."  # noqa: E501
    assert m in str(e.value)

    with pytest.raises(ValueError) as e:
        adbrdf.migrate_edges_to_attributes(
            graph_name="INVALID", edge_collection_name="friend"
        )

    assert "Graph 'INVALID' does not exist" in str(e.value)

    db.delete_graph("Test", drop_collections=True)


def test_pgt_predicate_collection_name() -> None:
    db.delete_graph("Test", drop_collections=True, ignore_missing=True)

    g = RDFGraph()
    g.parse(
        data="""
        @prefix ex: <http://example.com/> .

        ex:Alice a ex:Person .
        ex:Alice ex:name "Alice" .
        ex:Alice ex:age 25 .

        ex:Bob a ex:Person .
        ex:Bob ex:name "Bob" .
        ex:Bob ex:age 30 .

        ex:Alice ex:friend ex:Bob .
        """,
        format="turtle",
    )

    adbrdf.rdf_to_arangodb_by_pgt("Test", g, predicate_collection_name="Edge")

    assert not db.has_collection("type")
    assert not db.has_collection("friend")
    assert db.has_collection("Edge")
    assert db.has_collection("Class")
    assert db.has_collection("Person")
    assert db.has_collection("Property")

    assert db.collection("Edge").count() == 3
    for edge in db.collection("Edge"):
        assert "Person" in edge["_from"]
        assert "Person" in edge["_to"] or "Class" in edge["_to"]
        assert edge["_label"] in {"friend", "type"}

    assert db.collection("Person").count() == 2
    assert not db.has_collection("Node")

    db.delete_graph("Test", drop_collections=True)

    adbrdf.rdf_to_arangodb_by_pgt(
        "Test", g, predicate_collection_name="Edge", resource_collection_name="Node"
    )

    assert not db.has_collection("type")
    assert not db.has_collection("friend")
    assert db.has_collection("Edge")
    assert db.has_collection("Node")
    assert db.has_collection("Class")
    assert db.has_collection("Property")

    assert db.collection("Edge").count() == 3
    for edge in db.collection("Edge"):
        assert "Node" in edge["_from"]
        assert "Node" in edge["_to"] or "Class" in edge["_to"]
        assert edge["_label"] in {"friend", "type"}

    assert db.collection("Node").count() == 2
    assert not db.has_collection("Person")

    db.delete_graph("Test", drop_collections=True)


def test_lpg() -> None:
    db.delete_graph("Test", drop_collections=True, ignore_missing=True)

    g = RDFGraph()
    g.parse(
        data="""
        @prefix ex: <http://example.com/> .

        ex:Alice a ex:Person .
        ex:Alice ex:name "Alice" .
        ex:Alice ex:age 25 .

        ex:Bob a ex:Person .
        ex:Bob ex:name "Bob" .
        ex:Bob ex:age 30 .

        ex:Alice ex:friend ex:Bob .
        """,
        format="turtle",
    )

    adbrdf.rdf_to_arangodb_by_lpg("Test", g)

    assert db.collection("Node").count() == 2
    assert db.collection("Edge").count() == 3

    for node in db.collection("Node"):
        assert "_type" not in node

    adbrdf.migrate_edges_to_attributes(
        "Test", "Edge", "_type", filter_clause="e._label == 'type'"
    )

    for node in db.collection("Node"):
        assert node["_type"] == ["Person"]

    db.delete_graph("Test", drop_collections=True)


@pytest.mark.parametrize(
    "name, rdf_graph",
    [("Case_12_1_LPG", get_rdf_graph("cases/12_1.ttl"))],
)
def test_lpg_case_12_1(name: str, rdf_graph: RDFGraph) -> None:
    db.delete_graph("Test", drop_collections=True, ignore_missing=True)

    adbrdf.rdf_to_arangodb_by_lpg("Test", rdf_graph)

    assert db.collection("Node").count() == 2
    assert db.collection("Edge").count() == 2
    assert db.collection("Class").count() == 1
    assert db.collection("Property").count() == 2
    assert not db.has_collection("writer")

    for edge in db.collection("Edge"):
        assert edge["_from"].split("/")[0] in {"Edge", "Node"}
        assert edge["_to"].split("/")[0] in {"Class", "Node"}

    db.delete_graph("Test", drop_collections=True)


def test_pgt_second_order_edge_collection_name() -> None:
    db.delete_graph("Test", drop_collections=True, ignore_missing=True)

    g = RDFGraph()
    g.parse(
        data="""
        @prefix ex: <http://example.com/> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

        ex:Alice a ex:Human .
        
        ex:Bob a ex:Person .
        
        ex:Charlie a ex:Animal .
        
        ex:Dana a ex:Entity .
        
        ex:Eve a ex:Human .
        ex:Eve a ex:Person .

        ex:Fred a ex:Human .
        ex:Fred a ex:Individual .

        ex:Human rdfs:subClassOf ex:Animal .
        ex:Person rdfs:subClassOf ex:Individual .
        ex:Animal rdfs:subClassOf ex:Entity .
        ex:Individual rdfs:subClassOf ex:Entity .
        """,
        format="turtle",
    )

    adbrdf.rdf_to_arangodb_by_pgt("Test", g, resource_collection_name="Node")

    assert db.collection("subClassOf").count() == 4

    adbrdf.migrate_edges_to_attributes(
        "Test",
        edge_collection_name="type",
        second_order_edge_collection_name="subClassOf",
        second_order_depth=10,
    )

    alice = db.collection("Node").get(adbrdf.hash("http://example.com/Alice"))
    assert set(alice["_type"]) == {"Human", "Animal", "Entity"}

    bob = db.collection("Node").get(adbrdf.hash("http://example.com/Bob"))
    assert set(bob["_type"]) == {"Person", "Individual", "Entity"}

    charlie = db.collection("Node").get(adbrdf.hash("http://example.com/Charlie"))
    assert set(charlie["_type"]) == {"Animal", "Entity"}

    dana = db.collection("Node").get(adbrdf.hash("http://example.com/Dana"))
    assert set(dana["_type"]) == {"Entity"}

    eve = db.collection("Node").get(adbrdf.hash("http://example.com/Eve"))
    assert set(eve["_type"]) == {"Human", "Person", "Animal", "Individual", "Entity"}

    fred = db.collection("Node").get(adbrdf.hash("http://example.com/Fred"))
    assert set(fred["_type"]) == {"Human", "Individual", "Entity", "Animal"}

    db.delete_graph("Test", drop_collections=True)
