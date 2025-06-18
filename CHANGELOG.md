## 1.1.3 (2025-06-18)

### New

* `ArangoRDFImportException` (#32) [Anthony Mahanna]

  * new: `ArangoRDFImportException`

  * new: `collection` field

  * fix: test

  * fix: test


## 1.1.2 (2025-06-13)

### Fix

* Iri collection check (#30) [Anthony Mahanna]

  * fix: iri collection check

  * fix: lint

  * fix: set `raise_on_document_error` to True by default

  * fix: lint

  * fix: logic

  * fix: `build_tree`

  * fix: build_tree

### Other

* Add: isodate. [Anthony Mahanna]


## 1.1.1 (2025-06-03)

### Fix

* Missing param (#29) [Anthony Mahanna]

  * fix: missing param

  * fix: param


## 1.1.0 (2025-05-28)

### New

* `iri_collection` (#27) [Anthony Mahanna]

  * new: `namespace_prefix_collection`

  * fix: lint

  * initial commit

  * add: debug

  * Update README.md

* `namespace_prefix_collection` (#26) [Anthony Mahanna]

  * new: `namespace_prefix_collection`

  * fix: lint

  * fix: lint pt2


## 1.0.2 (2025-04-03)

### Other

* Update: `__pgt_create_adb_graph` (#24) [Anthony Mahanna]


## 1.0.1 (2025-01-23)

### Other

* Cleanup config (#22) [Anthony Mahanna]

  * cleanup config

  testing with rdflib 7...

  * attempt: isodate

  * attempt: replace `ConjunctiveGraph` with `Dataset`

  this won't work for now

  * fix: lint

  * attempt: update `conftest.py`

  * Revert "attempt: update `conftest.py`"

  This reverts commit 79783278adb5f41e6bb8b8f0192b20d0b40e492f.

  * Revert "fix: lint"

  This reverts commit e01eae6936c475f9c74521ec649d61d610b7e1ed.

  * Revert "attempt: replace `ConjunctiveGraph` with `Dataset`"

  This reverts commit 070310867d8a6771d915cb155f1f9e80ec6e648e.

  * update: pyproject


## 1.0.0 (2024-08-13)

### Other

* Update README.md. [Anthony Mahanna]

* Fix broken readme link. [Anthony Mahanna]


## 0.9.0 (2024-01-23)

### Other

* Fix hyperlinks. [Anthony Mahanna]

* Documentation updates (#19) [Anthony Mahanna]

  * cleanup hash logic, make `rdf_graph` private

  * checkpoint

  * more cleanup

  * update action versions

  * checkpoint

  * update tests

  * checkpoint: green tests

  * checkpoint: cases `14_2` and `15` are failing

  * fix: make `__hash()` public

  * cleanup `conftest`

  * fix `v_count/e_count` assertions, more test cleanup

  * new: case 15_1, 15_2, 15_3

  * cleanup case 13

  * checkpoing: all tests green (except 1 flaky)

  15_2 RPT is flaky, need to revisit

  * update case 14_1

  * cleanup tests

  * checkpoint: `__process_subject_predicate_object`

  * fix lint

  * fix conftest

  * set `continue-on-error`

  * update case `15_2` (still flaky)

  * fix: `type` instead of `isinstance`

  * update case `14_1`

  * update tests

  * update case `container.ttl`

  * update `test_main`

  add `+ RDFGraph()` hack, update `test_pgt_container`

  * new: `pgt_remove_blacklisted_statements`, `pgt_parse_literal_statements`, remove `adb_col_blacklist`

  so many todos...

  * fix flake

  * new: case 13_1 and 13_2

  * new: case 14_3

  * new: case 15_4

  * update tests

  * fix: rdf namespacing

  * fix case 7 and native graph

  * new: `adb_col_statements`, `write_adb_col_statements` (pgt)

  * update 13_2, 15_2, 14_3

  * new test cases, use `pytest.xfail` on flaky assertions

  tests should be green now...

  * flake ignore

  can't reproduce

  * new: `explicit_metagraph`, optimize `fetch_adb_docs`

  * fix typo

  * cleanup: `**adb_kwargs`

  * doc cleanup

  * cleanup

  * cleanup: `flatten_reified_triples`

  * cleanup: progress/spinner bars via `rich`

  * more `rich` cleanup

  * update cases 10, 14.3, 15_4

  * final main checkpoint: `arango_rdf`

  * minor cleanup

  * new: `__pgt_process_rdf_literal`

  * new: `serialize` as a conversion mode

  * new: `test_open_intelligence_graph`

  * fix lint

  * fix: `statements`, `rdf_graph` ref

  * cleanup: `write_adb_col_statements`

  * initial commit

  * fix lint

  * update notebook

  * checkpoint

  * fix lint

  * Create .readthedocs.yaml

  * Update README.md

  * Update requirements.txt

  * fix: code block warning

  * cleanup

  * nit

  * fix hyperlinks

  * fix docstring

* Housekeeping (#18) [Anthony Mahanna]

  * cleanup hash logic, make `rdf_graph` private

  * checkpoint

  * more cleanup

  * update action versions

  * checkpoint

  * update tests

  * checkpoint: green tests

  * checkpoint: cases `14_2` and `15` are failing

  * fix: make `__hash()` public

  * cleanup `conftest`

  * fix `v_count/e_count` assertions, more test cleanup

  * new: case 15_1, 15_2, 15_3

  * cleanup case 13

  * checkpoing: all tests green (except 1 flaky)

  15_2 RPT is flaky, need to revisit

  * update case 14_1

  * cleanup tests

  * checkpoint: `__process_subject_predicate_object`

  * fix lint

  * fix conftest

  * set `continue-on-error`

  * update case `15_2` (still flaky)

  * fix: `type` instead of `isinstance`

  * update case `14_1`

  * update tests

  * update case `container.ttl`

  * update `test_main`

  add `+ RDFGraph()` hack, update `test_pgt_container`

  * new: `pgt_remove_blacklisted_statements`, `pgt_parse_literal_statements`, remove `adb_col_blacklist`

  so many todos...

  * fix flake

  * new: case 13_1 and 13_2

  * new: case 14_3

  * new: case 15_4

  * update tests

  * fix: rdf namespacing

  * fix case 7 and native graph

  * new: `adb_col_statements`, `write_adb_col_statements` (pgt)

  * update 13_2, 15_2, 14_3

  * new test cases, use `pytest.xfail` on flaky assertions

  tests should be green now...

  * flake ignore

  can't reproduce

  * new: `explicit_metagraph`, optimize `fetch_adb_docs`

  * fix typo

  * cleanup: `**adb_kwargs`

  * doc cleanup

  * cleanup

  * cleanup: `flatten_reified_triples`

  * cleanup: progress/spinner bars via `rich`

  * more `rich` cleanup

  * update cases 10, 14.3, 15_4

  * final main checkpoint: `arango_rdf`

  * minor cleanup

  * new: `__pgt_process_rdf_literal`

  * new: `serialize` as a conversion mode

  * new: `test_open_intelligence_graph`

  * fix lint

  * fix: `statements`, `rdf_graph` ref

  * cleanup: `write_adb_col_statements`

* Revert 6f85c04. [Anthony Mahanna]

* Cleanup: notebooks. [Anthony Mahanna]

* Update notebook output. [Anthony Mahanna]

* Fix pepy badge. [Anthony Mahanna]


## 0.1.0 (2023-12-04)

### Other

* ArangoRDF Overhaul: 0.1.0 (#15) [Anthony Mahanna]

  * new: test suite & test data

  * update: repo config

  * new: arango_rdf overhaul checkpoint

  * temp: base ontology files

  location TBD

  * new: `flake8` & `mypy` workflows

  * fix: black, flake, mypy

  * cleanup

  * temp: disable black worflow

  * fix: add flake & mypy dependency

  * fix: add `rich` dependency

  * temp: disable `mypy` workflow

  getting inconsistent `mypy` results between local environment & Github Actions environment

  * enable: black, mypy

  * cleanup: `arango_rdf`

  formatting fixes, mypy fixes, docstring updates, general code cleanup

  * black: test_main

  * update: setup files

  * update: test_pgt_case_3_2

  addresses all **list_conversion** parameter cases

  * update: tests

  * misc: pragma no cover

  * fix: test assertions

  * update: test_rpt_basic_cases

  * cleanup: main

  * new: `rich` Live Group progress bars, `batch_size` parameter, code cleanup

  * update: `rich` trackers in utils

  * new: `RDFLists` typing

  * new: ignore E266 flake8

  * misc: line breaks

  * update: `process_rpt_term`, pragma no cover

  * new: case 7 prototype

  * update 6.trig

  * cleanup utils

  * cleanup

  * variable renaming, cleanup

  * cleanup: test data

  * rework: test suite

  * remove: examples/data

  * remove: arango_rdf/ontologies

  * new: arango_rdf/meta

  * checkpoint: arango_rdf

  * fix: isort

  * fix: compare_graphs

  * temp fix: mypy

  * new: fraud detection & imdb tests

  * checkpoint: main.py

  * fix: isort

  * fix: isort (again)

  * new: meta files

  switching to `trig` format

  * checkpoint: tests

  * checkpoint: arango_rdf

  working on adb mapping functionality

  * checkpoint: tests

  * checkpoint: arango_rdf

  * cleanup: tests

  * checkpoint: arango_rdf

  * update: test cases

  * cleanup: arango_rdf

  * fix: rpt case 5

  * cleanup: tests

  * new: cityhash dependency

  * cleanup & docstrings: arango_rdf

  flake8 will fail

  * fix: flake8

  autopep8 & yapf did not work, manual fix was required

  * fix: pgt case 6

  * new: __build_subclass_tree() and __identify_best_class()

  * update: Tree.show()

  * cleanup main

  * new: dc.trig & xsd.trig starter files

  only adding the nodes that are referenced by the other ontologies (OWL, RDF, RDFS) for now

  * update: tests

  * cleanup: arango_rdf

  new `__pgt_add_to_adb_mapping` helper method, add restriction to property type relationship creation if contextualize_graph = True

  * fix: pgt case 2_4

  * more cleanup: arango_rdf

  * new: load RDF Predicates regardless of contextualize_graph value (PGT only)

  * update: test_adb_native_graph_to_rdf

  * attempt fix: missing coverage on L922

  coveralls seems to think this line is not covered by tests...

  * Update README.md

  * update docstrings

  * Update README.md

  * Update README.md

  * Update README.md

  * Update README.md

  * fix: flake8

  * Update README.md

  * new: notebook overhaul baseline

  * fix: process_val_as_string

  * remove: unused func

  * fix: p_already_has_dr

  * new: __get_literal_val

  * update: __get_literal_val

  * fix: subgraph names

  * cp: adb_key_uri

  * cleanup: arango_rdf

  * update: meta trig files

  * cleanup: arango_rdf

  * update: tests

  * more cleanup

  * fix: flake8

  * new: ArangoRDFController

  * fix: isort

  * new: use_async (rdf to arangodb)

  * cleanup

  * update test params

  * update: test case 7

  * cleanup: insert_adb_docs

  * update: tests

  * cleanup

  * new: ArangoRDF.ipynb output file

  * revert: d2277fa7f66a04d148b23ce04d9ad92db598f97c

  * new: game of thrones dump

  * update: tests

  * cp: arango_rdf

  * update notebook

  * new: cases 8-15 in notebook

  * new: rdf-star support for rpt

  * Revert "new: rdf-star support for rpt"

  This reverts commit 2a0ae04c445ba21f254de7927375a771b43abd65.

  * checkpoint

  rdf-star support prototyping,

  * cleanup: adb to rdf

  * new: rdf_statement_blacklist

  * discard "List" collection for pgt

  * new: __get_adb_edge_key

  * cleanup

  * checkpoint

  * cleanup

  * new: rdf star cases (8 to 15)

  * new: individualize RPT tests

  * Update ArangoRDF.ipynb

  * cleanup

  * new: hash adb edge ids

  * update: rdf-star support workaround

  * new: test cases 8-15 (pgt)

  * update notebook

  * cleanup

  * actions: use ArangoDB 3.11

  * fix notebook

  * cleanup

  * Update setup.py

  * new: design doc

  template used: https://github.com/arangodb/documents/blob/master/DesignDocuments/DesignDocumentTemplate.md

  * new: simplify_reified_triples flag

  * new: keyify_literals (rpt)

  minor cleanup

  * rework: batch_size (adb to rdf)

  * use batch_size in tests

  (adb to rdf & rdf to adb)

  * new: adb_key URI test case

  * cleanup based on feedback

  * fix: mypy

  * update build workflow

  * update release workflow

  * cleanup, todo comments

  * swap python 3.7 for 3.12

  * cleanup tests (case 1 & 6)

  * cleanup

  * migrate to `pyproject.toml`

  * fix lint

  * fix mypy

  * flake8 extend ignore

  trying to workaround 3.12 builds: https://github.com/ArangoDB-Community/ArangoRDF/actions/runs/6856708733/job/18644393745?pr=15

* Update ArangoRDF.ipynb. [Chris Woodward]

  Removes expected time message as it is much faster now


## 0.0.3 (2022-07-19)

### New

* Test suite, repo config updates, CICD via Actions (#9) [Anthony Mahanna]

  * new: pytest suite

  note: `test_full_cycle_aircraft_without_normalize_literals` is currently failing

  * new: tqdm progress bar, cleanup export, set `import_ontology` as out of service

  * new: CICD via Github Actions

  CodeQL, test automation, release automation

  * new: config files

  * remove: import_ontology

  * fix: rename `export` to `export_rdf`

  * fix: actions branch target

  * update: ground truth for test_main

  * cleanup: main.py

  housekeeping, refactoring, new helper methods, etc.

  * remove: init_ontology_collections

  references #6

  * Update .gitignore

  * remove: round timestamp

  * Update test_main.py

  * rename: export_rdf

  * Update README.md

  * Update README.md

  * new: README badges

### Other

* Fix typo. [Chris Woodward]

* Update ArangoRDF_output.ipynb. [Chris Woodward]

  Update branch name

* Update ArangoRDF.ipynb. [Chris Woodward]

  update branch name

* Example update (#7) [Chris Woodward]

  * adds new example data, updates README example, adds note about ontology issues

  * small README fixes, adds notebook

  * black formatting

* Update README.md. [Chris Woodward]

  Fixes example code to match recent codebase changes

* Update setup.py. [aMahanna]

* Update README.md. [aMahanna]


## 0.0.2 (2022-07-14)

### New

* Codebase cleanup (#5) [Anthony Mahanna]

  What's new:

  - Codebase housekeeping
  - README updates
  - new: custom Configurations
  - new: normalize literals functionality
  - new: default_graph and sub_graph properties in constructor

  All changes were discussed and reviewed with @ArthurKeen and @cw00dw0rd


## 0.0.1.dev0 (2022-07-07)

### New

* Constructor cleanup (#4) [Anthony Mahanna]

### Other

* Update MANIFEST.in. [aMahanna]

* Update README.md. [Chris Woodward]

* Initial Merge (#1) [Chris Woodward]

  * initial commit for topology preserving rdf import

  * added test data and file. also added s to arangoSemantics

  * removed file with old name

  * added ontology import method

  * modified test.py file to test both target variants.  added a metadata.ttl file containing rdf/rdfs/owl/skos metamodels

  * Added code to test the loading of the RDFS/OWL/SKOS metadata, added condition to add xsd:string as a type to string without a type

  * separate rdfowl and skos in metadata folder and tested

  * added method for exporting to a file

  * cleaned up test.py

  * cleaned up code and added doc strings

  * ArangoRDF Refactoring (#2)

  * initial refactoring

  project structuring, code formatting, basic test suite, README, setup.py, etc.

  * delete: rdfExport.ttl

  * Update ArangoRDF.ipynb

  * new: utf8 encoding

  * Update README.md

  * update to localhost

  * new: code authors

  * Update setup.py

* Initial commit. [Chris Woodward]


