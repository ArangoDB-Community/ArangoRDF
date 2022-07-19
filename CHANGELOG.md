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


