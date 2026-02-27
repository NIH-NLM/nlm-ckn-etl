# NLM-CKN Extraction, Translation, and Loading

## Motivation

The National Library of Medicint (NLM) Cell Knowledge Network
(NLM-CKN) pilot aims to create a comprehensive cell phenotype
knowledge network that integrates knowledge about diseases and drugs
to facilitate discovery of new biomarkers and therapeutic targets. To
maximize interoperability of the derived knowledge with knowledge
about genes, pathways, diseases, and drugs from other NLM/NCBI
resources, the knowledge will be derived in the form of
semantically-structured assertions of subject-predicate-object triple
statements which are compatible with semantic web technologies, and
storage using graph databases, such as the
[ArangoDB](https://arangodb.com/) database system.

The NLM-CKN captures single cell genomics data from existing data
repositories, such as CELLxGENE, and uses NSForest to identify cell
type-specific marker genes. The cell types are manually mapped to the
Cell Ontology (CL), and the marker genes are linked to data from
external sources, such as Open Targets, to provide relationships to
diseases and drugs. In addition, the Cell KN uses natural language
processing to extract information about cell type-specific marker
genes, and their association with disease state from open access
peer-reviewed publications.

## Purpose

This repository provides:

- **A Java package** for parsing ontology OWL files, loading semantic
  triples into an ArangoDB instance, and identifying relevant
  subgraphs
- **Python modules** for parsing and loading ontologies, fetching data
  from external sources, and creating semantic triples from NSForest
  results, manual CL mappings, external data, and NLP results

This is a unified repository that combines the previously separate
`cell-kn-mvp-etl-ontologies` and `cell-kn-mvp-etl-results`
repositories, eliminating the need for git submodules and
system-scoped JAR dependencies.

## Project Structure

```
cell-kn-mvp-etl/
├── pom.xml                          # Maven POM (all Java dependencies)
├── src/
│   ├── main/
│   │   ├── java/gov/nih/nlm/        # 10 Java classes
│   │   └── shell/                   # ArangoDB shell scripts
│   └── test/
│       ├── java/gov/nih/nlm/        # 7 Java test classes
│       └── data/
│           ├── obo/                 # Test .owl files
│           └── summaries/           # Test .json files
├── python/
│   ├── pyproject.toml               # Poetry configuration
│   ├── src/                         # Python modules
│   └── tests/                       # Python test files
└── docs/
    └── python/                      # Sphinx documentation
```

## Ontologies

All terms from the following ontologies have been selected for loading
into the NLM-CKN:

- [CL](http://purl.obolibrary.org/obo/cl.owl): Cell Ontology
- [GO](https://purl.obolibrary.org/obo/go/extensions/go-plus.owl): Gene Ontology
- [UBERON](http://purl.obolibrary.org/obo/uberon/uberon-base.owl): Uberon multi-species anatomy ontology
- [NCBITaxon](http://purl.obolibrary.org/obo/ncbitaxon/subsets/taxslim.owl): NCBI organismal taxonomy
- [MONDO](http://purl.obolibrary.org/obo/mondo/mondo-simple.owl): Mondo Disease Ontology
- [HP]("http://purl.obolibrary.org/obo/hp.owl"): Human Phenotype Ontology
- [PATO](http://purl.obolibrary.org/obo/pato.owl): Phenotype And Trait Ontology
- [HsapDv](http://purl.obolibrary.org/obo/hsapdv.owl): Human Developmental Stages

## External Sources

Data can be fetched from the following external sources:

- [Open Targets](https://www.opentargets.org/): Includes diseases,
  drugs, interactions, pharmacogenetics, tractability, expression, and
  depmap resources
- [Gene](https://www.ncbi.nlm.nih.gov/gene/): Records include
  nomenclature, Reference Sequences (RefSeqs), maps, pathways,
  variations, phenotypes, and links to genome-, phenotype-, and
  locus-specific resources
- [UniProt](https://www.uniprot.org/): Includes protein sequence, and
  functional information resources

## Dependencies

### Docker

Install [Docker Desktop](https://docs.docker.com/desktop/).

### ArangoDB

An ArangoDB docker image can be downloaded and a container started as
follows (some environment variables assumed below):
```
$ export ARANGO_DB_HOST=127.0.0.1
$ export ARANGO_DB_PORT=8529
$ export ARANGO_DB_HOME="<some-path>/arangodb"
$ export ARANGO_DB_PASSWORD="<some-password>"
$ cd src/main/shell
$ ./start-arangodb.sh
```

### Java

Java SE 21 and Maven 3 or compatible are required to generate the
Javadocs, test, and package:
```
$ mvn javadoc:javadoc
$ mvn test
$ mvn clean package -DskipTests
```

### Data

The Python and Java classes require the ontology files to reside in
`data/obo`. Populate this directory as follows:
```
$ export CP="target/nlm-ckn-etl-1.0.jar"
$ java -cp $CP gov.nih.nlm.OntologyDownloader
```

### Python

Python 3.12 and Poetry are required to generate the Sphinx
documentation, test, and run. Install the dependencies as follows:
```
$ cd python
$ python3.12 -m venv .poetry
$ source .poetry/bin/activate
$ python -m pip install -r .poetry.txt
$ deactivate
$ python3.12 -m venv .venv
$ source .venv/bin/activate
$ .poetry/bin/poetry install
```
Generate the Sphinx documentation as follows:
```
$ cd docs/python
$ make clean html
```
Run Python tests as follows:
```
$ cd python/tests
$ python -m pytest *.py
```

## Usage

### ETL Pipeline Execution Order

1. **Download ontologies:**
   ```
   $ export CP="target/nlm-ckn-etl-1.0.jar"
   $ java -cp $CP gov.nih.nlm.OntologyDownloader
   ```

2. **Load ontologies into ArangoDB (Java):**
   ```
   $ export CP="target/cell-kn-mvp-etl-1.0.jar"
   $ java -cp $CP gov.nih.nlm.OntologyGraphBuilder
   ```

3. **Fetch external data:**
   ```
   $ cd python/src
   $ python ExternalApiResultsFetcher.py
   ```

4. **Create result tuples:**
   ```
   $ cd python/src
   $ python NSForestResultsTupleWriter.py
   $ python AuthorToClResultsTupleWriter.py
   $ python ExternalApiResultsTupleWriter.py
   ```

5. **Load results into ArangoDB (Java):**
   ```
   $ export CP="target/cell-kn-mvp-etl-1.0.jar"
   $ java -cp $CP gov.nih.nlm.ResultsGraphBuilder
   ```

6. **Select a relevant sub-graph (Java):**
   ```
   $ java -cp $CP gov.nih.nlm.PhenotypeGraphBuilder
   ```

7. **Create ArangoDB analyzers/views:**
   ```
   $ cd python/src
   $ python CellKnSchemaUtilities.py
   ```
