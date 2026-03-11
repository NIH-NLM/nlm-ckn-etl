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

1. **Download ontologies (Java):**
   ```
   $ export CP="target/nlm-ckn-etl-1.0.jar"
   $ java -cp $CP gov.nih.nlm.OntologyDownloader
   ```

2. **Load ontologies into ArangoDB (Java):**
   ```
   $ export CP="target/cell-kn-mvp-etl-1.0.jar"
   $ java -cp $CP gov.nih.nlm.OntologyGraphBuilder
   ```

3. **Fetch external data (Python):**
   ```
   $ cd python/src
   $ export NCBI_EMAIL="<some-email>"
   $ export NCBI_API_KEY="<some-api-key>"
   $ python ExternalApiResultsFetcher.py
   ```

4. **Create result tuples (Python):**
   ```
   $ cd python/src
   $ python NSForestResultsTupleWriter.py
   $ python AuthorToClResultsTupleWriter.py
   $ python ExternalApiResultsTupleWriter.py
   ```

5. **Load results into ArangoDB (Java):**
   ```
   $ export CP="target/nlm-ckn-etl-1.0.jar"
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

## Prefect Pipeline

`python/src/pipeline.py` is a [Prefect](https://www.prefect.io/) flow that
replaces `src/main/shell/etl.sh`.  It runs the same Docker commands but adds
per-step logging, retry support, and an optional Prefect UI.

### Prerequisites

- Docker Desktop running
- Python 3.10–3.12 with the project's Poetry environment (see **Python** above)
- `prefect` is listed in `pyproject.toml` and installed by `poetry install`

### Install Prefect

```bash
cd python
poetry install          # installs prefect along with all other dependencies
source .venv/bin/activate
```

### NCBI credentials (optional but recommended)

Copy the credentials template and fill in your values:

```bash
cp python/src/.zshenv.example python/src/.zshenv
# edit python/src/.zshenv — set NCBI_EMAIL and NCBI_API_KEY
source python/src/.zshenv   # or add to your shell profile
```

### Running locally (no Prefect server needed)

Run directly from the **repo root** using flags that mirror `etl.sh`:
lowercase letters check for an existing build first; uppercase force a full
rebuild.

```bash
# Full pipeline: ontology → results → archive
python python/src/pipeline.py -o -r -a

# Force rebuild everything
python python/src/pipeline.py -O -R -A

# Ontology stage only
python python/src/pipeline.py -o

# Results stage only (requires ArangoDB already running with ontology graph)
python python/src/pipeline.py -r

# Archive stage only
python python/src/pipeline.py -a

# Pass NCBI credentials explicitly (otherwise reads from $NCBI_EMAIL / $NCBI_API_KEY)
python python/src/pipeline.py -r --ncbi-email you@example.com --ncbi-api-key YOUR_KEY

# Show all options
python python/src/pipeline.py --help
```

### Running with the Prefect UI

Start the Prefect server in one terminal:

```bash
prefect server start
```

Then run the flow in another terminal — the run will appear in the UI at
`http://localhost:4200`:

```bash
python python/src/pipeline.py -o -r -a
```

### Sentinel files

The pipeline uses the same sentinel files as `etl.sh` to skip stages that
have already completed:

| File | Created after |
|---|---|
| `.built-ontology` | Ontology stage succeeds |
| `.built-results`  | Results stage succeeds |
| `.archived`       | Archive stage succeeds |

Delete a sentinel file (or use a force flag) to re-run the corresponding stage.
