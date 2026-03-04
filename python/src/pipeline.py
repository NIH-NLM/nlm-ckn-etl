#!/usr/bin/env python3
"""Prefect ETL pipeline for the NLM-CKN project.

Reads external API data (produced by ``fetcher.py``) from S3 or the local
filesystem, processes it through NSForest and author-to-CL tuple writers,
loads the results into ArangoDB, and archives the outputs to S3.

Prerequisites
-------------
Run ``fetcher.py`` first (or ensure ``data/external/`` contains fresh cache
files), then::

    cd python
    python src/pipeline.py --help
    python src/pipeline.py --run-ontology
    python src/pipeline.py --run-results
    python src/pipeline.py --run-archive
    python src/pipeline.py --run-ontology --run-results --run-archive

Or with the Prefect CLI after ``prefect server start``::

    prefect deployment run 'nlm-ckn-etl/local'

S3 mode
-------
Set ``S3_BUCKET`` to pull inputs from S3 before processing and push outputs
(tuples, archives) to S3 after each stage::

    S3_BUCKET=cell-kn-arangodb-data-952291113202 python src/pipeline.py --run-results

See the README for full instructions.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

from prefect import flow, get_run_logger, task

from _common import (
    ARANGO_DB_HOME,
    ARANGO_DB_HOST,
    ARANGO_DB_PORT,
    CLASSPATH,
    DEFAULT_JAVA_OPTS,
    REPO_ROOT,
    S3_BUCKET,
    _arango_net_args,
    _get_arangodb_id,
    _get_or_create_arango_password,
    _run_python_container,
    _s3_cp,
    _s3_sync,
    build_python_docker_image,
    sync_external_from_s3,
    validate_external_files,
)

# ── Tasks ──────────────────────────────────────────────────────────────────


@task(name="stop-arangodb", log_prints=True)
def stop_arangodb() -> None:
    """Stop and remove the running ArangoDB container, if any."""
    logger = get_run_logger()
    cid = _get_arangodb_id()
    if cid:
        logger.info(f"Stopping ArangoDB container {cid}")
        subprocess.run(
            ["docker", "container", "stop", cid], check=True, capture_output=True
        )
        subprocess.run(
            ["docker", "container", "rm", cid], check=True, capture_output=True
        )
    else:
        logger.info("No running ArangoDB container found")


@task(name="start-arangodb", log_prints=True)
def start_arangodb(
    arango_db_home: str, arango_db_port: int, arango_db_password: str
) -> None:
    """Start the ArangoDB container with the data directory mounted."""
    logger = get_run_logger()
    if _get_arangodb_id():
        logger.info("ArangoDB container already running")
        return
    Path(arango_db_home).mkdir(parents=True, exist_ok=True)
    logger.info(f"Starting ArangoDB (home={arango_db_home}, port={arango_db_port})")
    subprocess.run(
        [
            "docker", "run",
            "-e", f"ARANGO_ROOT_PASSWORD={arango_db_password}",
            "-p", f"{arango_db_port}:{arango_db_port}",
            "-d",
            "-v", f"{arango_db_home}:/var/lib/arangodb3",
            "arangodb",
        ],
        check=True,
        capture_output=True,
    )
    logger.info("ArangoDB container started")


@task(name="require-arangodb", log_prints=True)
def require_arangodb() -> str | None:
    """Return the ArangoDB container ID, or ``None`` for a remote instance.

    Remote mode (``ARANGO_DB_HOST`` != ``"localhost"``): ArangoDB is
    managed externally (e.g. a dedicated EC2 instance).  ``None`` is returned
    and callers skip the ``--network container:…`` Docker flag, relying on the
    injected ``ARANGO_DB_HOST`` env var instead.

    Local mode: raises ``RuntimeError`` if no ArangoDB container is running.
    """
    logger = get_run_logger()
    if ARANGO_DB_HOST != "localhost":
        logger.info(f"Remote ArangoDB mode: host={ARANGO_DB_HOST}, port={ARANGO_DB_PORT}")
        return None
    cid = _get_arangodb_id()
    if not cid:
        raise RuntimeError(
            "ArangoDB container is not running. "
            "Start it first or run the ontology stage."
        )
    logger.info(f"Local ArangoDB container: {cid}")
    return cid


@task(name="maven-build", log_prints=True)
def maven_build() -> None:
    """Run ``mvn clean package -DskipTests`` inside a Maven Docker container."""
    logger = get_run_logger()
    logger.info("Building Maven package (maven:3-eclipse-temurin-21)")
    subprocess.run(
        [
            "docker", "run", "--rm",
            "-v", f"{REPO_ROOT}:/app",
            "-v", f"{Path.home() / '.m2'}:/root/.m2",
            "-w", "/app",
            "maven:3-eclipse-temurin-21",
            "mvn", "clean", "package", "-DskipTests",
        ],
        check=True,
        cwd=REPO_ROOT,
    )
    jar = REPO_ROOT / CLASSPATH
    if not jar.exists():
        raise FileNotFoundError(f"JAR not found at {jar} after Maven build")
    logger.info(f"Maven build successful: {jar}")


@task(name="download-ontologies", log_prints=True)
def download_ontologies(java_opts: str = DEFAULT_JAVA_OPTS) -> None:
    """Run OntologyDownloader to fetch OWL files into data/obo/."""
    logger = get_run_logger()
    logger.info(f"Downloading ontologies (gov.nih.nlm.OntologyDownloader, {java_opts})")
    subprocess.run(
        [
            "docker", "run", "--rm",
            "-v", f"{REPO_ROOT}:/app",
            "-w", "/app",
            "eclipse-temurin:21",
            "java", java_opts, "-cp", CLASSPATH, "gov.nih.nlm.OntologyDownloader",
        ],
        check=True,
        cwd=REPO_ROOT,
    )
    owl_files = list((REPO_ROOT / "data" / "obo").glob("*.owl"))
    if not owl_files:
        raise FileNotFoundError("No OWL files found in data/obo/ after OntologyDownloader")
    logger.info(f"Downloaded {len(owl_files)} OWL file(s) to data/obo/")


@task(name="build-ontology-graph", log_prints=True)
def build_ontology_graph(
    arangodb_id: str | None,
    arango_db_port: int,
    arango_db_password: str,
    java_opts: str = DEFAULT_JAVA_OPTS,
) -> None:
    """Run OntologyGraphBuilder to load OWL triples into ArangoDB."""
    logger = get_run_logger()
    logger.info(f"Building ontology graph (gov.nih.nlm.OntologyGraphBuilder, {java_opts})")
    subprocess.run(
        [
            "docker", "run", "--rm",
            "-v", f"{REPO_ROOT}:/app",
            "-w", "/app",
            *_arango_net_args(arangodb_id),
            "-e", f"ARANGO_DB_HOST={ARANGO_DB_HOST}",
            "-e", f"ARANGO_DB_PORT={arango_db_port}",
            "-e", "ARANGO_DB_USER=root",
            "-e", f"ARANGO_DB_PASSWORD={arango_db_password}",
            "eclipse-temurin:21",
            "java", java_opts, "-cp", CLASSPATH, "gov.nih.nlm.OntologyGraphBuilder",
        ],
        check=True,
        cwd=REPO_ROOT,
    )
    logger.info("Ontology graph built")


@task(name="validate-results-sources", log_prints=True)
def validate_results_sources() -> None:
    """Fail fast if results-sources config points to missing NSForest directories.

    ``collect_results_sources_data()`` inside ``ExternalApiResultsFetcher.py``
    reads ``data/results-sources-*.json`` to locate NSForest result CSV files.
    If those directories don't exist, it silently returns empty collections and
    the pipeline completes without error but produces an empty results graph.

    This task catches that condition before any expensive Docker containers start
    and raises a clear ``FileNotFoundError`` listing exactly which directories
    are missing and which config file declared them.

    Paths in the JSON are relative to ``python/src/`` (the script's working
    directory inside the container, which mirrors the repo root via the volume
    mount), so they are resolved relative to ``python/src/`` on the host.
    """
    logger = get_run_logger()
    python_src = REPO_ROOT / "python" / "src"
    sources_files = sorted((REPO_ROOT / "data").glob("results-sources-*.json"))

    if not sources_files:
        raise FileNotFoundError(
            "No data/results-sources-*.json files found in the repo.\n"
            "Create at least one to tell the pipeline where NSForest results live."
        )

    missing: list[str] = []
    for src_file in sources_files:
        try:
            entries = json.loads(src_file.read_text())
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON in {src_file.name}: {exc}") from exc

        for entry in entries:
            raw = entry.get("nsforest_dirpath", "")
            dirpath = (python_src / raw).resolve()
            if not dirpath.is_dir():
                missing.append(f"  {dirpath}  (declared in {src_file.name} as '{raw}')")
            else:
                csvs = list(dirpath.rglob("*.csv"))
                if not csvs:
                    missing.append(
                        f"  {dirpath}  (exists but contains no .csv files; "
                        f"declared in {src_file.name} as '{raw}')"
                    )
                else:
                    logger.info(
                        f"OK: {dirpath.relative_to(REPO_ROOT)} "
                        f"({len(csvs)} CSV file(s))"
                    )

    if missing:
        raise FileNotFoundError(
            "The following NSForest results directories are missing or empty.\n"
            "Sync them from S3 (set S3_BUCKET) or upload them manually:\n"
            + "\n".join(missing)
        )

    logger.info(
        f"Results sources validated: {len(sources_files)} config file(s), "
        "all referenced NSForest directories present with CSV files"
    )


@task(name="sync-results-from-s3", log_prints=True)
def sync_results_from_s3() -> None:
    """Pull NSForest results CSVs from S3 into ``data/``.

    The NSForest directories are declared in ``data/results-sources-*.json``
    with paths like ``../../data/results-2026-01-06``.  S3 is expected to
    mirror this layout under the ``results/`` prefix, e.g.::

        s3://bucket/results/results-2026-01-06/...

    Syncing ``s3://bucket/results/`` → ``data/`` reproduces the expected
    local structure.  No-op when ``S3_BUCKET`` is empty (local-only mode).
    """
    logger = get_run_logger()
    if not S3_BUCKET:
        logger.info("S3_BUCKET not set — skipping S3 sync (local mode)")
        return
    data_dir = REPO_ROOT / "data"
    logger.info(f"Syncing s3://{S3_BUCKET}/results/ → data/")
    _s3_sync(f"s3://{S3_BUCKET}/results/", str(data_dir))
    logger.info("NSForest results synced from S3")


@task(name="write-nsforest-tuples", log_prints=True)
def write_nsforest_tuples(arangodb_id: str | None, arango_db_password: str) -> None:
    """Run NSForestResultsTupleWriter.py to create JSON tuples from NSForest results."""
    logger = get_run_logger()
    logger.info("Writing NSForest result tuples (NSForestResultsTupleWriter)")
    _run_python_container(
        "NSForestResultsTupleWriter.py", arangodb_id, arango_db_password
    )
    logger.info("NSForest tuples written")


@task(name="write-author-to-cl-tuples", log_prints=True)
def write_author_to_cl_tuples(arangodb_id: str | None, arango_db_password: str) -> None:
    """Run AuthorToClResultsTupleWriter.py to create JSON tuples from author-CL mappings."""
    logger = get_run_logger()
    logger.info("Writing author-to-CL result tuples (AuthorToClResultsTupleWriter)")
    _run_python_container(
        "AuthorToClResultsTupleWriter.py", arangodb_id, arango_db_password
    )
    logger.info("Author-to-CL tuples written")


@task(name="write-external-api-tuples", log_prints=True)
def write_external_api_tuples(arangodb_id: str | None, arango_db_password: str) -> None:
    """Run ExternalApiResultsTupleWriter.py to create JSON tuples from external API data."""
    logger = get_run_logger()
    logger.info("Writing external API result tuples (ExternalApiResultsTupleWriter)")
    _run_python_container(
        "ExternalApiResultsTupleWriter.py", arangodb_id, arango_db_password
    )
    logger.info("External API tuples written")


@task(name="sync-tuples-to-s3", log_prints=True)
def sync_tuples_to_s3() -> None:
    """Push tuple JSON files from ``data/tuples/`` to S3.

    No-op when ``S3_BUCKET`` is empty (local-only mode).
    """
    logger = get_run_logger()
    if not S3_BUCKET:
        logger.info("S3_BUCKET not set — skipping S3 sync (local mode)")
        return
    tuples_dir = REPO_ROOT / "data" / "tuples"
    logger.info(f"Syncing data/tuples/ → s3://{S3_BUCKET}/tuples/")
    _s3_sync(str(tuples_dir), f"s3://{S3_BUCKET}/tuples/")
    logger.info("Tuples pushed to S3")


@task(name="validate-tuple-files", log_prints=True)
def validate_tuple_files() -> None:
    """Raise an error if no JSON files were produced in data/tuples/."""
    logger = get_run_logger()
    tuples_dir = REPO_ROOT / "data" / "tuples"
    json_files = list(tuples_dir.glob("*.json"))
    if not json_files:
        raise FileNotFoundError(
            "No JSON files in data/tuples/ after Python tuple writers ran.\n"
            "Ensure data/results-sources-*.json points to existing NSForest results."
        )
    logger.info(f"Tuple files: {len(json_files)} JSON file(s) in data/tuples/")


@task(name="build-results-graph", log_prints=True)
def build_results_graph(
    arangodb_id: str | None,
    arango_db_port: int,
    arango_db_password: str,
    java_opts: str = DEFAULT_JAVA_OPTS,
) -> None:
    """Run ResultsGraphBuilder to load result tuples into ArangoDB."""
    logger = get_run_logger()
    logger.info(f"Building results graph (gov.nih.nlm.ResultsGraphBuilder, {java_opts})")
    subprocess.run(
        [
            "docker", "run", "--rm",
            "-v", f"{REPO_ROOT}:/app",
            "-w", "/app",
            *_arango_net_args(arangodb_id),
            "-e", f"ARANGO_DB_HOST={ARANGO_DB_HOST}",
            "-e", f"ARANGO_DB_PORT={arango_db_port}",
            "-e", "ARANGO_DB_USER=root",
            "-e", f"ARANGO_DB_PASSWORD={arango_db_password}",
            "eclipse-temurin:21",
            "java", java_opts, "-cp", CLASSPATH, "gov.nih.nlm.ResultsGraphBuilder",
        ],
        check=True,
        cwd=REPO_ROOT,
    )
    logger.info("Results graph built")


@task(name="build-phenotype-graph", log_prints=True)
def build_phenotype_graph(
    arangodb_id: str | None,
    arango_db_port: int,
    arango_db_password: str,
    java_opts: str = DEFAULT_JAVA_OPTS,
) -> None:
    """Run PhenotypeGraphBuilder to build the phenotype subgraph in ArangoDB."""
    logger = get_run_logger()
    logger.info(f"Building phenotype graph (gov.nih.nlm.PhenotypeGraphBuilder, {java_opts})")
    subprocess.run(
        [
            "docker", "run", "--rm",
            "-v", f"{REPO_ROOT}:/app",
            "-w", "/app",
            *_arango_net_args(arangodb_id),
            "-e", f"ARANGO_DB_HOST={ARANGO_DB_HOST}",
            "-e", f"ARANGO_DB_PORT={arango_db_port}",
            "-e", "ARANGO_DB_USER=root",
            "-e", f"ARANGO_DB_PASSWORD={arango_db_password}",
            "eclipse-temurin:21",
            "java", java_opts, "-cp", CLASSPATH, "gov.nih.nlm.PhenotypeGraphBuilder",
        ],
        check=True,
        cwd=REPO_ROOT,
    )
    logger.info("Phenotype graph built")


@task(name="create-analyzers-and-views", log_prints=True)
def create_analyzers_and_views(arangodb_id: str | None, arango_db_password: str) -> None:
    """Run CellKnSchemaUtilities.py to create ArangoDB analyzers and search views."""
    logger = get_run_logger()
    logger.info("Creating ArangoDB analyzers and views (CellKnSchemaUtilities)")
    _run_python_container(
        "CellKnSchemaUtilities.py", arangodb_id, arango_db_password
    )
    logger.info("Analyzers and views created")


@task(name="make-archives", log_prints=True)
def make_archives(arango_db_home: str) -> None:
    """Create obo.tar.gz (OWL files, external data, tuples) and arangodb.tar.gz."""
    logger = get_run_logger()
    logger.info("Creating archives")

    obo_paths = ["data/obo"]
    if (REPO_ROOT / "data" / "external").is_dir():
        obo_paths.append("data/external")
    if (REPO_ROOT / "data" / "tuples").is_dir():
        obo_paths.append("data/tuples")

    subprocess.run(
        ["tar", "-czf", "obo.tar.gz"] + obo_paths,
        check=True,
        cwd=REPO_ROOT,
    )
    logger.info(f"Created obo.tar.gz ({', '.join(obo_paths)})")

    arango_home = Path(arango_db_home)
    subprocess.run(
        [
            "tar", "-czf", "arangodb.tar.gz",
            "-C", str(arango_home.parent),
            arango_home.name,
        ],
        check=True,
        cwd=REPO_ROOT,
    )
    logger.info("Created arangodb.tar.gz")


@task(name="upload-archives-to-s3", log_prints=True)
def upload_archives_to_s3() -> None:
    """Rename and upload versioned archives to S3, superseding ``upload.sh``.

    Uploads to ``s3://${S3_BUCKET}/YYYY-MM-DD/`` with filenames that embed
    the Java and Python package versions, e.g.::

        obo-1.0-0.1.0.tar.gz
        arangodb-1.0-0.1.0.tar.gz
        build-info.txt

    ``build-info.txt`` includes the fetch metadata from
    ``data/external/fetch-info.json`` if it exists (written by ``fetcher.py``).

    No-op when ``S3_BUCKET`` is empty (local-only mode).
    """
    logger = get_run_logger()
    if not S3_BUCKET:
        logger.info("S3_BUCKET not set — skipping archive upload (local mode)")
        return

    # ── Check archives exist ───────────────────────────────────────────────
    obo_archive = REPO_ROOT / "obo.tar.gz"
    arango_archive = REPO_ROOT / "arangodb.tar.gz"
    for path in (obo_archive, arango_archive):
        if not path.exists():
            raise FileNotFoundError(
                f"{path.name} not found — run the archive stage first"
            )

    # ── Extract version strings ────────────────────────────────────────────
    pom = (REPO_ROOT / "pom.xml").read_text()
    java_version = re.search(r"<version>([^<]+)</version>", pom).group(1).strip()

    pyproject = (REPO_ROOT / "python" / "pyproject.toml").read_text()
    py_version = re.search(r'^version\s*=\s*"([^"]+)"', pyproject, re.MULTILINE).group(1).strip()

    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=REPO_ROOT, text=True
        ).strip()
    except Exception:
        commit = "unknown"

    date_stamp = datetime.now().strftime("%Y-%m-%d")
    version = f"{java_version}-{py_version}"
    s3_prefix = f"s3://{S3_BUCKET}/{date_stamp}"

    logger.info(f"Java version:   {java_version}")
    logger.info(f"Python version: {py_version}")
    logger.info(f"Commit hash:    {commit}")
    logger.info(f"S3 prefix:      {s3_prefix}/")

    # ── Build build-info.txt (merge fetch-info.json if available) ─────────
    build_info_lines = [
        f"Date:           {date_stamp}",
        f"Commit:         {commit}",
        f"Java version:   {java_version}",
        f"Python version: {py_version}",
    ]
    fetch_info_path = REPO_ROOT / "data" / "external" / "fetch-info.json"
    if fetch_info_path.exists():
        try:
            fetch_info = json.loads(fetch_info_path.read_text())
            build_info_lines += [
                f"Fetched at:     {fetch_info.get('fetched_at', 'unknown')}",
                f"Fetch commit:   {fetch_info.get('commit', 'unknown')}",
            ]
            for fname, size in fetch_info.get("files", {}).items():
                size_str = f"{size:,} bytes" if size is not None else "missing"
                build_info_lines.append(f"  {fname}: {size_str}")
        except Exception as exc:
            logger.warning(f"Could not read fetch-info.json: {exc}")

    build_info_path = REPO_ROOT / "build-info.txt"
    build_info_path.write_text("\n".join(build_info_lines) + "\n")

    # ── Rename archives with version suffix ───────────────────────────────
    versioned_obo = REPO_ROOT / f"obo-{version}.tar.gz"
    versioned_arango = REPO_ROOT / f"arangodb-{version}.tar.gz"
    obo_archive.rename(versioned_obo)
    arango_archive.rename(versioned_arango)

    # ── Upload to S3 ──────────────────────────────────────────────────────
    for local, remote_name in [
        (versioned_obo, versioned_obo.name),
        (versioned_arango, versioned_arango.name),
        (build_info_path, "build-info.txt"),
    ]:
        dst = f"{s3_prefix}/{remote_name}"
        logger.info(f"Uploading {local.name} → {dst}")
        _s3_cp(str(local), dst)

    logger.info(f"Archives uploaded to {s3_prefix}/")


# ── Flow ───────────────────────────────────────────────────────────────────


@flow(name="nlm-ckn-etl", log_prints=True)
def nlm_ckn_etl(
    run_ontology: bool = False,
    force_ontology: bool = False,
    run_results: bool = False,
    force_results: bool = False,
    run_archive: bool = False,
    force_archive: bool = False,
    java_opts: str = DEFAULT_JAVA_OPTS,
) -> None:
    """NLM-CKN ETL pipeline — orchestrated with Prefect.

    Reads external API data produced by ``fetcher.py`` from S3 or the local
    filesystem, processes it into ArangoDB graph data, and archives the
    outputs.  At least one stage flag must be ``True``.

    Sentinel files in the repo root prevent redundant re-runs:

    - ``.built-ontology``  — written after the ontology stage completes
    - ``.built-results``   — written after the results stage completes
    - ``.archived``        — written after archives are created

    Parameters
    ----------
    run_ontology:
        Build the ontology graph if it has not been built yet.
    force_ontology:
        Force a full rebuild of the ontology graph (clears ArangoDB data).
    run_results:
        Build the results and phenotype graphs if not already built.
    force_results:
        Force a full rebuild of the results and phenotype graphs.
    run_archive:
        Create obo.tar.gz and arangodb.tar.gz and upload to S3 if not done.
    force_archive:
        Force re-creation and re-upload of the archives.
    java_opts:
        JVM flags passed to every Java container (default: ``-Xmx2g``).
        Increase (e.g. ``-Xmx4g``) if you have enough Docker memory, or
        decrease if the container is OOM-killed (exit 137).
    """
    logger = get_run_logger()

    if not any(
        [run_ontology, force_ontology, run_results, force_results, run_archive, force_archive]
    ):
        logger.warning(
            "No stage flags set — nothing to do.  Pass at least one of: "
            "run_ontology, force_ontology, run_results, force_results, "
            "run_archive, force_archive."
        )
        return

    arango_db_password = _get_or_create_arango_password()
    arango_db_home = ARANGO_DB_HOME

    if S3_BUCKET:
        logger.info(f"S3 mode: bucket={S3_BUCKET}")
    else:
        logger.info("Local mode: S3_BUCKET not set")

    built_ontology_file = REPO_ROOT / ".built-ontology"
    built_results_file = REPO_ROOT / ".built-results"
    archived_file = REPO_ROOT / ".archived"

    # ── Ontology stage ─────────────────────────────────────────────────────
    if run_ontology or force_ontology:
        if built_ontology_file.exists() and not force_ontology:
            logger.info(
                "Ontology graph already built; use force_ontology=True to force rebuild"
            )
            logger.info(f"  {built_ontology_file.read_text().strip()}")
        else:
            logger.info("=== Ontology Stage ===")
            if ARANGO_DB_HOST == "localhost":
                # Local mode: manage the ArangoDB container lifecycle and wipe
                # the data directory so OntologyGraphBuilder starts from scratch.
                stop_arangodb()
                arango_home = Path(arango_db_home)
                if arango_home.exists():
                    shutil.rmtree(arango_home)
                start_arangodb(arango_db_home, ARANGO_DB_PORT, arango_db_password)
            else:
                # Remote mode: ArangoDB is managed externally.  The caller is
                # responsible for wiping the database if a clean rebuild is needed.
                logger.info(
                    f"Remote ArangoDB at {ARANGO_DB_HOST}:{ARANGO_DB_PORT} — "
                    "skipping container start/stop and data-dir wipe"
                )
            arangodb_id = require_arangodb()
            maven_build()
            download_ontologies(java_opts)
            build_ontology_graph(arangodb_id, ARANGO_DB_PORT, arango_db_password, java_opts)
            msg = f"Built ontology graph on {datetime.now()}"
            built_ontology_file.write_text(msg)
            archived_file.unlink(missing_ok=True)
            logger.info(msg)

    # ── Results stage ──────────────────────────────────────────────────────
    if run_results or force_results:
        if built_results_file.exists() and not force_results:
            logger.info(
                "Results and phenotype graphs already built; "
                "use force_results=True to force rebuild"
            )
            logger.info(f"  {built_results_file.read_text().strip()}")
        else:
            logger.info("=== Results Stage ===")

            # Pull inputs from S3 (no-op in local mode)
            sync_results_from_s3()     # NSForest CSVs
            sync_external_from_s3()    # external API cache from fetcher.py

            validate_results_sources()
            validate_external_files()  # assert fetcher.py ran before this stage

            arangodb_id = require_arangodb()
            build_python_docker_image()

            tuples_dir = REPO_ROOT / "data" / "tuples"
            tuples_dir.mkdir(parents=True, exist_ok=True)
            for f in tuples_dir.glob("*.json"):
                f.unlink()

            write_nsforest_tuples(arangodb_id, arango_db_password)
            write_author_to_cl_tuples(arangodb_id, arango_db_password)
            write_external_api_tuples(arangodb_id, arango_db_password)

            sync_tuples_to_s3()        # persist tuple output
            validate_tuple_files()

            maven_build()
            build_results_graph(arangodb_id, ARANGO_DB_PORT, arango_db_password, java_opts)
            build_phenotype_graph(arangodb_id, ARANGO_DB_PORT, arango_db_password, java_opts)
            create_analyzers_and_views(arangodb_id, arango_db_password)

            msg = f"Built results and phenotype graphs on {datetime.now()}"
            built_results_file.write_text(msg)
            archived_file.unlink(missing_ok=True)
            logger.info(msg)

    # ── Archive stage ──────────────────────────────────────────────────────
    if run_archive or force_archive:
        if archived_file.exists() and not force_archive:
            logger.info(
                "Archives already made; use force_archive=True to force re-archive"
            )
            logger.info(f"  {archived_file.read_text().strip()}")
        else:
            logger.info("=== Archive Stage ===")
            make_archives(arango_db_home)
            upload_archives_to_s3()    # no-op if S3_BUCKET unset
            msg = f"Archived obo and arangodb on {datetime.now()}"
            archived_file.write_text(msg)
            logger.info(msg)


# ── CLI entry point ────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="NLM-CKN ETL pipeline (Prefect)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "-o", "--run-ontology",
        action="store_true",
        help="Build the ontology graph if not already built",
    )
    parser.add_argument(
        "-O", "--force-ontology",
        action="store_true",
        help="Force rebuild of the ontology graph",
    )
    parser.add_argument(
        "-r", "--run-results",
        action="store_true",
        help="Build the results and phenotype graphs if not already built",
    )
    parser.add_argument(
        "-R", "--force-results",
        action="store_true",
        help="Force rebuild of the results and phenotype graphs",
    )
    parser.add_argument(
        "-a", "--run-archive",
        action="store_true",
        help="Create and upload archives if not already done",
    )
    parser.add_argument(
        "-A", "--force-archive",
        action="store_true",
        help="Force re-creation and re-upload of archives",
    )
    parser.add_argument(
        "--java-opts",
        default=DEFAULT_JAVA_OPTS,
        help=(
            f"JVM flags for Java containers (default: '{DEFAULT_JAVA_OPTS}'). "
            "Lower -Xmx if containers are OOM-killed (exit 137), or raise "
            "Docker Desktop memory in Settings → Resources → Memory first."
        ),
    )
    args = parser.parse_args()

    nlm_ckn_etl(
        run_ontology=args.run_ontology,
        force_ontology=args.force_ontology,
        run_results=args.run_results,
        force_results=args.force_results,
        run_archive=args.run_archive,
        force_archive=args.force_archive,
        java_opts=args.java_opts,
    )
