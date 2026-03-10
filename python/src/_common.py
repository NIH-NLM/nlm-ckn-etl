"""Shared constants, helpers, and Prefect tasks for the NLM-CKN ETL.

Imported by both ``fetcher.py`` (external API data collection) and
``pipeline.py`` (data processing and graph building) to avoid duplication.

Design note
-----------
All Python and Java scripts are invoked **directly** via ``subprocess`` using
the same interpreter / JRE that is already installed on the host (EC2 or
ECS Fargate task).  There are no Docker-in-Docker calls here.

- Python scripts run with ``sys.executable`` so they share the host's
  installed packages (cellxgene-census, scanpy, etc.).
- Java programs run with the ``java`` binary on ``PATH``; the JAR is either
  downloaded from S3 or built locally by CI/CD.
- ``PYTHONPATH`` is always set to ``python/src/`` so scripts can import
  sibling modules (``LoaderUtilities``, ``ArangoDbUtilities``, etc.).
"""

import json
import os
import secrets
import subprocess
import sys
from pathlib import Path

import docker as docker_sdk
from prefect import get_run_logger, task

# ── Constants ──────────────────────────────────────────────────────────────

REPO_ROOT = Path(__file__).parents[2]

# Relative path to the compiled JAR (from REPO_ROOT).
# The JAR is downloaded from S3 by ``ensure_jar()`` in pipeline.py, or
# built locally with ``mvn clean package -DskipTests`` for development.
CLASSPATH = "target/nlm-ckn-etl-1.0.jar"

# Default Java heap.  Raise with --java-opts if OOM-killed (exit 137).
DEFAULT_JAVA_OPTS = "-Xmx4g"

ARANGO_DB_HOST = os.getenv("ARANGO_DB_HOST", "localhost")
ARANGO_DB_PORT = int(os.getenv("ARANGO_DB_PORT", "8529"))
ARANGO_DB_HOME = os.getenv("ARANGO_DB_HOME", str(REPO_ROOT / "data" / "arangodb"))

# Host-side path for the ArangoDB data directory, used as the Docker volume
# source when starting the ArangoDB sibling container via the Docker socket.
#
# When the pipeline itself runs inside Docker (with /var/run/docker.sock
# mounted), volume paths passed to the Docker SDK are resolved by the HOST
# daemon.  ARANGO_DB_HOME is a container-internal path and therefore unknown
# to the host daemon — this causes a "path not shared from host" error.
#
# Two ways to resolve this:
#   1. Set ARANGO_DB_HOST_HOME to the host-side path that corresponds to
#      ARANGO_DB_HOME, e.g.:
#        -e ARANGO_DB_HOST_HOME=$(pwd)/data/arangodb
#      start_arangodb will bind-mount that path into the ArangoDB container.
#   2. Leave ARANGO_DB_HOST_HOME unset.  When running inside a container
#      (detected by /.dockerenv), start_arangodb falls back to the named
#      Docker volume "nlm-ckn-arangodb-data", which the host daemon manages
#      without needing a host path.
ARANGO_DB_HOST_HOME = os.getenv("ARANGO_DB_HOST_HOME", "")

# Named Docker volume used as the ArangoDB data volume when running inside a
# container without ARANGO_DB_HOST_HOME set.  The volume is created on first
# use and persists across pipeline runs.
ARANGO_DB_VOLUME_NAME = "nlm-ckn-arangodb-data"

# S3 bucket for durable storage of external cache, tuples, JAR, and archives.
# Empty string → local-only mode (no S3 operations performed).
S3_BUCKET = os.getenv("S3_BUCKET", "")

# PYTHONPATH injected into every direct Python script invocation so that
# sibling imports (LoaderUtilities, ArangoDbUtilities, …) resolve correctly.
PYTHON_SRC = str(REPO_ROOT / "python" / "src")


# ── Private helpers ────────────────────────────────────────────────────────


def _get_or_create_arango_password() -> str:
    """Read the ArangoDB root password from .arangodb-password, creating it on first run."""
    password_file = REPO_ROOT / ".arangodb-password"
    if password_file.exists():
        return password_file.read_text().strip()
    password = secrets.token_urlsafe(24)
    password_file.write_text(password)
    return password


def _get_arangodb_id() -> str | None:
    """Return the short container ID of a running ArangoDB container, or None.

    Uses the Docker SDK (no ``docker`` CLI binary required).  Returns ``None``
    if Docker is unreachable or no ArangoDB container is running.

    Checks in order:
    1. A container named exactly ``arangodb`` (the name we assign on start).
    2. Any running container built from the ``arangodb`` image (ancestor filter),
       as a fallback for containers started outside this script.
    """
    try:
        client = docker_sdk.from_env()
        # Primary: by name (fast, exact)
        named = client.containers.list(filters={"name": "arangodb", "status": "running"})
        if named:
            return named[0].short_id
        # Fallback: by image ancestor (catches containers with random names)
        by_image = client.containers.list(filters={"ancestor": "arangodb", "status": "running"})
        return by_image[0].short_id if by_image else None
    except docker_sdk.errors.DockerException:
        return None


def _arango_env(arango_db_password: str) -> dict[str, str]:
    """Return environment variables for ArangoDB connectivity.

    Injected into every Python script and Java program subprocess so they
    can reach the ArangoDB instance regardless of where it runs.
    """
    return {
        "ARANGO_DB_HOST": ARANGO_DB_HOST,
        "ARANGO_DB_PORT": str(ARANGO_DB_PORT),
        "ARANGO_DB_USER": "root",
        "ARANGO_DB_PASSWORD": arango_db_password,
    }


def _run_python_script(
    script: str,
    arango_db_password: str,
    extra_env: dict[str, str] | None = None,
    extra_args: list[str] | None = None,
) -> None:
    """Run a Python script directly using ``sys.executable``.

    The script runs in the same interpreter (and therefore the same installed
    packages) as the Prefect worker.  ``PYTHONPATH`` is set to ``python/src/``
    so scripts can ``import LoaderUtilities``, ``import ArangoDbUtilities``,
    etc. without modification.

    Parameters
    ----------
    script:
        Filename relative to ``python/src/`` (e.g. ``"ExternalApiResultsFetcher.py"``).
    arango_db_password:
        ArangoDB root password, forwarded as ``ARANGO_DB_PASSWORD``.
    extra_env:
        Additional environment variables to merge in (e.g. NCBI credentials).
    extra_args:
        Additional command-line arguments appended to the script invocation
        (e.g. ``["--force-all"]``).
    """
    env = {
        **os.environ,
        "PYTHONPATH": PYTHON_SRC,
        **_arango_env(arango_db_password),
        **(extra_env or {}),
    }
    subprocess.run(
        [sys.executable, str(REPO_ROOT / "python" / "src" / script), *(extra_args or [])],
        check=True,
        env=env,
    )


def _s3_sync(src: str, dst: str) -> None:
    """Sync ``src`` to ``dst`` via ``aws s3 sync``.

    No-op when ``S3_BUCKET`` is empty (local-only mode).
    """
    if not S3_BUCKET:
        return
    subprocess.run(["aws", "s3", "sync", src, dst], check=True)


def _s3_cp(src: str, dst: str) -> None:
    """Upload a single file via ``aws s3 cp``.

    No-op when ``S3_BUCKET`` is empty (local-only mode).
    """
    if not S3_BUCKET:
        return
    subprocess.run(["aws", "s3", "cp", src, dst], check=True)


# ── Shared tasks ───────────────────────────────────────────────────────────


@task(name="clean-empty-external-files", log_prints=True)
def clean_empty_external_files() -> None:
    """Remove corrupt or structurally invalid files from data/external/.

    ``ExternalApiResultsFetcher.py`` uses cache files in ``data/external/``
    to resume interrupted runs.  Two classes of bad files are cleaned here:

    1. **Zero-byte files** — causes ``JSONDecodeError`` on next load.

    2. **Structurally invalid cache files** — the fetcher writes a sentinel
       key into each cache file so the resume branch can reconstruct its
       working state.  A file without its sentinel raises ``KeyError``.

       Known sentinels:
       - ``gene.json``    → ``"gene_entrez_ids"``
       - ``uniprot.json`` → ``"protein_accessions"``
    """
    logger = get_run_logger()
    external_dir = REPO_ROOT / "data" / "external"
    external_dir.mkdir(parents=True, exist_ok=True)

    # 1. Remove zero-byte files
    removed = [f for f in external_dir.iterdir() if f.is_file() and f.stat().st_size == 0]
    if removed:
        for f in removed:
            f.unlink()
            logger.warning(f"Removed empty/corrupt external cache file: {f.name}")
        logger.info(f"Cleaned {len(removed)} empty file(s) from data/external/")
    else:
        logger.info("No empty files found in data/external/")

    # 2. Remove cache files missing their sentinel key
    sentinel_keys = {
        "gene.json": "gene_entrez_ids",
        "uniprot.json": "protein_accessions",
    }
    for filename, key in sentinel_keys.items():
        path = external_dir / filename
        if path.exists() and path.stat().st_size > 0:
            try:
                data = json.loads(path.read_text())
                if key not in data:
                    path.unlink()
                    logger.warning(
                        f"Removed data/external/{filename}: "
                        f"missing sentinel key '{key}' (would cause KeyError)"
                    )
            except json.JSONDecodeError:
                pass  # already handled by the zero-byte check above


@task(name="validate-external-files", log_prints=True)
def validate_external_files() -> None:
    """Verify that all required external cache files exist and contain valid JSON.

    Called by the fetch flow after fetching and by the pipeline flow after
    syncing from S3, ensuring TupleWriters never run against missing or
    corrupt inputs.

    Files checked: ``cellxgene.json``, ``opentargets.json``, ``gene.json``,
    ``uniprot.json``.
    """
    logger = get_run_logger()
    external_dir = REPO_ROOT / "data" / "external"
    required = ["cellxgene.json", "opentargets.json", "gene.json", "uniprot.json", "pubmed.json"]

    # Files where an empty dict ({}) is a valid state — e.g. pubmed.json is
    # legitimately empty when no author-to-CL mapping files exist in the data.
    may_be_empty = {"pubmed.json"}

    errors = []
    for filename in required:
        path = external_dir / filename
        if not path.exists():
            errors.append(f"  {filename} — file not found")
        elif path.stat().st_size == 0:
            errors.append(f"  {filename} — empty (zero bytes)")
        else:
            try:
                data = json.loads(path.read_text())
                if not data and filename not in may_be_empty:
                    logger.warning(
                        f"data/external/{filename} is valid JSON but contains no entries "
                        f"— annotations from this source will be skipped. "
                        f"Run fetcher.py to populate it."
                    )
                else:
                    logger.info(f"OK: data/external/{filename} ({path.stat().st_size:,} bytes)")
            except json.JSONDecodeError as exc:
                errors.append(f"  {filename} — invalid JSON: {exc}")

    if errors:
        raise RuntimeError(
            "Required external cache files are missing or invalid.\n"
            "Run fetcher.py first (or set S3_BUCKET so the pipeline can sync them):\n"
            + "\n".join(errors)
        )


@task(name="sync-external-from-s3", log_prints=True)
def sync_external_from_s3() -> None:
    """Restore the external API cache from S3 to ``data/external/``.

    No-op when ``S3_BUCKET`` is empty (local-only mode).  Used by both the
    fetch flow (to resume an interrupted run) and the pipeline flow (to pull
    the cache that the fetch flow produced).
    """
    logger = get_run_logger()
    if not S3_BUCKET:
        logger.info("S3_BUCKET not set — skipping S3 sync (local mode)")
        return
    external_dir = REPO_ROOT / "data" / "external"
    external_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Syncing s3://{S3_BUCKET}/external/ → data/external/")
    _s3_sync(f"s3://{S3_BUCKET}/external/", str(external_dir))
    logger.info("External cache restored from S3")


@task(name="sync-external-to-s3", log_prints=True)
def sync_external_to_s3() -> None:
    """Push the external API cache from ``data/external/`` to S3.

    No-op when ``S3_BUCKET`` is empty (local-only mode).
    """
    logger = get_run_logger()
    if not S3_BUCKET:
        logger.info("S3_BUCKET not set — skipping S3 sync (local mode)")
        return
    external_dir = REPO_ROOT / "data" / "external"
    logger.info(f"Syncing data/external/ → s3://{S3_BUCKET}/external/")
    _s3_sync(str(external_dir), f"s3://{S3_BUCKET}/external/")
    logger.info("External cache pushed to S3")
