"""Shared constants, helpers, and Prefect tasks for the NLM-CKN ETL.

Imported by both ``fetcher.py`` (external API data collection) and
``pipeline.py`` (data processing and graph building) to avoid duplication.
"""

import json
import os
import secrets
import subprocess
from pathlib import Path

from prefect import get_run_logger, task

# ── Constants ──────────────────────────────────────────────────────────────

REPO_ROOT = Path(__file__).parents[2]
CLASSPATH = "target/nlm-ckn-etl-1.0.jar"

# Default Java heap.  Override with --java-opts if the container is OOM-killed
# (exit 137).  Docker Desktop on Mac defaults to ~half of host RAM; keep this
# well below that limit.  Raise it in Docker Desktop → Settings → Resources →
# Memory if you have headroom.
DEFAULT_JAVA_OPTS = "-Xmx2g"

ARANGO_DB_HOST = os.getenv("ARANGO_DB_HOST", "localhost")
ARANGO_DB_PORT = int(os.getenv("ARANGO_DB_PORT", "8529"))
ARANGO_DB_HOME = os.getenv("ARANGO_DB_HOME", str(REPO_ROOT / "data" / "arangodb"))

# S3 bucket for durable storage of external cache, tuples, and archives.
# An empty string disables all S3 operations (local-only mode, suitable for
# development).  Set via the S3_BUCKET environment variable.
S3_BUCKET = os.getenv("S3_BUCKET", "")


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
    """Return the container ID of a running ArangoDB container, or None."""
    result = subprocess.run(
        ["docker", "ps"], capture_output=True, text=True, check=True
    )
    for line in result.stdout.splitlines():
        if "arangodb" in line:
            return line.split()[0]
    return None


def _arango_net_args(arangodb_id: str | None) -> list[str]:
    """Return Docker network flags for reaching ArangoDB.

    Local mode  (``arangodb_id`` is set): share the ArangoDB container's
    network namespace so that ``localhost:8529`` resolves correctly inside
    child containers.

    Remote mode (``arangodb_id`` is ``None``): no special network flags —
    ``ARANGO_DB_HOST`` is an external hostname reachable over the default
    bridge network.
    """
    if arangodb_id is not None:
        return ["--network", f"container:{arangodb_id}"]
    return []


def _run_python_container(
    script: str,
    arangodb_id: str | None,
    arango_db_password: str,
    ncbi_email: str = "",
    ncbi_api_key: str = "",
) -> None:
    """Run a Python script inside the nlm-ckn-etl-python Docker image.

    Local mode: the container shares the ArangoDB container's network
    namespace so that ``localhost:8529`` resolves to ArangoDB.

    Remote mode: ``ARANGO_DB_HOST`` / ``ARANGO_DB_PORT`` are injected as
    environment variables so the script connects to the external ArangoDB
    instance.
    """
    subprocess.run(
        [
            "docker", "run", "--rm",
            "-v", f"{REPO_ROOT}:/app",
            *_arango_net_args(arangodb_id),
            "-e", f"ARANGO_DB_HOST={ARANGO_DB_HOST}",
            "-e", f"ARANGO_DB_PORT={ARANGO_DB_PORT}",
            "-e", f"ARANGO_DB_PASSWORD={arango_db_password}",
            "-e", f"NCBI_EMAIL={ncbi_email}",
            "-e", f"NCBI_API_KEY={ncbi_api_key}",
            "nlm-ckn-etl-python",
            "python", f"/app/python/src/{script}",
        ],
        check=True,
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


@task(name="build-python-docker-image", log_prints=True)
def build_python_docker_image() -> None:
    """Build the nlm-ckn-etl-python Docker image from Dockerfile.python.

    Pinned to linux/amd64 because scikit-misc has no linux/arm64 binary
    wheels.  Works via Rosetta on Apple Silicon.
    """
    logger = get_run_logger()
    logger.info("Building Python Docker image (platform linux/amd64)")
    subprocess.run(
        [
            "docker", "build",
            "--platform", "linux/amd64",
            "-t", "nlm-ckn-etl-python",
            "-f", "src/main/shell/Dockerfile.python",
            ".",
        ],
        check=True,
        cwd=REPO_ROOT,
        env={**os.environ, "DOCKER_BUILDKIT": "1"},
    )
    logger.info("Python Docker image ready: nlm-ckn-etl-python")


@task(name="clean-empty-external-files", log_prints=True)
def clean_empty_external_files() -> None:
    """Remove corrupt or structurally invalid files from data/external/.

    ``ExternalApiResultsFetcher.py`` uses cache files in ``data/external/``
    to resume interrupted runs.  Two classes of bad files are cleaned here:

    1. **Zero-byte files** — The fetcher crashes with ``JSONDecodeError`` when
       it finds an existing cache file that is empty (e.g. an interrupted write
       left a 0-byte placeholder).  Removing them lets the fetcher recreate
       them cleanly.

    2. **Structurally invalid cache files** — The fetcher writes a sentinel key
       into each cache file so the resume branch can reconstruct its working
       state.  A file that exists but lacks its sentinel raises ``KeyError``
       inside the fetcher container.  Removing it forces a clean re-fetch.

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

    # 2. Remove cache files that are missing their sentinel key.
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
                        f"missing required sentinel key '{key}' "
                        f"(structurally invalid cache — would cause KeyError)"
                    )
            except json.JSONDecodeError:
                pass  # already handled by the zero-byte check above


@task(name="validate-external-files", log_prints=True)
def validate_external_files() -> None:
    """Verify that all required external cache files exist and contain valid JSON.

    Called by the fetch flow after fetching and by the pipeline flow after
    syncing from S3 — ensuring TupleWriters never run against missing or
    corrupt inputs.

    Files checked: ``cellxgene.json``, ``opentargets.json``, ``gene.json``,
    ``uniprot.json``.
    """
    logger = get_run_logger()
    external_dir = REPO_ROOT / "data" / "external"
    required = ["cellxgene.json", "opentargets.json", "gene.json", "uniprot.json"]

    errors = []
    for filename in required:
        path = external_dir / filename
        if not path.exists():
            errors.append(f"  {filename} — file not found")
        elif path.stat().st_size == 0:
            errors.append(f"  {filename} — empty (zero bytes)")
        else:
            try:
                json.loads(path.read_text())
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
