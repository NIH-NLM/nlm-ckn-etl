#!/usr/bin/env python3
"""Prefect fetch flow for the NLM-CKN ETL pipeline.

Downloads raw data from external APIs (CELLxGENE, Open Targets, NCBI Gene,
UniProt, HuBMAP) via ``ExternalApiResultsFetcher.py`` and writes the results
to local storage and/or S3.

Designed to run independently on a schedule (hourly via EventBridge + ECS
Fargate) without requiring ArangoDB or the full ETL pipeline.

Usage
-----
Run directly (no Prefect server needed)::

    cd python
    python src/fetcher.py
    python src/fetcher.py --ncbi-email user@example.com --ncbi-api-key KEY

Or with the Prefect CLI after ``prefect server start``::

    prefect deployment run 'nlm-ckn-fetch/local'

See the README for full local-run and AWS deployment instructions.

Local vs S3 mode
----------------
When ``S3_BUCKET`` is unset (the default), all external cache files are
written to ``data/external/`` on the local filesystem only.  Set
``S3_BUCKET`` to push the cache to S3 after each successful fetch, making
it available to ``pipeline.py`` running on a different host.
"""

import argparse
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact

from _common import (
    REPO_ROOT,
    S3_BUCKET,
    build_python_docker_image,
    clean_empty_external_files,
    sync_external_from_s3,
    sync_external_to_s3,
    validate_external_files,
    _run_python_container,
)

# ── Tasks ──────────────────────────────────────────────────────────────────


@task(name="fetch-external-api-results", log_prints=True)
def fetch_external_api_results(
    arango_db_password: str = "",
    ncbi_email: str = "",
    ncbi_api_key: str = "",
) -> None:
    """Run ``ExternalApiResultsFetcher.py`` inside the nlm-ckn-etl-python image.

    ArangoDB is not required for the fetch — ``arangodb_id`` is always
    ``None`` here, so no ``--network container:…`` flag is added.  The
    ``ARANGO_DB_PASSWORD`` env var is injected but ignored by the fetcher.
    """
    logger = get_run_logger()
    logger.info("Fetching external API results (ExternalApiResultsFetcher)")
    _run_python_container(
        "ExternalApiResultsFetcher.py",
        arangodb_id=None,  # fetcher never needs a local ArangoDB container
        arango_db_password=arango_db_password,
        ncbi_email=ncbi_email,
        ncbi_api_key=ncbi_api_key,
    )
    logger.info("External API results fetched")


@task(name="record-fetch-artifact", log_prints=True)
def record_fetch_artifact() -> None:
    """Write ``fetch-info.json`` and a Prefect UI artifact summarising the run.

    ``fetch-info.json`` is stored alongside the cache files in
    ``data/external/`` so it travels to S3 with the ``sync_external_to_s3``
    task.  ``pipeline.py`` reads it during the archive stage and merges its
    contents into ``build-info.txt``.

    Fields written:

    - ``fetched_at``  — ISO-8601 UTC timestamp
    - ``commit``      — short git commit hash of the repo at fetch time
    - ``files``       — mapping of cache filename → byte size (``null`` if missing)
    """
    logger = get_run_logger()
    external_dir = REPO_ROOT / "data" / "external"

    # Collect file sizes for the required cache files
    required = ["cellxgene.json", "opentargets.json", "gene.json", "uniprot.json"]
    files_info: dict[str, int | None] = {}
    for name in required:
        path = external_dir / name
        files_info[name] = path.stat().st_size if path.exists() else None

    # Current git commit hash (best-effort; falls back to "unknown")
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=REPO_ROOT,
            text=True,
        ).strip()
    except Exception:
        commit = "unknown"

    info = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "commit": commit,
        "files": files_info,
    }

    info_path = external_dir / "fetch-info.json"
    info_path.write_text(json.dumps(info, indent=2))
    logger.info(f"Fetch artifact written to {info_path.relative_to(REPO_ROOT)}")

    # Prefect UI artifact — summary table visible in the flow-run page
    rows = "\n".join(
        f"| `{name}` | {size:,} bytes |"
        if size is not None
        else f"| `{name}` | ⚠ missing |"
        for name, size in files_info.items()
    )
    s3_note = (
        f"**S3 destination:** `s3://{S3_BUCKET}/external/`"
        if S3_BUCKET
        else "_S3_BUCKET not set — files stored locally only._"
    )
    create_markdown_artifact(
        key="fetch-summary",
        markdown=f"""## External API Fetch Summary

**Fetched at:** {info["fetched_at"]}
**Commit:** `{commit}`
{s3_note}

| File | Size |
|------|------|
{rows}
""",
        description="External API fetch results",
    )


# ── Flow ───────────────────────────────────────────────────────────────────


@flow(name="nlm-ckn-fetch", log_prints=True)
def nlm_ckn_fetch(
    ncbi_email: str = "",
    ncbi_api_key: str = "",
) -> None:
    """NLM-CKN external API fetch flow.

    Downloads raw data from CELLxGENE, Open Targets, NCBI Gene, UniProt,
    and HuBMAP into ``data/external/`` (local) and
    ``s3://${S3_BUCKET}/external/`` (when ``S3_BUCKET`` is set).

    Designed to run independently on a schedule without ArangoDB.

    Parameters
    ----------
    ncbi_email:
        NCBI E-Utilities email address.  Falls back to the ``NCBI_EMAIL``
        environment variable.
    ncbi_api_key:
        NCBI E-Utilities API key.  Falls back to the ``NCBI_API_KEY``
        environment variable.
    """
    logger = get_run_logger()

    # Resolve credentials: explicit parameters take priority, then env vars
    ncbi_email = ncbi_email or os.getenv("NCBI_EMAIL", "")
    ncbi_api_key = ncbi_api_key or os.getenv("NCBI_API_KEY", "")
    # ArangoDB password is not used by the fetcher but is forwarded to the
    # container env for forward-compatibility; ignore if unset.
    arango_db_password = os.getenv("ARANGO_DB_PASSWORD", "")

    if S3_BUCKET:
        logger.info(f"S3 mode: bucket={S3_BUCKET}")
    else:
        logger.info("Local mode: S3_BUCKET not set, writing to data/external/ only")

    build_python_docker_image()
    sync_external_from_s3()        # restore cache (no-op if no S3)
    clean_empty_external_files()
    fetch_external_api_results(
        arango_db_password=arango_db_password,
        ncbi_email=ncbi_email,
        ncbi_api_key=ncbi_api_key,
    )
    validate_external_files()
    record_fetch_artifact()
    sync_external_to_s3()          # persist to S3 (no-op if no S3)


# ── CLI entry point ────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="NLM-CKN external API fetch (Prefect)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--ncbi-email",
        default=os.getenv("NCBI_EMAIL", ""),
        help="NCBI E-Utilities email (default: $NCBI_EMAIL)",
    )
    parser.add_argument(
        "--ncbi-api-key",
        default=os.getenv("NCBI_API_KEY", ""),
        help="NCBI E-Utilities API key (default: $NCBI_API_KEY)",
    )
    args = parser.parse_args()

if args.ncbi_email is not None and args.ncbi_api_key is not None:
    nlm_ckn_fetch(
        ncbi_email=args.ncbi_email,
        ncbi_api_key=args.ncbi_api_key,
    )
else:
    parser.error("Both NCBI email and API key are required. Use --ncbi-email and --ncbi-api-key to provide them.")
