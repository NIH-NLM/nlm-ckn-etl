# Dockerfile — single image for the NLM-CKN ETL
#
# Runs both the scheduled fetch flow (fetcher.py) and the full ETL pipeline
# (pipeline.py).  The Java JAR is not baked in; pipeline.py's ensure_jar()
# task downloads it from S3 on first use, or you can bind-mount a pre-built
# JAR at /app/target/nlm-ckn-etl-1.0.jar.
#
# ── Local usage ──────────────────────────────────────────────────────────────
#
#   Build:
#     docker build -t nlm-ckn-etl .
#
#   Run the full pipeline (starts ArangoDB as a sibling container via the socket):
#
#   Option A — named Docker volume (simplest; data persists in Docker-managed volume):
#     docker run --rm \
#       -v "$(pwd)/data:/app/data" \
#       -v "$(pwd)/target:/app/target" \
#       -v /var/run/docker.sock:/var/run/docker.sock \
#       -e ARANGO_DB_PASSWORD=<password> \
#       nlm-ckn-etl pipeline.py --run-ontology --run-results
#
#   Option B — bind-mount (data visible on the host at $(pwd)/data/arangodb):
#     docker run --rm \
#       -v "$(pwd)/data:/app/data" \
#       -v "$(pwd)/target:/app/target" \
#       -v /var/run/docker.sock:/var/run/docker.sock \
#       -e ARANGO_DB_PASSWORD=<password> \
#       -e ARANGO_DB_HOST_HOME="$(pwd)/data/arangodb" \
#       nlm-ckn-etl pipeline.py --run-ontology --run-results
#
#   Run the fetch flow only (writes to data/external/):
#   NOTE: fetcher.py imports scanpy and loads NSForest h5ad files to build
#   gene ID lists.  The science stack alone needs ~4 GiB; use --memory 8g
#   (or raise Docker Desktop's memory limit) to avoid an OOM kill.
#     docker run --rm \
#       --memory 8g \
#       -v "$(pwd)/data:/app/data" \
#       -e NCBI_EMAIL=<email> -e NCBI_API_KEY=<key> \
#       nlm-ckn-etl python fetcher.py
#
#   Run the scheduled fetch with S3 sync (mirrors ECS Fargate):
#     docker run --rm \
#       --memory 8g \
#       -e S3_BUCKET=<bucket> \
#       -e NCBI_EMAIL=<email> -e NCBI_API_KEY=<key> \
#       --entrypoint fetch-entrypoint \
#       nlm-ckn-etl
#
# ── AWS usage ────────────────────────────────────────────────────────────────
#   The image is pushed to ECR by .github/workflows/build-image.yml.
#   The fetch-entrypoint is used by the ECS Fargate task (cloudformation/fetch.yaml).
#   The ETL pipeline runs as an AWS Batch job (cloudformation/batch.yaml).

# Pin to linux/amd64: scikit-misc has no linux/arm64 binary wheel on PyPI.
# amd64 wheels run correctly under Rosetta on Apple Silicon.
FROM --platform=linux/amd64 python:3.12-slim

# ── System build deps (needed by scikit-misc, scanpy, h5py, etc.) ──────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        gfortran \
        libhdf5-dev \
        liblapack-dev \
        libblas-dev \
    && rm -rf /var/lib/apt/lists/*

# ── uv (fast Python installer) ─────────────────────────────────────────────
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
ENV UV_SYSTEM_PYTHON=1

# ── Prefect runtime settings ───────────────────────────────────────────────
# PREFECT_HOME on tmpfs (/tmp) keeps the ephemeral SQLite API database in
# memory-backed storage, which is faster than disk — important when running
# under Rosetta emulation (amd64 image on Apple Silicon) where disk I/O is
# slow enough to trigger the default 20-second startup timeout.
ENV PREFECT_HOME=/tmp/prefect
# Raise the ephemeral API server startup timeout to 120 seconds.
# The default (20 s) is tight for Rosetta-emulated containers; this is a
# no-op cost on native amd64 hardware where the server starts in < 5 s.
ENV PREFECT_SERVER_EPHEMERAL_STARTUP_TIMEOUT_SECONDS=120

# ── Python dependencies ────────────────────────────────────────────────────
# poetry is used only to export the pinned requirements from poetry.lock;
# uv handles the actual install (parallel downloads, ~10-100x faster than pip).
RUN pip install --no-cache-dir poetry==2.3.2 poetry-plugin-export

WORKDIR /app

COPY python/pyproject.toml python/poetry.lock python/

RUN --mount=type=cache,target=/root/.cache/uv \
    cd python \
    && poetry export --without dev --without-hashes -f requirements.txt -o /tmp/requirements.txt \
    && uv pip install --system -r /tmp/requirements.txt

# ── AWS CLI ────────────────────────────────────────────────────────────────
# Used by S3 sync tasks in fetcher.py and pipeline.py.
RUN pip install --no-cache-dir awscli

# ── Application source ────────────────────────────────────────────────────
COPY python/src /app/python/src

# ── Static config data files ───────────────────────────────────────────────
# Small repo-tracked files read at runtime (NSForest source lists, mappings).
# Large data (obo/, external/, results/, tuples/) must be mounted or synced.
COPY data/*.json data/*.csv /app/data/

# ── Scheduled-fetch entrypoint ─────────────────────────────────────────────
# Used by the ECS Fargate task: syncs S3 → runs fetcher.py → syncs back.
COPY src/main/shell/fetch-entrypoint.sh /usr/local/bin/fetch-entrypoint
RUN chmod +x /usr/local/bin/fetch-entrypoint

# ── Default entrypoint ─────────────────────────────────────────────────────
# Runs pipeline.py; pass stage flags (e.g. --run-results) as docker run args.
# Override with --entrypoint to run fetcher.py or fetch-entrypoint instead.
ENTRYPOINT ["python"]
WORKDIR /app/python/src
CMD [ "pipeline.py"]
