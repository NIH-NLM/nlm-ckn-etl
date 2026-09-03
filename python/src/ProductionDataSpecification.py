"""Canonical spec for nlm-ckn prod data files.

Single source of truth for the file-naming conventions and column
expectations that the ETL imposes on the upstream ``data/prod`` tree
(the ``sc-nsforest-qc-nf`` results plus the CELLxGENE harvester output).

Both the production readers (``LoaderUtilities``) and the standalone
``ProductionDataValidator`` validator import from here so the two cannot
drift.  This module is intentionally dependency-light (standard library
only) so it can be imported without pulling in scanpy/boto3/rdflib.

The assumptions encoded here were compiled from the readers:
``LoaderUtilities.get_dataset_file_paths`` /
``get_dataset_version_id_lists`` / ``get_cellxgene_harvester_data``,
``NSForestTupleWriter``, ``MappingTupleWriter``, and
``TupleWriterUtilities.build_cell_set_dataset``.
"""

from pathlib import Path
import re

# ---------------------------------------------------------------------------
# Filename prefixes / patterns
# ---------------------------------------------------------------------------

# The NSForest results file is the anchor; every companion file shares the
# same basename with this prefix substituted (see ``companion_basename``).
NSFOREST_PREFIX = "results_ensg"
SUMMARY_PREFIX = "master_dataset_summary"
SILHOUETTE_PREFIX = "silhouette_fscore_summary"
MAPPING_PREFIX = "cluster_cid_mapping"
# Per-cluster binary scores, gene × cluster.  The ``_ensg`` variant is the
# one the ETL reads: its gene index is spelled in the Ensembl ids that the
# results file's ``binary_genes`` lists, so the two join without mapping.
BINARY_SCORES_PREFIX = "binary_scores_ensg"

# Companion kinds keyed by a short label, mapped to their filename prefix.
COMPANION_PREFIXES = {
    "summary": SUMMARY_PREFIX,
    "silhouette": SILHOUETTE_PREFIX,
    "mapping": MAPPING_PREFIX,
    "binary_scores": BINARY_SCORES_PREFIX,
}

# Globs used by the readers (recursive ``**/`` form, matching
# ``get_dataset_file_paths`` which searches nested release dirs).
NSFOREST_GLOB = f"**/{NSFOREST_PREFIX}_*.csv"
HARVESTER_GLOB = "*_harvester_final.csv"
HARVESTER_GLOB_RECURSIVE = f"**/{HARVESTER_GLOB}"
# A harvester filename is ``<species>_<organ>_harvester_final.csv``, and the
# organ is recoverable only from it: the table itself has no organ column
# (see organ_of_harvester_path).
HARVESTER_SPECIES_PREFIX = "homo_sapiens_"
UBERON_PREFIX = "uberon"
UBERON_GLOB = f"{UBERON_PREFIX}_*.csv"
UBERON_GLOB_RECURSIVE = f"**/{UBERON_GLOB}"
MANIFEST_NAME = "master_s3_manifest.csv"

# Clusters smaller than this are dropped by every results-consuming writer.
MIN_CLUSTER_SIZE = 10

# ---------------------------------------------------------------------------
# Required / used columns, per file kind
# ---------------------------------------------------------------------------

# results_ensg_*.csv — columns the NSForest/Mapping writers access by name.
# ``uuid`` is intentionally NOT required: load_results adds it if absent.
# ``cluster_header`` is conditionally required (see CLUSTER_HEADER_*).
NSFOREST_REQUIRED_COLUMNS = [
    "clusterName",
    "clusterSize",
    "f_score",
    "NSForest_markers",
    "binary_genes",
]
# Columns whose values must be stringified Python lists of gene tokens.
# collect_unique_gene_names ast.literal_evals these without a guard, so a
# malformed value raises during gene collection.
GENE_LIST_COLUMNS = ["NSForest_markers", "binary_genes"]
# Used opportunistically via as_float/as_int/.get — absence is tolerated.
NSFOREST_OPTIONAL_COLUMNS = [
    "precision",
    "recall",
    "TP",
    "FP",
    "FN",
    "onTarget",
    "marker_count",
    "software_version",
    "cluster_header",
]

# master_dataset_summary_*.csv — only dataset_version_id is load-bearing
# (an empty/all-null column raises in get_dataset_version_id_lists).  The
# rest feed build_cell_set_dataset via .get() and are optional.
SUMMARY_REQUIRED_COLUMNS = ["dataset_version_id"]
SUMMARY_OPTIONAL_COLUMNS = [
    "dataset_title",
    "collection_name",
    "organ",
    "collection_url",
    "explorer_url",
    "n_cells",
    "embedding",
    "cluster_header",
    "mean_silhouette",
    "std_silhouette",
    "mean_fscore",
    "median_fscore",
    "first_author",
    "year",
    "journal",
    "doi",
    "tissue_ontology_term_id",
    # Dataset-scoped rollups the harvester also reports, under these same
    # names.  The summary is the primary source for them because it covers
    # every dataset, while the per-organ harvester tables do not
    # (Springbok-LLC/nlm-ckn-etl#63).
    "tissue_ontology_summary",
    "assay_ontology_summary",
    # Reported by the summary alone.  ``filtered_cell_count`` has no
    # harvester equivalent -- the harvester's ``normal_cell_count`` is a
    # different quantity, not this one computed elsewhere
    # (Springbok-LLC/nlm-ckn-etl#64) -- and the harvester reports no
    # clustering statistics at all.
    "filtered_cell_count",
    "median_silhouette",
    "n_clusters",
    # Donor age rollup, reported under this name by the harvester too.  The
    # summary pads it with every stage of the vocabulary at a zero count, so
    # readers drop the zero-count pairs (as_nonzero_rollup_str).
    "development_stage_summary",
]

# silhouette_fscore_summary_*.csv — the five stat columns merged into the
# NSForest frame, PLUS a join column whose NAME equals the value of the
# results file's cluster_header (validated cross-file, not listed here).
SILHOUETTE_REQUIRED_COLUMNS = ["median", "mean", "std", "q1", "q3"]

# binary_scores_ensg_*.csv — a gene × cluster matrix of binary scores, read
# with the first (unnamed) column as the gene index.  It declares no fixed
# column names: the columns are the results file's cluster names, and the
# index its ``binary_genes`` Ensembl ids.  Sparse in prod (a few results sets
# ship none), so NSForestTupleWriter leaves mean_binary_score empty rather
# than failing when it is absent.
BINARY_SCORES_GENE_INDEX_COLUMN = 0

# cluster_cid_mapping_*.csv — author-to-CL mapping consumed by MappingTupleWriter.
MAPPING_REQUIRED_COLUMNS = [
    "cluster_name",
    "skos",
    "manual_mapped_cid",
    "cell_ontology_id",
]
MAPPING_OPTIONAL_COLUMNS = ["dataset_version_id"]

# *_harvester_final.csv — joined to summaries on dataset_version_id.
HARVESTER_REQUIRED_COLUMNS = ["dataset_version_id"]

# uberon_<organ>.csv — the harvester's root/descendant table for an organ,
# keyed to a summary by its ``organ`` column.  Rows with level == root name
# the anatomical structure every cell set of that organ is rolled up to.
UBERON_REQUIRED_COLUMNS = ["obo_id", "label", "level"]
UBERON_ROOT_LEVEL = "root"

# Organs the harvester ships no uberon_<organ>.csv for, with the root term
# supplied here instead (NIH-NLM/nlm-ckn#171 names brain as one of the roots).
# Drop an entry once the harvester ships the corresponding file.
ORGAN_ROOT_OVERRIDES = {"neocortex": "UBERON:0000955"}

# master_s3_manifest.csv — unioned at extraction; integrity cross-check.
MANIFEST_REQUIRED_COLUMNS = ["filename", "s3_path"]

# Basenames that legitimately repeat across datasets and so must be EXCLUDED
# from the post-flatten uniqueness check (extract_release_tarball unions them).
FLATTEN_COLLISION_ALLOWED = {MANIFEST_NAME}

# File-kind prefixes whose post-flatten basename collisions actually corrupt
# the pipeline (consumed files).  Other repeated basenames in the prod tree
# (e.g. cluster_stats.log) are flattened last-writer-wins but unconsumed.
CONSUMED_PREFIXES = [
    NSFOREST_PREFIX,
    SUMMARY_PREFIX,
    SILHOUETTE_PREFIX,
    MAPPING_PREFIX,
    BINARY_SCORES_PREFIX,
]

# ---------------------------------------------------------------------------
# Value-format patterns
# ---------------------------------------------------------------------------

# A valid Cell Ontology CURIE after purl_to_curie normalization.
CL_CURIE_RE = re.compile(r"CL:\d{7}$")

# A general ontology-term CURIE (e.g. UBERON:0002048), used to sanity-check
# tissue_ontology_term_id tokens (split on "|").
ONTOLOGY_CURIE_RE = re.compile(r"[A-Za-z]+:\d+$")

# OBO PURL → CURIE, mirroring TupleWriterUtilities.purl_to_curie.
_OBO_PURL_RE = re.compile(r"https?://purl\.obolibrary\.org/obo/(\w+?)_(\d+)$")


def obo_purl_to_curie(purl: str) -> str:
    """Convert an OBO PURL to a CURIE, else return the input unchanged.

    Mirrors ``TupleWriterUtilities.purl_to_curie`` without importing it
    (that module pulls in the ckn_schema dependency).

    Parameters
    ----------
    purl : str
        An OBO PURL or CURIE string.

    Returns
    -------
    str
        A CURIE (e.g. ``"CL:0000235"``), or the input unchanged if it does
        not match the OBO PURL pattern.
    """
    m = _OBO_PURL_RE.match(purl)
    if m:
        return f"{m.group(1)}:{m.group(2)}"
    return purl


def normalize_organ(organ: str) -> str:
    """Normalize an organ name to the form used in ``uberon_<organ>.csv``.

    Parameters
    ----------
    organ : str
        An organ name, as it appears in a summary's ``organ`` column, in a
        ``uberon_<organ>.csv`` filename, or as an UBERON root term's label.

    Returns
    -------
    str
        The organ name lowercased with whitespace and hyphens replaced by
        underscores (e.g. ``"heart plus pericardium"`` →
        ``"heart_plus_pericardium"``).
    """
    return re.sub(r"[\s-]+", "_", str(organ).strip().lower())


def organ_of_uberon_path(path) -> str:
    """Return the organ named by a ``uberon_<organ>.csv`` filename."""
    return normalize_organ(Path(path).stem[len(UBERON_PREFIX) + 1 :])


def organ_of_harvester_path(path) -> str:
    """Return the organ named by a harvester filename.

    A harvester table holds one organ's rows, but carries no organ column,
    so the organ has to come from the ``<species>_<organ>_harvester_final``
    filename.  Without it a dataset harvested for several organs cannot be
    joined to the right row (Springbok-LLC/nlm-ckn-etl#63).

    Parameters
    ----------
    path : Path or str
        Path to a ``*_harvester_final.csv``.

    Returns
    -------
    str
        The normalized organ name (e.g. ``"skin_of_body"``).
    """
    stem = Path(path).stem
    suffix = HARVESTER_GLOB[1:].removesuffix(".csv")
    if stem.endswith(suffix):
        stem = stem[: -len(suffix)]
    if stem.startswith(HARVESTER_SPECIES_PREFIX):
        stem = stem[len(HARVESTER_SPECIES_PREFIX) :]
    return normalize_organ(stem)


def companion_basename(nsforest_basename: str, companion_prefix: str) -> str:
    """Return the expected companion filename for an NSForest results file.

    Mirrors the prefix-substitution that ``get_dataset_file_paths`` uses to
    locate companion files (summary / silhouette / mapping).

    Parameters
    ----------
    nsforest_basename : str
        Basename of a ``results_ensg_*.csv`` file.
    companion_prefix : str
        One of the prefixes in :data:`COMPANION_PREFIXES`.

    Returns
    -------
    str
        The companion basename with the NSForest prefix substituted.
    """
    return nsforest_basename.replace(NSFOREST_PREFIX, companion_prefix)
