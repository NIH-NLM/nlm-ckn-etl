#!/usr/bin/env python3
"""Validate the nlm-ckn ``data/prod`` tree against the ETL's assumptions.

The ETL imposes naming-convention and content assumptions on the upstream
``data/prod`` files (NSForest results, dataset summaries, silhouette scores,
author-to-CL mappings, CELLxGENE harvester output, S3 manifests).  Many of
those assumptions fail *silently* downstream — an inner merge that drops
rows, a dataset skipped for a malformed CL id, a companion file missed
because its suffix does not match.  This script checks them up front so a
release can be rejected before the expensive fetch + ETL stages run.

It validates the NESTED prod tree (the source of truth), simulating the
flatten that ``release.extract_release_tarball`` performs so post-flatten
collisions are caught without packaging a tarball.

The canonical patterns and column lists come from :mod:`ProductionDataSpecification`,
shared with ``LoaderUtilities`` so the validator cannot drift from the
readers.

Scope (P0): structural checks (discovery, companion pairing, post-flatten
uniqueness, manifest membership) and the high-value silent-failure content
checks (required columns, cluster_header⇔silhouette pairing, silhouette
join-column name, dataset_version_id presence, CL-CURIE conformance).

Usage
-----
::

    python src/ProductionDataValidator.py --data-dir /path/to/nlm-ckn/data/prod
    python src/ProductionDataValidator.py --data-dir DIR --json
    python src/ProductionDataValidator.py --data-dir DIR --strict   # warnings fail too

Exit status is non-zero when any ERROR (or, with ``--strict``, any WARN)
is found.
"""

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

import ProductionDataSpecification as spec

ERROR = "ERROR"
WARN = "WARN"


@dataclass
class Finding:
    """A single validation result.

    Parameters
    ----------
    severity : str
        :data:`ERROR` or :data:`WARN`.
    check : str
        Short stable identifier for the check (e.g. ``"companion-missing"``).
    message : str
        Human-readable description of what failed.
    path : str
        Relative path (to the data dir) of the offending file, or ``""``.
    """

    severity: str
    check: str
    message: str
    path: str = ""


@dataclass
class Report:
    """Accumulated findings for a validation run."""

    data_dir: Path
    findings: list = field(default_factory=list)

    def add(self, severity, check, message, path=""):
        """Record a finding."""
        self.findings.append(Finding(severity, check, message, str(path)))

    def errors(self):
        """Return all ERROR-severity findings."""
        return [f for f in self.findings if f.severity == ERROR]

    def warnings(self):
        """Return all WARN-severity findings."""
        return [f for f in self.findings if f.severity == WARN]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_header(path):
    """Return the column list of a CSV without loading its rows.

    Returns ``None`` if the file cannot be parsed.
    """
    try:
        return list(pd.read_csv(path, nrows=0).columns)
    except Exception as exc:  # malformed/unreadable CSV
        return exc


def _rel(report, path):
    """Render *path* relative to the data dir for compact messages."""
    try:
        return str(Path(path).relative_to(report.data_dir))
    except ValueError:
        return str(path)


# ---------------------------------------------------------------------------
# Structural checks
# ---------------------------------------------------------------------------


def check_structure(report):
    """Discovery, companion pairing, post-flatten uniqueness.

    Returns the list of discovered ``results_ensg_*.csv`` paths so content
    checks can reuse it.
    """
    data_dir = report.data_dir
    nsforest_paths = sorted(data_dir.glob(spec.NSFOREST_GLOB))

    if not nsforest_paths:
        report.add(
            ERROR,
            "no-results",
            f"No {spec.NSFOREST_PREFIX}_*.csv files found under {data_dir} — "
            "an empty graph would be built downstream.",
        )
        return nsforest_paths

    # Companion pairing: summary required, silhouette/mapping optional.  A
    # companion is located by prefix substitution, mirroring the reader.
    for p in nsforest_paths:
        for label, prefix in spec.COMPANION_PREFIXES.items():
            want = spec.companion_basename(p.name, prefix)
            matches = list(data_dir.glob(f"**/{want}"))
            if label == "summary" and not matches:
                report.add(
                    ERROR,
                    "companion-missing",
                    f"Required summary companion {want!r} not found for "
                    f"{p.name} (dataset version id lookup would fail).",
                    _rel(report, p),
                )
            if len(matches) > 1:
                report.add(
                    WARN,
                    "companion-ambiguous",
                    f"{len(matches)} files match {label} companion {want!r}; "
                    "the reader uses the first.",
                    _rel(report, p),
                )

    # Post-flatten basename uniqueness among CONSUMED file kinds (others
    # collide harmlessly).  master_s3_manifest.csv is unioned, so excluded.
    seen = {}
    for p in data_dir.rglob("*"):
        if not p.is_file():
            continue
        name = p.name
        if name in spec.FLATTEN_COLLISION_ALLOWED:
            continue
        if not any(name.startswith(pre) for pre in spec.CONSUMED_PREFIXES) and not (
            name.endswith("_harvester_final.csv")
        ):
            continue
        seen.setdefault(name, []).append(p)
    for name, paths in seen.items():
        if len(paths) > 1:
            locs = ", ".join(_rel(report, p) for p in paths)
            report.add(
                ERROR,
                "flatten-collision",
                f"Basename {name!r} occurs {len(paths)}× ({locs}); flattening "
                "into one dir would silently drop all but the last.",
            )

    return nsforest_paths


def check_manifest(report):
    """Manifest header and results membership cross-check."""
    data_dir = report.data_dir
    manifest_paths = list(data_dir.rglob(spec.MANIFEST_NAME))
    if not manifest_paths:
        report.add(
            WARN,
            "manifest-absent",
            f"No {spec.MANIFEST_NAME} found; the extraction-time integrity "
            "cross-check would be skipped.",
        )
        return

    listed = set()
    for mp in manifest_paths:
        cols = _read_header(mp)
        if isinstance(cols, Exception):
            report.add(ERROR, "manifest-unreadable", f"{cols}", _rel(report, mp))
            continue
        missing = [c for c in spec.MANIFEST_REQUIRED_COLUMNS if c not in cols]
        if missing:
            report.add(
                ERROR,
                "manifest-columns",
                f"{spec.MANIFEST_NAME} missing column(s) {missing}.",
                _rel(report, mp),
            )
            continue
        df = pd.read_csv(mp)
        for fn in df["filename"].dropna().astype(str):
            if fn.startswith(f"{spec.NSFOREST_PREFIX}_") and fn.endswith(".csv"):
                listed.add(fn)

    found = {p.name for p in data_dir.glob(spec.NSFOREST_GLOB)}
    for fn in sorted(listed - found):
        report.add(
            ERROR,
            "manifest-missing-file",
            f"{fn} is listed in a manifest but not present in the tree.",
        )
    for fn in sorted(found - listed):
        report.add(
            WARN,
            "manifest-unlisted-file",
            f"{fn} is present but not listed in any manifest.",
        )


# ---------------------------------------------------------------------------
# Content / silent-failure checks
# ---------------------------------------------------------------------------


def _check_required_columns(report, path, required, check, kind):
    """Emit an ERROR for any missing required column; return the header.

    Returns ``None`` if the file is unreadable.
    """
    cols = _read_header(path)
    if isinstance(cols, Exception):
        report.add(ERROR, "unreadable", f"{kind} unreadable: {cols}", _rel(report, path))
        return None
    missing = [c for c in required if c not in cols]
    if missing:
        report.add(
            ERROR,
            check,
            f"{kind} missing required column(s) {missing}.",
            _rel(report, path),
        )
    return cols


def check_content(report, nsforest_paths):
    """Required columns and cross-file silent-failure checks per dataset."""
    data_dir = report.data_dir

    for p in nsforest_paths:
        ns_cols = _check_required_columns(
            report, p, spec.NSFOREST_REQUIRED_COLUMNS, "nsforest-columns", "results_ensg"
        )

        # Summary: required column + at least one non-null dataset_version_id.
        summary_name = spec.companion_basename(p.name, spec.SUMMARY_PREFIX)
        for sp in data_dir.glob(f"**/{summary_name}"):
            s_cols = _check_required_columns(
                report,
                sp,
                spec.SUMMARY_REQUIRED_COLUMNS,
                "summary-columns",
                "master_dataset_summary",
            )
            if s_cols and "dataset_version_id" in s_cols:
                sdf = pd.read_csv(sp)
                if sdf["dataset_version_id"].dropna().empty:
                    report.add(
                        ERROR,
                        "summary-no-dvid",
                        "dataset_version_id column has no non-null value "
                        "(get_dataset_version_id_lists would raise).",
                        _rel(report, sp),
                    )

        # Silhouette: stat columns + cross-file cluster_header dependency.
        sil_name = spec.companion_basename(p.name, spec.SILHOUETTE_PREFIX)
        sil_matches = list(data_dir.glob(f"**/{sil_name}"))
        if sil_matches:
            sil = sil_matches[0]
            sil_cols = _check_required_columns(
                report,
                sil,
                spec.SILHOUETTE_REQUIRED_COLUMNS,
                "silhouette-columns",
                "silhouette_fscore_summary",
            )
            # cluster_header⇔silhouette: when a silhouette file exists, the
            # results file MUST carry cluster_header (read at row 0).
            if ns_cols is not None and "cluster_header" not in ns_cols:
                report.add(
                    ERROR,
                    "cluster-header-missing",
                    f"{p.name} has a silhouette companion but no "
                    "'cluster_header' column (NSForest merge would crash).",
                    _rel(report, p),
                )
            elif ns_cols is not None and sil_cols is not None:
                # The silhouette join column NAME equals the cluster_header value.
                try:
                    ch_val = str(pd.read_csv(p, usecols=["cluster_header"]).iloc[0, 0])
                except Exception as exc:
                    ch_val = None
                    report.add(
                        WARN,
                        "cluster-header-unreadable",
                        f"Could not read cluster_header value: {exc}",
                        _rel(report, p),
                    )
                if ch_val is not None and ch_val not in sil_cols:
                    report.add(
                        ERROR,
                        "silhouette-join-column",
                        f"silhouette file lacks join column {ch_val!r} "
                        "(the results cluster_header value); merge yields no rows.",
                        _rel(report, sil),
                    )

        # Mapping: required columns + CL-CURIE conformance (rows the reader
        # would warn-and-skip).
        map_name = spec.companion_basename(p.name, spec.MAPPING_PREFIX)
        for mp in data_dir.glob(f"**/{map_name}"):
            m_cols = _check_required_columns(
                report,
                mp,
                spec.MAPPING_REQUIRED_COLUMNS,
                "mapping-columns",
                "cluster_cid_mapping",
            )
            if m_cols and {"manual_mapped_cid", "cell_ontology_id"} <= set(m_cols):
                mdf = pd.read_csv(mp)
                bad = 0
                for _, row in mdf.iterrows():
                    raw = row.get("manual_mapped_cid")
                    if pd.isna(raw) or str(raw).strip() == "":
                        raw = row.get("cell_ontology_id")
                    if pd.isna(raw) or str(raw).strip() == "":
                        bad += 1
                        continue
                    if not spec.CL_CURIE_RE.match(spec.obo_purl_to_curie(str(raw))):
                        bad += 1
                if bad:
                    report.add(
                        WARN,
                        "mapping-cl-curie",
                        f"{bad}/{len(mdf)} mapping rows have no valid CL CURIE "
                        "(manual_mapped_cid/cell_ontology_id); rows are skipped.",
                        _rel(report, mp),
                    )

    # Harvester: required join column on every harvester file.
    for hp in data_dir.rglob("*_harvester_final.csv"):
        _check_required_columns(
            report,
            hp,
            spec.HARVESTER_REQUIRED_COLUMNS,
            "harvester-columns",
            "harvester_final",
        )


# ---------------------------------------------------------------------------
# Driver / reporting
# ---------------------------------------------------------------------------


def validate(data_dir):
    """Run all P0 checks against *data_dir* and return a :class:`Report`."""
    report = Report(data_dir=Path(data_dir))
    nsforest_paths = check_structure(report)
    check_manifest(report)
    check_content(report, nsforest_paths)
    return report


def _print_report(report):
    """Print a human-readable summary grouped by severity."""
    errs, warns = report.errors(), report.warnings()
    for f in errs + warns:
        loc = f" [{f.path}]" if f.path else ""
        print(f"{f.severity:5} {f.check}: {f.message}{loc}")
    print(f"\n{len(errs)} error(s), {len(warns)} warning(s) in {report.data_dir}")


def main(argv=None):
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Validate the nlm-ckn data/prod tree against ETL assumptions."
    )
    parser.add_argument(
        "--data-dir",
        required=True,
        help="Path to the nested prod data tree (e.g. .../nlm-ckn/data/prod).",
    )
    parser.add_argument(
        "--json", action="store_true", help="Emit findings as JSON to stdout."
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero on warnings as well as errors.",
    )
    args = parser.parse_args(argv)

    data_dir = Path(args.data_dir)
    if not data_dir.is_dir():
        parser.error(f"--data-dir not a directory: {data_dir}")

    report = validate(data_dir)

    if args.json:
        print(
            json.dumps(
                {
                    "data_dir": str(report.data_dir),
                    "findings": [vars(f) for f in report.findings],
                    "errors": len(report.errors()),
                    "warnings": len(report.warnings()),
                },
                indent=2,
            )
        )
    else:
        _print_report(report)

    failed = bool(report.errors()) or (args.strict and bool(report.warnings()))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
