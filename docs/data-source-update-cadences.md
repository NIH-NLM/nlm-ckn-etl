# Data Source Update Cadences

*Reference summary for client project — Open Targets, NCBI Gene, NCBI Datasets / RefSeq, and UniProt*

*Compiled June 2026*

## Purpose

This document summarizes how frequently each upstream data source changes significantly, so the project can set an appropriate re-sync schedule and pin to reproducible versions where needed.

## Summary table

| Source | Update model | Cadence | Versioned / pinnable? |
|---|---|---|---|
| Open Targets Platform | Discrete batch release | ~Quarterly (every 3 months) | Yes — `YY.MM` release (e.g. 26.03) |
| UniProt | Discrete batch release | Every 8 weeks (~6–7/year) | Yes — `YYYY_XX` release (e.g. 2026_01) |
| NCBI Gene | Continuous / rolling | Daily–weekly increments | No discrete version |
| NCBI RefSeq (comprehensive release) | Discrete batch release | Bi-monthly (odd months) + daily updates | Yes — release number (200s) |
| NCBI RefSeq genome annotation | Per-organism release | A few times/year per organism | Yes — assembly + annotation release |

## Source details

### Open Targets Platform

Open Targets releases on a roughly quarterly cycle, about every three months. Releases are versioned by year and month (e.g. 25.03, 25.06, 25.09, 25.12, 26.03), and each is a discrete batch in which evidence, target–disease associations, and integrated datasets can shift substantially.

The cadence is not perfectly clockwork — quarters are occasionally skipped (there was no late-2024 release; it jumped from 24.09 to 25.03). Treat each numbered release as the unit of "significant change" and pin to a specific version.

### UniProt

UniProt runs on a fixed 8-week release cycle, producing roughly 6–7 releases per year. Releases are versioned as `YYYY_XX` (e.g. 2026_01, released late January 2026), and Swiss-Prot and TrEMBL share the same numbering. Each release is a clean, citable snapshot and is the natural checkpoint for re-syncing.

### NCBI Gene

NCBI Gene is a continuously updated database rather than a discrete versioned release. Update frequency varies by data element:

- Human and mouse records from the HUGO / Mouse Gene Nomenclature Committees are received **daily**.
- Most GeneRIFs are integrated **weekly**.
- About **two days** are required for an update to propagate to all reports; FTP files can lag roughly a day further.

Because there is no single periodic release to pin to, reproducibility requires capturing the per-record modification date or snapshotting the relevant FTP files at retrieval time.

### NCBI Datasets — gene data package

The NCBI Datasets gene data package is an on-demand packaging layer over the live Gene database, not a separately versioned product — a requested gene package reflects whatever is current in Gene at that moment. It therefore inherits Gene's rolling update behavior and offers no distinct "gene package" version to pin to.

For a discrete checkpoint, anchor to the underlying RefSeq layers below instead.

### NCBI RefSeq (comprehensive release)

RefSeq is the versioned source from which the gene packages draw. It follows a formal **bi-monthly** release cycle (odd-numbered months) with incremental daily updates between releases. Releases are numbered (currently in the 200s, after jumping from 99 to 200 in May 2020 to avoid colliding with annotation-release numbering). Each release is a frozen, comprehensive, citable snapshot keyed to a data cutoff date.

### NCBI RefSeq — genome annotation release

The strongest anchor for gene-level reproducibility is the RefSeq genome annotation release, which is what NCBI Datasets targets when pulling data by assembly. These are versioned per organism and tied to a specific assembly accession plus annotation run (e.g. human annotation on `GCF_000001405.x`). Major re-annotations for a given organism typically land a few times per year rather than every cycle.

## Recommendation

For reproducibility across the pipeline, align re-syncs to the slowest meaningful cadence — the **Open Targets quarterly release** — while recording exact version identifiers for each source at every sync:

- **Open Targets** — record the `YY.MM` release number.
- **UniProt** — record the `YYYY_XX` release number.
- **NCBI** — do **not** rely on the gene package's freshness. Pin to the **RefSeq release number** (bi-monthly), or ideally the **genome annotation release / assembly accession** for the organism(s) of interest, and capture per-record modification dates for any live Gene lookups.

This yields a stable, citable checkpoint for every source and makes the full dataset reconstructable at any later date.

## Sources

**Open Targets Platform**
- Release frequency (every 3 months): [Registry of Open Data on AWS — Open Targets](https://registry.opendata.aws/opentargets/)
- Release history and version notes: [Open Targets Platform release notes](https://platform-docs.opentargets.org/release-notes)

**UniProt**
- 8-week release cycle: [UniProt help — release cycle / synchronization](https://www.uniprot.org/help/synchronization)
- Release notes and numbering: [UniProt release notes](https://www.uniprot.org/release-notes)

**NCBI Gene**
- Update frequency by data element (daily/weekly, ~2-day propagation): [Gene Help — NCBI Bookshelf (NBK3841)](https://www.ncbi.nlm.nih.gov/books/NBK3841/)
- Daily nomenclature updates (HGNC/MGNC): [RefSeq FAQ — NCBI Bookshelf (NBK50679)](https://www.ncbi.nlm.nih.gov/books/NBK50679/)

**NCBI Datasets — gene data package**
- Package contents and coverage of all Gene records: [NCBI Datasets Gene Data Package documentation](https://www.ncbi.nlm.nih.gov/datasets/docs/v2/reference-docs/data-packages/gene-package/)

**NCBI RefSeq — comprehensive release**
- Bi-monthly release cycle (odd months) with daily updates: [NCBI Reference Sequences, *Nucleic Acids Research*](https://academic.oup.com/nar/article/37/suppl_1/D32/1007290)
- Release numbering change (99 → 200): [NCBI Insights — RefSeq release number change](https://ncbiinsights.ncbi.nlm.nih.gov/2020/04/07/refseq-rel-num-change/)
- Example release with data cutoff: [NCBI Insights — RefSeq Release 215](https://ncbiinsights.ncbi.nlm.nih.gov/2022/11/15/refseq-release-215/)

**NCBI RefSeq — genome annotation release**
- Per-organism annotation releases (assembly + annotation versioning): [NCBI Insights — human genome annotation release](https://ncbiinsights.ncbi.nlm.nih.gov/2019/07/03/new-human-genome-annotation-release-with-mane-select-and-other-improvements/)

*Cadences reflect each provider's stated schedule as of June 2026 and may vary in practice (e.g. Open Targets occasionally skips a quarter; RefSeq annotation timing varies by organism).*
