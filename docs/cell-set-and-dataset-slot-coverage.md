# CellSet and CellSetDataset slot coverage

Springbok-LLC/nlm-ckn-etl#63 (sources: NIH-NLM/nlm-ckn#255, NIH-NLM/nlm-ckn#257)

Measured over the tuples written for nlm-ckn `v1.0.0-rc.7` — the tuple files
are the complete set of vertex annotations the graph builder loads, so slot
presence there is slot presence in the graph. Counts confirmed against the
`data/tuples-1.0.0-rc.7/` baseline, which reproduces #257's finding exactly:
89 CellSetDataset documents in 5 distinct attribute sets.

## Result

| | before | after |
|---|---|---|
| distinct CSD attribute sets | 5 | 3 |
| distinct CellSet attribute sets | 3 | 3 |

The 3 remaining CSD attribute sets are the real groups in the data, not
loading gaps: 74 datasets whose organ has a CELLxGENE harvester table, 10
whose organ has none (all 9 heart, whose table is misnamed, plus Guo), and the
5 Jorstad neocortex superclusters that NS-Forest QC was never run on.

## Slot-by-slot enumeration

### CellSetDataset (89 documents)

| slot | before | after | source |
|---|---|---|---|
| `dataset_name` | 89 | 89 | summary `dataset_title` / CELLxGENE |
| `dataset_identifier` | 89 | 89 | composed from dvid + organ |
| `species` | 89 | 89 | constant |
| `version` | 89 | 89 | dvid |
| `dataset_collection_version` | 0 | **89** | CELLxGENE `Collection_version_ID`, harvester `collection_version_id` |
| `publication` | 15 | **89** | CELLxGENE `Link_to_publication`, summary `doi` |
| `anatomical_structure` | 89 | 89 | summary `organ` |
| `disease_status` | 89 | 89 | CELLxGENE / harvester `disease` |
| `cell_count` | 89 | 89 | CELLxGENE `Number_of_cells` |
| `cell_type` | 0 | 0 | **not loaded — see Open questions** |
| `cellxgene_collection` | 89 | 89 | summary `collection_url` |
| `cellxgene_dataset` | 89 | 89 | summary `explorer_url` |
| `collection_id` | 89 | 89 | CELLxGENE `Collection_ID` |
| `filtered_cell_count` | 76 | **84** | summary `filtered_cell_count` (was harvester `normal_cell_count`) |
| `embedding` | 89 | 89 | summary `embedding` |
| `tissue_annotation` | 76 | **84** | summary `tissue_ontology_summary` |
| `tissue_annotation_id` | 0 | 0 | **blocked by the schema — see Open questions** |
| `median_of_median_silhouette` | 0 | **84** | summary `median_silhouette` |
| `mean_f_beta_score` | 84 | 84 | summary `mean_fscore` |
| `median_of_f_beta_scores` | 84 | 84 | summary `median_fscore` |
| `mean_silhouette` | 84 | 84 | summary `mean_silhouette` |
| `standard_deviation_of_silhouette` | 84 | 84 | summary `std_silhouette` |
| `cluster_annotation` | 84 | 84 | summary `cluster_header` |
| `donor_id_count` | 76 | 74 | harvester `donor_id_count` only, from the dataset's own organ |
| `assay_summary` | 76 | **84** | summary `assay_ontology_summary` |
| `cluster_summary` | 0 | **84** | summary `n_clusters` |

"summary" is `master_dataset_summary_*.csv`.

### CellSet (2617 documents)

| slot | before | after | source |
|---|---|---|---|
| `author_cell_term` | 2617 | 2617 | results `clusterName` |
| `assay` | 0 | 0 | **not produced per cluster — see Open questions** |
| `ontology_purl` | 723 | 723 | manual CL mapping, which exists for 723 clusters only |
| `anatomical_structure` | 723 | **2617** | the dataset's root UBERON term |
| `species` | 723 | **2617** | constant |
| `publication` | 723 | **2617** | the dataset's DOI |
| `dataset_name` | 0 | **2617** | the dataset's name |
| `cell_count` | 2617 | 2617 | results `clusterSize` |
| `biomarker_combination` | 2617 | 2617 | results `NSForest_markers` |
| `binary_gene_set` | 2617 | 2617 | results `binary_genes` |
| `expressed_genes` | 2617 | 2617 | results `binary_genes` |
| `cellxgene_collection` | 0 | **2617** | the dataset's collection URL |
| `cellxgene_dataset` | 723 | **2617** | the dataset's h5ad URL |
| `label` | 0 | 0 | **no distinct value — see Open questions** |
| `silhouette_score` | 2464 | 2464 | silhouette `median` |
| `cluster_annotation` | 0 | **2464** | results `cluster_header` |
| `median_silhouette` | 2464 | 2464 | silhouette `median` |
| `mean_silhouette` | 2464 | 2464 | silhouette `mean` |
| `standard_deviation_of_silhouette` | 2464 | 2464 | silhouette `std` |
| `cluster_cell_count` | 2617 | 2617 | results `clusterSize` |
| `percent_total_cells` | 0 | 0 | **derivable, not produced — see Open questions** |
| `maximum_silhouette` | 0 | 0 | **not produced — see Open questions** |
| `minimum_silhouette` | 0 | 0 | **not produced — see Open questions** |
| `first_quartile_silhouette` | 2464 | 2464 | silhouette `q1` |
| `third_quartile_silhouette` | 2464 | 2464 | silhouette `q3` |
| `true_positive` / `false_negative` / `false_positive` | 2617 | 2617 | results `TP` / `FN` / `FP` |
| `on_target` | 2617 | 2617 | results `onTarget` |
| `f_beta_score` / `precision` / `recall` | 2617 | 2617 | results |

The 153 cell sets short of 2617 on every silhouette slot are all Jorstad's.

## What was wrong, and why

**The harvester tables do not cover every dataset.** `filtered_cell_count`,
`tissue_annotation`, `assay_summary` and `donor_id_count` were read only from
`*_harvester_final.csv`. Two prod files never match that glob —
`homo_sapiens_heart_plus_pericardium_final.csv` (no `harvester` in the name)
and `homo_sapiens_respiratory_system_harvester_guo.csv` — so 8 datasets got
none of those slots. `master_dataset_summary_*.csv` carries the same rollups
for all 84 non-Jorstad datasets, so it is now the primary source and the
harvester the fallback. `donor_id_count` has no summary equivalent, so it is
still harvester-only and now stops at 74 — the 15 documents without it are
exactly those whose organ has no harvester table: all 9 heart, all 5
neocortex, and Guo.

**The harvester join ignored the organ.** A source dataset filtered for
several organs yields one CSD per organ, but the harvester row was matched on
`dataset_version_id` alone, so all of them received the same counts. Wells
2025, for instance, reported 142608 filtered cells for its bone marrow,
respiratory system, and skin datasets alike; the correct per-organ counts are
142607, 213154, and 1731.

A harvester table holds one organ's rows but names that organ only in its
filename, so `get_cellxgene_harvester_data` now tags each row with the organ
it was read from and `get_harvester_row` selects on `(dataset_version_id,
organ)`. A dataset with one harvester row is matched regardless of organ,
since there is nothing to tell apart; a dataset with several and none for the
wanted organ gets none, an arbitrary organ's counts being worse than no
counts.

**`publication` came only from the manual mapping.** Only the 11 datasets
with a `cluster_cid_mapping_*.csv` reached `build_cell_set_dataset` with a
DOI, which is why exactly 15 CSDs carried the slot (10 datasets plus Jorstad's
5). CELLxGENE knows the DOI for every dataset and already used it to key the
`PUB_` vertex; it now also lands on the dataset vertex. DOIs are normalized
(protocol and resolver host stripped, lowercased) so a dataset names its paper
the same way its `PUB_` vertex key does.

**Cell sets were only described if they were mapped.** `species`,
`anatomical_structure`, `publication` and `cellxgene_dataset` were set in
`MappingTupleWriter` and nowhere else, so they reached only the 723 manually
mapped clusters. `NSForestTupleWriter` now resolves each cluster's dataset
before building the cell set and copies that dataset's metadata onto it.

## Deliberate value changes on reload

- **`filtered_cell_count` is read from `filtered_cell_count`, not
  `normal_cell_count`** — 33 values change. The slot's meaning is unchanged;
  the harvester's `normal_cell_count` was a differently-computed quantity.
  The two columns are disjoint across the two file kinds — only the harvester
  has `normal_cell_count`, only the summary has `filtered_cell_count` — so
  correcting the column also moves the read from the harvester to the master
  summary, which is what recovers the 8 datasets the harvester tables miss.

  This satisfies #64 as well: the summary's `filtered_cell_count` is
  bit-identical to `dataset_summary_*.csv`'s `n_cells` for all 84 datasets
  (verified column by column), so the value #64 asks for is the value loaded,
  without adding a third file to the loader. **#64's hold still applies** —
  do not ship the reload until the author of `n_cells` confirms the values.
- **`donor_id_count` is taken from the dataset's own organ** — 11 values
  change. 9 were another organ's count (Han (2020) loaded 2 donors for its
  digestive tract, kidney, liver, pancreas and respiratory system datasets
  alike, whose real counts are 16, 7, 4, 4 and 6). The other 2 are heart
  datasets that had borrowed a count from an unrelated organ and now have
  none, which is why coverage falls from 76 to 74. The 15 documents without
  the slot are now exactly those whose organ has no harvester table.
- **List separator in `tissue_annotation` and `assay_summary`** changes from
  `"; "` to `" | "` for 38–40 documents, because the summary spells these
  rollups with `" | "` and the harvester with `"; "`. Both sources are now
  rejoined on `" | "` in the builder, so the separator is a property of the
  slot rather than of whichever file covered the dataset, matching
  `tissue_ontology_term_id`. **Any UI that splits these strings on `;` needs
  updating.**
- **DOI case** — one CSD and 55 cell sets change from `10.1158/…BCD-24-0342`
  to the lowercased form.

## Open questions for NLM

Slots the workflow does not produce, or that the schema will not accept:

1. **`CellSetDataset.tissue_annotation_id`** — the value exists (the summary's
   `tissue_ontology_term_id`, e.g. `UBERON:0001225 | UBERON:0002113`), but the
   slot's range `TissueEnum` is generated with **no permissible values**, so
   Pydantic rejects every string. Either define the enum's values or change
   the range to `string`, and this loads for 84 documents immediately.
2. **`CellSetDataset.cell_type`** — the summary and harvester both carry
   `cell_type_ontology_summary` (`CL:0002306: 24278 | CL:1001106: 16705 | …`),
   the exact analogue of `tissue_annotation`. But the slot's description is a
   definition of a cell type, not of a summary. Confirm the intent and it
   loads for 84; otherwise it should be dropped from the class.
3. **`CellSetDataset.cluster_summary`** — loaded from the summary's
   `n_clusters`, per the description "Count of clusters in this dataset".
   Flagging it because the name suggests the `scsilhouette_cluster_summary_*`
   tables instead. Confirm or correct.
4. **`CellSet.maximum_silhouette` / `minimum_silhouette`** — not produced.
   `silhouette_fscore_summary_*.csv` reports mean, std, median, count, q1, q3
   but no extremes, though `silhouette_scores_*.csv` has the per-cell scores
   they would be computed from. Ask `sc-nsforest-qc-nf` to emit them, or drop
   the slots.
5. **`CellSet.percent_total_cells`** — derivable as the cluster's cell count
   over the dataset's, but the definition ("of all the cells for the dataset")
   does not say whether the denominator is `cell_count` or
   `filtered_cell_count`. Not computed pending that answer.
6. **`CellSet.assay`** — only a dataset-scoped `assay_ontology_summary`
   exists; nothing reports a per-cluster assay. Drop the slot or redefine it.
7. **`CellSet.label`** — no value distinct from `author_cell_term` is
   produced. Drop it, or say what it should hold.
8. **`CellSet.ontology_purl` stops at 723 of 2617** — this is the manual
   author-to-CL mapping's coverage, which is expected, not a load gap.
9. **The 5 Jorstad neocortex superclusters have no NS-Forest QC at all** —
   no `dataset_summary`, no `scsilhouette` or `silhouette_fscore_summary`
   file, and a hand-built `master_dataset_summary` whose `cluster_header`,
   `mean_silhouette`, `std_silhouette`, `mean_fscore` and `median_fscore`
   columns are empty. So `cluster_annotation`, all four dataset-level stats,
   `filtered_cell_count`, `tissue_annotation` and `assay_summary` cannot be
   loaded for them, and their 153 cell sets have no silhouette statistics.
   These are the 5-document and 153-cell-set attribute sets. Decide whether
   to run the QC workflow on them or accept the gap.
10. **Two prod files are named outside the spec** —
    `homo_sapiens_heart_plus_pericardium_final.csv` is a harvester table
    missing `harvester` in its name, and
    `homo_sapiens_respiratory_system_harvester_guo.csv` is a per-dataset
    harvester table outside the `*_harvester_final.csv` convention. Renaming
    them upstream would recover `donor_id_count` for 10 of the 15 documents
    that lack it; the 5 Jorstad ones have no harvester table at all. The
    harvester tables also carry no `organ` column, so the ETL has to parse
    the organ out of the filename to join them correctly — an explicit
    column would be sturdier than a naming convention that two files already
    break.
11. **`master_dataset_summary_retina_Xu_Cell_2023_…_5188c1.csv` has
    `dataset_title` = `Bone_marrow`** for a retina run. The dataset name and
    filtered cell count loaded for that CSD follow the summary, so the vertex
    reads "Xu (2023) Cell - Bone_marrow" under a retina identifier. Upstream
    labeling question, not an ETL one.
12. **`cellxgene_dataset` has two spellings** — the CELLxGENE writer sets the
    h5ad URL, the summary-driven writers set the explorer URL, and whichever
    tuple file loads last wins. Not introduced here, but worth settling.
