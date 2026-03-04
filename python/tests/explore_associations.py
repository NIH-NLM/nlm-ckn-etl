"""Explore creating Pydantic Association instances from test data.

Systematically attempts to instantiate each Association class from the
available test data's results/data sections (not tuples), reporting
successes, failures, and ambiguities.
"""

import ast
import json
import re
import sys
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from ckn_schema.pydantic.ckn_schema import (
    AnatomicalStructure,
    AnatomicalStructurePartOfAnatomicalStructure,
    BiomarkerCombination,
    BiomarkerCombinationSubclusterOfBinaryGeneSet,
    BinaryGeneSet,
    CellSet,
    CellSetComposedPrimarilyOfCellType,
    CellSetDataset,
    CellSetDatasetHasSourcePublication,
    CellSetDerivesFromAnatomicalStructure,
    CellSetExactMatchCellSet,
    CellSetExpressesBinaryGeneSet,
    CellSetHasCharacterizingMarkerSetBiomarkerCombination,
    CellSetHasSourceCellSetDataset,
    CellType,
    CellTypeDevelopsFromCellType,
    CellTypeExpressesGene,
    CellTypeHasExemplarDataCellSetDataset,
    CellTypeHasPlasmaMembranePartProtein,
    CellTypeInteractsWithCellType,
    CellTypeLacksPlasmaMembranePartProtein,
    CellTypePartOfAnatomicalStructure,
    CellTypeSubclassOfCellType,
    ClinicalTrial,
    Disease,
    Drug,
    DrugEvaluatedInClinicalTrial,
    DrugIsSubstanceThatTreatsDisease,
    DrugMolecularlyInteractsWithGene,
    DrugMolecularlyInteractsWithProtein,
    Gene,
    GeneGeneticallyInteractsWithGene,
    GeneHasQualityMutation,
    GeneIsGeneticBasisForDisease,
    GeneMolecularlyInteractsWithDrug,
    GenePartOfBiomarkerCombination,
    GeneProducesProtein,
    Mutation,
    MutationHasPharamcologicalEffectDrug,
    Protein,
    ProteinCapableOfMolecularFunction,
    ProteinInvolvedInBiologicalProcess,
    ProteinLocatedInCellularComponent,
    ProteinPartOfCellType,
    Publication,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
TEST_DATA = PROJECT_ROOT / "src" / "test" / "data"
SUMMARIES = TEST_DATA / "summaries"
TUPLES = TEST_DATA / "tuples"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def purl_to_curie(purl: str) -> str:
    """Convert an OBO PURL to a CURIE.

    "http://purl.obolibrary.org/obo/CL_0000235"  -> "CL:0000235"
    "https://purl.obolibrary.org/obo/CL_4030027"  -> "CL:4030027"
    "UBERON:0000955" (already a CURIE)             -> "UBERON:0000955"
    """
    m = re.match(r"https?://purl\.obolibrary\.org/obo/(\w+?)_(\d+)$", purl)
    if m:
        return f"{m.group(1)}:{m.group(2)}"
    return purl


def parse_string_list(s: str) -> list[str]:
    """Parse a stringified Python list.

    "['SLC12A7', 'OTOGL']" -> ["SLC12A7", "OTOGL"]
    """
    try:
        result = ast.literal_eval(s)
        if isinstance(result, list):
            return [str(x) for x in result]
    except (ValueError, SyntaxError):
        pass
    return []


def get_columnar_row(data: dict, row_key: str = "0") -> dict[str, Any]:
    """Extract a single row from columnar (pandas-style) JSON.

    Columnar format: {"col_name": {"0": value, "1": value, ...}}
    Returns: {"col_name": value, ...} for the given row_key.
    """
    row = {}
    for col, values in data.items():
        if isinstance(values, dict) and row_key in values:
            row[col] = values[row_key]
        elif isinstance(values, dict):
            # Try first available key
            first_key = next(iter(values), None)
            if first_key is not None:
                row[col] = values[first_key]
    return row


def try_create(cls, **kwargs) -> tuple[Any | None, str | None]:
    """Try to create a Pydantic instance, return (instance, error)."""
    try:
        instance = cls(**kwargs)
        return instance, None
    except (ValidationError, Exception) as e:
        return None, str(e)


# ---------------------------------------------------------------------------
# Result tracking
# ---------------------------------------------------------------------------

ALL_ASSOCIATIONS = [
    "CellTypePartOfAnatomicalStructure",
    "AnatomicalStructurePartOfAnatomicalStructure",
    "CellTypeExpressesGene",
    "CellSetComposedPrimarilyOfCellType",
    "CellSetDerivesFromAnatomicalStructure",
    "CellSetHasSourceCellSetDataset",
    "CellSetDatasetHasSourcePublication",
    "CellTypeHasExemplarDataCellSetDataset",
    "GenePartOfBiomarkerCombination",
    "CellSetHasCharacterizingMarkerSetBiomarkerCombination",
    "BiomarkerCombinationSubclusterOfBinaryGeneSet",
    "CellSetExpressesBinaryGeneSet",
    "GeneIsGeneticBasisForDisease",
    "GeneProducesProtein",
    "GeneGeneticallyInteractsWithGene",
    "DrugIsSubstanceThatTreatsDisease",
    "DrugMolecularlyInteractsWithGene",
    "GeneMolecularlyInteractsWithDrug",
    "DrugEvaluatedInClinicalTrial",
    "DrugMolecularlyInteractsWithProtein",
    "CellTypeSubclassOfCellType",
    "CellTypeInteractsWithCellType",
    "CellTypeDevelopsFromCellType",
    "CellTypeHasPlasmaMembranePartProtein",
    "CellTypeLacksPlasmaMembranePartProtein",
    "ProteinPartOfCellType",
    "GeneHasQualityMutation",
    "MutationHasPharamcologicalEffectDrug",
    "CellSetExactMatchCellSet",
    "ProteinCapableOfMolecularFunction",
    "ProteinInvolvedInBiologicalProcess",
    "ProteinLocatedInCellularComponent",
]

results: dict[str, dict] = {}


def record(name: str, status: str, source: str, instance: Any = None,
           error: str | None = None, notes: str = "") -> None:
    """Record a result for a given Association class."""
    results[name] = {
        "status": status,
        "source": source,
        "instance": instance,
        "error": error,
        "notes": notes,
    }


# ---------------------------------------------------------------------------
# 1. test-triples.json (tuples section - RDF triples)
# ---------------------------------------------------------------------------

# def from_test_triples() -> None:
#     """Extract associations from test-triples.json tuples."""
#     with open(TUPLES / "test-triples.json") as f:
#         data = json.load(f)

#     tuples = data.get("tuples", [])

#     # Triple: [CL_0000235, rdfs:subClassOf, CL_0000145]
#     for t in tuples:
#         if len(t) >= 3 and "subClassOf" in t[1]:
#             subj_curie = purl_to_curie(t[0])
#             obj_curie = purl_to_curie(t[2])
#             inst, err = try_create(
#                 CellTypeSubclassOfCellType,
#                 subject=CellType(ontology_purl=subj_curie),
#                 predicate="subclass_of",
#                 object=CellType(ontology_purl=obj_curie),
#             )
#             record("CellTypeSubclassOfCellType", "Created" if inst else "FAILED",
#                    "test-triples.json", inst, err,
#                    f"{subj_curie} subClassOf {obj_curie}")
#             return


# ---------------------------------------------------------------------------
# 2. hubmap-allen-brain-v1.7.json (data section)
# ---------------------------------------------------------------------------

def from_hubmap() -> None:
    """Extract associations from HuBMAP data section."""
    with open(SUMMARIES / "hubmap-allen-brain-v1.7.json") as f:
        data = json.load(f)

    hubmap = data.get("data", {}).get("hubmap", {})
    cell_types = hubmap.get("cell_types", [])
    anat_structs = hubmap.get("anatomical_structures", [])

    # --- CellTypeSubclassOfCellType from ccf_ct_isa ---
    # for ct in cell_types:
    #     for parent_id in ct.get("ccf_ct_isa", []):
    #         subj_curie = ct["id"]  # e.g. "CL:0000236"
    #         obj_curie = parent_id  # e.g. "CL:0000000"
    #         inst, err = try_create(
    #             CellTypeSubclassOfCellType,
    #             subject=CellType(ontology_purl=subj_curie),
    #             predicate="subclass_of",
    #             object=CellType(ontology_purl=obj_curie),
    #         )
    #         if "CellTypeSubclassOfCellType" not in results:
    #             record("CellTypeSubclassOfCellType",
    #                    "Created" if inst else "FAILED",
    #                    "hubmap", inst, err,
    #                    f"{subj_curie} subClassOf {obj_curie}")
    #         break
    #     break

    # --- CellTypePartOfAnatomicalStructure from ccf_located_in ---
    for ct in cell_types:
        for uberon_id in ct.get("ccf_located_in", []):
            inst, err = try_create(
                CellTypePartOfAnatomicalStructure,
                subject=CellType(ontology_purl=ct["id"]),
                predicate="part_of",
                object=AnatomicalStructure(ontology_purl=uberon_id),
            )
            record("CellTypePartOfAnatomicalStructure",
                   "Created" if inst else "FAILED",
                   "hubmap", inst, err,
                   f"{ct['id']} located_in {uberon_id}. "
                   "Note: ccf_located_in vs part_of semantic mismatch")
            break
        break

    # --- AnatomicalStructurePartOfAnatomicalStructure from ccf_part_of ---
    for as_ in anat_structs:
        for parent_id in as_.get("ccf_part_of", []):
            inst, err = try_create(
                AnatomicalStructurePartOfAnatomicalStructure,
                subject=AnatomicalStructure(ontology_purl=as_["id"]),
                predicate="part_of",
                object=AnatomicalStructure(ontology_purl=parent_id),
            )
            record("AnatomicalStructurePartOfAnatomicalStructure",
                   "Created" if inst else "FAILED",
                   "hubmap", inst, err,
                   f"{as_['id']} part_of {parent_id}")
            break
        break


# ---------------------------------------------------------------------------
# 3. nlm-ckn-nsforest-results-li-2023.json (results section)
# ---------------------------------------------------------------------------

def from_nsforest() -> None:
    """Extract associations from NSForest results."""
    with open(SUMMARIES / "nlm-ckn-nsforest-results-li-2023.json") as f:
        data = json.load(f)

    row = get_columnar_row(data.get("results", {}))
    cluster_name = row.get("clusterName", "")
    cluster_size = row.get("clusterSize")
    f_score = row.get("f_score")
    uuid = row.get("uuid", "")
    markers_str = row.get("NSForest_markers", "")
    binary_str = row.get("binary_genes", "")

    markers = parse_string_list(markers_str)
    binary_genes = parse_string_list(binary_str)

    # Build entities
    bmc = BiomarkerCombination(
        markers=" ".join(markers),
        f_beta_score=f_score,
    )
    bgs = BinaryGeneSet(markers=" ".join(binary_genes))
    cell_set = CellSet(
        author_cell_term=cluster_name,
        cell_count=int(cluster_size) if cluster_size else None,
    )

    # --- GenePartOfBiomarkerCombination ---
    if markers:
        gene_symbol = markers[0]
        inst, err = try_create(
            GenePartOfBiomarkerCombination,
            subject=gene_symbol,
            predicate="part_of",
            object=bmc,
        )
        record("GenePartOfBiomarkerCombination",
               "Created" if inst else "FAILED",
               "nsforest", inst, err,
               f"Gene {gene_symbol} part_of BMC({markers})")

    # --- CellSetHasCharacterizingMarkerSetBiomarkerCombination ---
    inst, err = try_create(
        CellSetHasCharacterizingMarkerSetBiomarkerCombination,
        subject=cell_set,
        predicate="has_characterizing_marker_set",
        object=bmc,
    )
    record("CellSetHasCharacterizingMarkerSetBiomarkerCombination",
           "Created" if inst else "FAILED",
           "nsforest", inst, err,
           f"CellSet({cluster_name}) -> BMC({markers})")

    # --- BiomarkerCombinationSubclusterOfBinaryGeneSet ---
    inst, err = try_create(
        BiomarkerCombinationSubclusterOfBinaryGeneSet,
        subject=bmc,
        predicate="subcluster_of",
        object=bgs,
    )
    record("BiomarkerCombinationSubclusterOfBinaryGeneSet",
           "Created" if inst else "FAILED",
           "nsforest", inst, err,
           f"BMC({markers}) subcluster_of BGS({len(binary_genes)} genes)")


# ---------------------------------------------------------------------------
# 4. nlm-ckn-map-author-to-cl-li-2023.json (results section)
# ---------------------------------------------------------------------------

def from_author_to_cl() -> None:
    """Extract associations from author-to-CL mapping results."""
    with open(SUMMARIES / "nlm-ckn-map-author-to-cl-li-2023.json") as f:
        data = json.load(f)

    row = get_columnar_row(data.get("results", {}))
    cl_purl = row.get("cell_ontology_id", "")
    cl_curie = purl_to_curie(cl_purl)
    uberon_purl = row.get("uberon_entity_id", "")
    uberon_curie = purl_to_curie(uberon_purl)
    author_cell_term = row.get("author_cell_term", "")
    cluster_name = row.get("clusterName", "")
    cluster_size = row.get("clusterSize")
    uuid = row.get("uuid", "")
    dataset_version_id = row.get("dataset_version_id", "")
    dataset_id = row.get("dataset_id", "")
    collection_id = row.get("collection_id", "")
    pmid = str(row.get("PMID", ""))
    pmcid = row.get("PMCID", "")
    doi = row.get("DOI", "")
    markers_str = row.get("NSForest_markers", "")
    binary_str = row.get("binary_genes", "")
    markers = parse_string_list(markers_str)
    binary_genes = parse_string_list(binary_str)

    # Build entities
    cell_type = CellType(ontology_purl=cl_curie, label=row.get("cell_ontology_term"))
    anat_struct = AnatomicalStructure(
        ontology_purl=uberon_curie,
        label=row.get("uberon_entity_term"),
    )
    cell_set = CellSet(
        author_cell_term=author_cell_term,
        cell_count=int(cluster_size) if cluster_size else None,
    )
    dataset = CellSetDataset(
        dataset_identifier=dataset_version_id,
        publication=doi,
    )
    publication = Publication(
        pmid=pmid,
        pmcid=pmcid,
        publication_doi_identifier=doi,
    )
    bmc = BiomarkerCombination(markers=" ".join(markers))
    bgs = BinaryGeneSet(markers=" ".join(binary_genes))

    # --- CellTypePartOfAnatomicalStructure ---
    if "CellTypePartOfAnatomicalStructure" not in results:
        inst, err = try_create(
            CellTypePartOfAnatomicalStructure,
            subject=cell_type,
            predicate="part_of",
            object=anat_struct,
        )
        record("CellTypePartOfAnatomicalStructure",
               "Created" if inst else "FAILED",
               "author-to-cl", inst, err,
               f"{cl_curie} part_of {uberon_curie}")

    # --- CellSetComposedPrimarilyOfCellType ---
    inst, err = try_create(
        CellSetComposedPrimarilyOfCellType,
        subject=cell_set,
        predicate="composed_primarily_of",
        object=cell_type,
    )
    record("CellSetComposedPrimarilyOfCellType",
           "Created" if inst else "FAILED",
           "author-to-cl", inst, err,
           f"CellSet({author_cell_term}) -> CellType({cl_curie})")

    # --- CellSetDerivesFromAnatomicalStructure ---
    inst, err = try_create(
        CellSetDerivesFromAnatomicalStructure,
        subject=cell_set,
        predicate="derives_from",
        object=anat_struct,
    )
    record("CellSetDerivesFromAnatomicalStructure",
           "Created" if inst else "FAILED",
           "author-to-cl", inst, err,
           f"CellSet({author_cell_term}) derives_from {uberon_curie}")

    # --- CellSetHasSourceCellSetDataset ---
    inst, err = try_create(
        CellSetHasSourceCellSetDataset,
        subject=cell_set,
        predicate="source",
        object=dataset,
    )
    record("CellSetHasSourceCellSetDataset",
           "Created" if inst else "FAILED",
           "author-to-cl", inst, err,
           f"CellSet -> CellSetDataset({dataset_version_id[:12]}...)")

    # --- CellSetDatasetHasSourcePublication ---
    inst, err = try_create(
        CellSetDatasetHasSourcePublication,
        subject=dataset,
        predicate="source",
        object=publication,
    )
    record("CellSetDatasetHasSourcePublication",
           "Created" if inst else "FAILED",
           "author-to-cl", inst, err,
           f"CellSetDataset -> Publication(PMID:{pmid}). "
           "Note: title/year/journal only in tuples section")

    # --- CellTypeHasExemplarDataCellSetDataset ---
    inst, err = try_create(
        CellTypeHasExemplarDataCellSetDataset,
        subject=cell_type,
        predicate="has_exemplar_data",
        object=dataset,
    )
    record("CellTypeHasExemplarDataCellSetDataset",
           "Created" if inst else "FAILED",
           "author-to-cl", inst, err,
           f"CellType({cl_curie}) -> CellSetDataset")

    # --- CellTypeExpressesGene ---
    if markers:
        inst, err = try_create(
            CellTypeExpressesGene,
            subject=cell_type,
            predicate="expresses",
            object=markers[0],
        )
        record("CellTypeExpressesGene",
               "Created" if inst else "FAILED",
               "author-to-cl", inst, err,
               f"CellType({cl_curie}) expresses {markers[0]}. "
               "Note: object is str (gene symbol), not Gene entity")

    # --- CellSetExpressesBinaryGeneSet ---
    inst, err = try_create(
        CellSetExpressesBinaryGeneSet,
        subject=cell_set,
        predicate="expresses",
        object=bgs,
    )
    record("CellSetExpressesBinaryGeneSet",
           "Created" if inst else "FAILED",
           "author-to-cl", inst, err,
           f"CellSet({author_cell_term}) -> BGS({len(binary_genes)} genes)")

    # --- Repeat BMC/BGS associations if not already recorded ---
    if "GenePartOfBiomarkerCombination" not in results and markers:
        inst, err = try_create(
            GenePartOfBiomarkerCombination,
            subject=markers[0],
            predicate="part_of",
            object=bmc,
        )
        record("GenePartOfBiomarkerCombination",
               "Created" if inst else "FAILED",
               "author-to-cl", inst, err,
               f"Gene({markers[0]}) part_of BMC")

    if "CellSetHasCharacterizingMarkerSetBiomarkerCombination" not in results:
        inst, err = try_create(
            CellSetHasCharacterizingMarkerSetBiomarkerCombination,
            subject=cell_set,
            predicate="has_characterizing_marker_set",
            object=bmc,
        )
        record("CellSetHasCharacterizingMarkerSetBiomarkerCombination",
               "Created" if inst else "FAILED",
               "author-to-cl", inst, err)

    if "BiomarkerCombinationSubclusterOfBinaryGeneSet" not in results:
        inst, err = try_create(
            BiomarkerCombinationSubclusterOfBinaryGeneSet,
            subject=bmc,
            predicate="subcluster_of",
            object=bgs,
        )
        record("BiomarkerCombinationSubclusterOfBinaryGeneSet",
               "Created" if inst else "FAILED",
               "author-to-cl", inst, err)


# ---------------------------------------------------------------------------
# 5. nlm-ckn-external-api-results.json (results section)
# ---------------------------------------------------------------------------

def from_external_api() -> None:
    """Extract associations from external API results."""
    with open(SUMMARIES / "nlm-ckn-external-api-results.json") as f:
        data = json.load(f)

    api_results = data.get("results", {})

    # --- OpenTargets ---
    ot = api_results.get("opentargets", {})
    gene_ids = ot.get("gene_ensembl_ids", [])
    if gene_ids:
        gene_id = gene_ids[0]
        gene_data = ot.get(gene_id, {})
        target = gene_data.get("target", {})
        gene_symbol = target.get("approvedSymbol", "")
        diseases = gene_data.get("diseases", [])
        drugs = gene_data.get("drugs", [])
        interactions = gene_data.get("interactions", [])

        # --- GeneIsGeneticBasisForDisease ---
        if diseases:
            d = diseases[0]
            disease_info = d.get("disease", {})
            disease_id = disease_info.get("id", "")
            # Convert MONDO_0009061 to MONDO:0009061
            disease_curie = disease_id.replace("_", ":")
            inst, err = try_create(
                GeneIsGeneticBasisForDisease,
                subject=gene_symbol,
                predicate="is_genetic_basis_for_condition",
                object=Disease(
                    ontology_purl=disease_curie,
                    label=disease_info.get("name"),
                    definition=disease_info.get("description"),
                ),
            )
            record("GeneIsGeneticBasisForDisease",
                   "Created" if inst else "FAILED",
                   "external-api (opentargets)", inst, err,
                   f"{gene_symbol} -> Disease({disease_curie})")

        # --- DrugIsSubstanceThatTreatsDisease ---
        if drugs:
            drug_entry = drugs[0]
            drug_info = drug_entry.get("drug", {})
            drug_name = drug_info.get("name", drug_entry.get("approvedName", ""))
            disease_id = drug_entry.get("diseaseId", "")
            disease_curie = disease_id.replace("_", ":")
            inst, err = try_create(
                DrugIsSubstanceThatTreatsDisease,
                subject=Drug(name=drug_name,
                             mechanism_of_action=drug_entry.get("mechanismOfAction")),
                predicate="is_substance_that_treats",
                object=Disease(ontology_purl=disease_curie),
            )
            record("DrugIsSubstanceThatTreatsDisease",
                   "Created" if inst else "FAILED",
                   "external-api (opentargets)", inst, err,
                   f"Drug({drug_name}) -> Disease({disease_curie}). "
                   f"Note: diseaseId uses EFO namespace, not MONDO")

        # --- DrugMolecularlyInteractsWithGene ---
        if drugs:
            drug_entry = drugs[0]
            drug_info = drug_entry.get("drug", {})
            drug_name = drug_info.get("name", drug_entry.get("approvedName", ""))
            inst, err = try_create(
                DrugMolecularlyInteractsWithGene,
                subject=Drug(name=drug_name),
                predicate="molecularly_interacts_with",
                object=gene_symbol,
            )
            record("DrugMolecularlyInteractsWithGene",
                   "Created" if inst else "FAILED",
                   "external-api (opentargets)", inst, err,
                   f"Drug({drug_name}) -> Gene({gene_symbol})")

        # --- GeneMolecularlyInteractsWithDrug ---
        if drugs:
            drug_entry = drugs[0]
            drug_info = drug_entry.get("drug", {})
            drug_name = drug_info.get("name", drug_entry.get("approvedName", ""))
            inst, err = try_create(
                GeneMolecularlyInteractsWithDrug,
                subject=gene_symbol,
                predicate="molecularly_interacts_with",
                object=Drug(name=drug_name),
            )
            record("GeneMolecularlyInteractsWithDrug",
                   "Created" if inst else "FAILED",
                   "external-api (opentargets)", inst, err,
                   f"Gene({gene_symbol}) -> Drug({drug_name})")

        # --- DrugEvaluatedInClinicalTrial ---
        ct_created = False
        for drug_entry in drugs:
            ct_ids = drug_entry.get("ctIds", [])
            if ct_ids:
                drug_info = drug_entry.get("drug", {})
                drug_name = drug_info.get("name", drug_entry.get("approvedName", ""))
                ct_id = ct_ids[0]
                inst, err = try_create(
                    DrugEvaluatedInClinicalTrial,
                    subject=Drug(name=drug_name),
                    predicate="evaluated_in",
                    object=ClinicalTrial(study_id=ct_id),
                )
                record("DrugEvaluatedInClinicalTrial",
                       "Created" if inst else "FAILED",
                       "external-api (opentargets)", inst, err,
                       f"Drug({drug_name}) -> ClinicalTrial({ct_id}). "
                       "Note: only some drugs have ctIds")
                ct_created = True
                break
        if not ct_created:
            record("DrugEvaluatedInClinicalTrial", "PARTIAL",
                   "external-api (opentargets)", None, None,
                   "No drugs in test data have ctIds")

        # --- GeneGeneticallyInteractsWithGene ---
        if interactions:
            ix = interactions[0]
            target_b = ix.get("targetB", {})
            gene_b_symbol = target_b.get("approvedSymbol", "")
            inst, err = try_create(
                GeneGeneticallyInteractsWithGene,
                subject=gene_symbol,
                predicate="genetically_interacts_with",
                object=gene_b_symbol,
            )
            record("GeneGeneticallyInteractsWithGene",
                   "Created" if inst else "FAILED",
                   "external-api (opentargets)", inst, err,
                   f"{gene_symbol} <-> {gene_b_symbol}")

        # --- GeneProducesProtein ---
        # Get protein IDs from target or interactions
        protein_ids = target.get("proteinIds", [])
        uniprot_id = None
        for pid in protein_ids:
            if pid.get("source") == "uniprot_swissprot":
                uniprot_id = pid["id"]
                break
        if not uniprot_id and interactions:
            ix = interactions[0]
            for pid in ix.get("targetA", {}).get("proteinIds", []):
                if "uniprot" in pid.get("source", ""):
                    uniprot_id = pid["id"]
                    break
        if uniprot_id:
            inst, err = try_create(
                GeneProducesProtein,
                subject=gene_symbol,
                predicate="produces",
                object=Protein(uniprot_id=uniprot_id),
            )
            record("GeneProducesProtein",
                   "Created" if inst else "FAILED",
                   "external-api (opentargets)", inst, err,
                   f"{gene_symbol} -> Protein({uniprot_id})")
        else:
            record("GeneProducesProtein", "PARTIAL",
                   "external-api (opentargets)", None, None,
                   "No UniProt ID found in target proteinIds")

        # --- DrugMolecularlyInteractsWithProtein ---
        if drugs and uniprot_id:
            drug_entry = drugs[0]
            drug_info = drug_entry.get("drug", {})
            drug_name = drug_info.get("name", drug_entry.get("approvedName", ""))
            inst, err = try_create(
                DrugMolecularlyInteractsWithProtein,
                subject=Drug(name=drug_name),
                predicate="molecularly_interacts_with",
                object=Protein(uniprot_id=uniprot_id),
            )
            record("DrugMolecularlyInteractsWithProtein",
                   "Created" if inst else "FAILED",
                   "external-api (opentargets)", inst, err,
                   f"Drug({drug_name}) -> Protein({uniprot_id}). "
                   "Note: indirect - drug targets gene, gene produces protein")
        elif not drugs:
            record("DrugMolecularlyInteractsWithProtein", "PARTIAL",
                   "external-api (opentargets)", None, None,
                   "No drugs in test data")


# ---------------------------------------------------------------------------
# Mark associations with no data available
# ---------------------------------------------------------------------------

NO_DATA = {
    # From CL?
    "CellTypeSubclassOfCellType": "No cell-cell interaction data in test files",
    "CellTypeInteractsWithCellType": "No cell-cell interaction data in test files",
    "CellTypeDevelopsFromCellType": "No developmental lineage data in test files",
    "CellTypeHasPlasmaMembranePartProtein": "No plasma membrane protein data",
    "CellTypeLacksPlasmaMembranePartProtein": "No plasma membrane protein data",
    # From Open Targets?
    "GeneHasQualityMutation": "No mutation data in results sections",
    "MutationHasPharamcologicalEffectDrug": "No mutation-drug data in results sections",
    # ?
    "ProteinPartOfCellType": "No protein-celltype data",
    "ProteinCapableOfMolecularFunction": "No protein function data",
    "ProteinInvolvedInBiologicalProcess": "No protein-process data",
    "ProteinLocatedInCellularComponent": "No protein-component data",
    # From FRMatch?
    "CellSetExactMatchCellSet": "No cell set matching data",
}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def print_report() -> None:
    """Print a structured report of all results."""
    print("=" * 90)
    print("ASSOCIATION CREATION REPORT")
    print("=" * 90)

    created = 0
    partial = 0
    failed = 0
    not_possible = 0

    for name in ALL_ASSOCIATIONS:
        r = results.get(name)
        if r is None:
            # Should have been recorded somewhere
            status = "NOT RECORDED"
            source = "???"
            notes = ""
        else:
            status = r["status"]
            source = r["source"]
            notes = r["notes"]

        if status == "Created":
            marker = "OK"
            created += 1
        elif status == "PARTIAL":
            marker = "~~"
            partial += 1
        elif status == "FAILED":
            marker = "XX"
            failed += 1
        elif status == "Not possible":
            marker = "--"
            not_possible += 1
        else:
            marker = "??"

        print(f"\n[{marker}] {name}")
        print(f"     Status: {status}")
        print(f"     Source: {source}")
        if notes:
            print(f"     Notes:  {notes}")
        if r and r.get("error"):
            err_lines = r["error"].split("\n")
            print(f"     Error:  {err_lines[0]}")
            for line in err_lines[1:4]:
                print(f"             {line}")

    print("\n" + "=" * 90)
    print("SUMMARY")
    print("=" * 90)
    print(f"  Created:      {created}")
    print(f"  Partial:      {partial}")
    print(f"  Failed:       {failed}")
    print(f"  Not possible: {not_possible}")
    print(f"  Total:        {len(ALL_ASSOCIATIONS)}")

    print("\n" + "=" * 90)
    print("KEY AMBIGUITIES")
    print("=" * 90)
    ambiguities = [
        ("PURL-to-CURIE conversion",
         "Data uses http(s)://purl.obolibrary.org/obo/CL_0000235 but "
         "CellType.ontology_purl validates CL:[0-9]{7} - must convert"),
        ("Gene typed as str in Associations",
         "CellTypeExpressesGene.object is Optional[str] (gene symbol), "
         "not Optional[Gene]"),
        ("Species format",
         "Data has 'Homo sapiens' but schema expects CURIEs like NCBITaxon:9606"),
        ("ccf_located_in vs part_of",
         "Semantic mismatch - HuBMAP uses ccf_located_in but schema "
         "relation is part_of"),
        ("HuBMAP markers on CellType",
         "Schema puts has_characterizing_marker_set on CellSet, "
         "but HuBMAP data has markers on CellType"),
        ("Drug disease ID namespace",
         "Drug diseaseId uses EFO (EFO_0000684) but diseases use MONDO"),
        ("NSForest_markers as string",
         "Stored as \"['SLC12A7', 'OTOGL']\" - needs ast.literal_eval parsing"),
        ("Missing Publication fields",
         "From results: only PMID/PMCID/DOI available. "
         "Title/year/journal/authors only in tuples section"),
    ]
    for i, (title, desc) in enumerate(ambiguities, 1):
        print(f"  {i}. {title}")
        print(f"     {desc}")

    print()


def main() -> None:
    # from_test_triples()
    from_hubmap()
    from_nsforest()
    from_author_to_cl()
    from_external_api()

    # Mark associations with no data
    for name, reason in NO_DATA.items():
        if name not in results:
            record(name, "Not possible", "N/A", notes=reason)

    # Catch any unrecorded associations
    for name in ALL_ASSOCIATIONS:
        if name not in results:
            record(name, "NOT RECORDED", "N/A",
                   notes="Not attempted - check source functions")

    print_report()


if __name__ == "__main__":
    main()
