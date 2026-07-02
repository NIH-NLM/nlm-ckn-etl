import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd

from MappingTupleWriter import create_tuples


class MappingTupleWriterTestCase(unittest.TestCase):
    """Tests for MappingTupleWriter.create_tuples (cluster_cid_mapping schema)."""

    def _make_data(self):
        # Merged cluster_cid_mapping + NSForest results (one cluster row).
        return pd.DataFrame({
            "dataset_version_id": ["dv-001"],
            "cluster_name": ["T-Cell"],
            "skos": ["skos:broadMatch"],
            "manual_mapped_cid": ["CL:0000084"],   # manual curation (wins)
            "cell_ontology_id": ["CL:0000236"],     # automatic (fallback)
            "clusterName": ["T-Cell"],
            "clusterSize": [1718],
            "NSForest_markers": ["['TP53', 'BRCA1']"],
            "binary_genes": ["['TP53', 'BRCA1', 'EGFR']"],
            "uuid": ["abc123"],
        })

    def _make_summary(self):
        return pd.DataFrame({
            "dataset_version_id": ["dv-001"],
            "tissue_ontology_term_id": ["UBERON:0000966"],
            "doi": ["doi.org/10.1101/2023.11.07.566105"],
        })

    def test_creates_tuples(self):
        tuples = create_tuples(self._make_data(), self._make_summary())
        self.assertGreater(len(tuples), 0)

    def test_contains_composed_primarily_of(self):
        tuples = create_tuples(self._make_data(), self._make_summary())
        preds = [str(t[1]) for t in tuples if len(t) == 3]
        self.assertTrue(any("RO_0002473" in p for p in preds))

    def test_contains_has_exemplar_data(self):
        tuples = create_tuples(self._make_data(), self._make_summary())
        preds = [str(t[1]) for t in tuples if len(t) == 3]
        self.assertTrue(any("RO_0015001" in p for p in preds))

    def test_skos_edge_annotation(self):
        # skos replaces the old Match / Mapping_method edge annotations.
        tuples = create_tuples(self._make_data(), self._make_summary())
        edge_annots = [
            t for t in tuples
            if len(t) == 5 and "skos" in str(t[3])
        ]
        self.assertGreater(len(edge_annots), 0)
        self.assertIn("skos:broadMatch", str(edge_annots[0][4]))

    def test_manual_mapped_cid_wins(self):
        # manual_mapped_cid (CL:0000084) takes priority over cell_ontology_id
        # (CL:0000236) when both are present.
        tuples = create_tuples(self._make_data(), self._make_summary())
        blob = " ".join(str(part) for t in tuples for part in t)
        self.assertIn("CL_0000084", blob)
        self.assertNotIn("CL_0000236", blob)

    def test_falls_back_to_cell_ontology_id(self):
        # With no manual curation, the automatic cell_ontology_id is used.
        data = self._make_data()
        data.loc[0, "manual_mapped_cid"] = ""
        tuples = create_tuples(data, self._make_summary())
        blob = " ".join(str(part) for t in tuples for part in t)
        self.assertIn("CL_0000236", blob)

    def test_skips_non_cl_ids(self):
        data = self._make_data()
        data.loc[0, "manual_mapped_cid"] = ""
        data.loc[0, "cell_ontology_id"] = "UBERON:0004225"
        tuples = create_tuples(data, self._make_summary())
        self.assertEqual(len(tuples), 0)

    def test_skips_small_clusters(self):
        data = self._make_data()
        data.loc[0, "clusterSize"] = 5
        tuples = create_tuples(data, self._make_summary())
        self.assertEqual(len(tuples), 0)

    def test_merge_uuid_collision_resolved(self):
        # Mirrors main(): load_results adds a uuid to BOTH the mapping and the
        # NSForest frames; the mapping's must be dropped so the merged uuid (the
        # CellSet identity) resolves to the NSForest cluster's, not uuid_x/uuid_y.
        mapping = pd.DataFrame({
            "dataset_version_id": ["dv-001"],
            "cluster_name": ["T-Cell"],
            "skos": ["skos:broadMatch"],
            "manual_mapped_cid": ["CL:0000084"],
            "cell_ontology_id": ["CL:0000236"],
            "uuid": ["mapping-uuid"],
        })
        nsforest = pd.DataFrame({
            "clusterName": ["T-Cell"],
            "clusterSize": [1718],
            "NSForest_markers": ["['TP53']"],
            "binary_genes": ["['TP53']"],
            "uuid": ["nsforest-uuid"],
        })
        mapping = mapping.drop(columns=["uuid"], errors="ignore")
        merged = mapping.merge(nsforest, left_on="cluster_name", right_on="clusterName")
        self.assertIn("uuid", merged.columns)
        tuples = create_tuples(merged, self._make_summary())
        self.assertGreater(len(tuples), 0)
        blob = " ".join(str(part) for t in tuples for part in t)
        self.assertIn("nsforest-uuid", blob)


if __name__ == "__main__":
    unittest.main()
