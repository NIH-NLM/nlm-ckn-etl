import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd

from NSForestTupleWriter import create_tuples


class NSForestTupleWriterTestCase(unittest.TestCase):
    """Tests for NSForestTupleWriter.create_tuples."""

    def _make_data(self):
        nsforest = pd.DataFrame({
            "clusterName": ["T Cell"],
            "clusterSize": [1718],
            "f_score": [0.716],
            "precision": [0.787],
            "TN": [103482],
            "FP": [245],
            "FN": [813],
            "TP": [905],
            "marker_count": [1],
            "NSForest_markers": ["['TP53']"],
            "binary_genes": ["['TP53', 'BRCA1', 'EGFR']"],
            "uuid": ["abc123"],
        })
        summary = pd.DataFrame({
            "tissue_ontology_term_id": ["UBERON:0000966"],
        })
        return nsforest, summary

    def test_creates_tuples(self):
        nsf, summary = self._make_data()
        tuples = create_tuples(nsf, summary, ["dvid-001"])
        self.assertGreater(len(tuples), 0)

    def test_contains_derives_from(self):
        nsf, summary = self._make_data()
        tuples = create_tuples(nsf, summary, ["dvid-001"])
        preds = [str(t[1]) for t in tuples if len(t) == 3]
        self.assertTrue(any("RO_0001000" in p for p in preds))  # derives_from

    def test_contains_has_characterizing_marker_set(self):
        nsf, summary = self._make_data()
        tuples = create_tuples(nsf, summary, ["dvid-001"])
        preds = [str(t[1]) for t in tuples if len(t) == 3]
        self.assertTrue(any("RO_0015004" in p for p in preds))

    def test_contains_gene_part_of_bmc(self):
        nsf, summary = self._make_data()
        tuples = create_tuples(nsf, summary, ["dvid-001"])
        preds = [str(t[1]) for t in tuples if len(t) == 3]
        self.assertTrue(any("BFO_0000050" in p for p in preds))  # part_of

    def test_contains_subcluster_of(self):
        nsf, summary = self._make_data()
        tuples = create_tuples(nsf, summary, ["dvid-001"])
        preds = [str(t[1]) for t in tuples if len(t) == 3]
        self.assertTrue(any("RO_0015003" in p for p in preds))  # subcluster_of

    def test_contains_expresses(self):
        nsf, summary = self._make_data()
        tuples = create_tuples(nsf, summary, ["dvid-001"])
        preds = [str(t[1]) for t in tuples if len(t) == 3]
        self.assertTrue(any("RO_0002292" in p for p in preds))  # expresses

    def test_contains_expresses_gene_per_binary_gene(self):
        nsf, summary = self._make_data()
        tuples = create_tuples(nsf, summary, ["dvid-001"])
        # CS -[expresses]-> Gene triples: predicate RO_0002292, object GS_<symbol>
        gene_edges = [
            t for t in tuples
            if len(t) == 3
            and "RO_0002292" in str(t[1])
            and "/GS_" in str(t[2])
        ]
        objects = {str(t[2]).rsplit("/", 1)[-1] for t in gene_edges}
        self.assertEqual(objects, {"GS_TP53", "GS_BRCA1", "GS_EGFR"})

    def test_contains_source_quintuple(self):
        nsf, summary = self._make_data()
        tuples = create_tuples(nsf, summary, ["dvid-001"])
        quints = [t for t in tuples if len(t) == 5 and "Source" in str(t[3])]
        self.assertGreater(len(quints), 0)
        sources = {str(t[4]) for t in quints}
        self.assertTrue({"NS-Forest", "CELLxGENE"} & sources)

    def test_skips_small_clusters(self):
        nsf, summary = self._make_data()
        nsf.loc[0, "clusterSize"] = 5  # Below MIN_CLUSTER_SIZE
        tuples = create_tuples(nsf, summary, ["dvid-001"])
        self.assertEqual(len(tuples), 0)

    def test_edge_annotations_on_cs_bmc(self):
        nsf, summary = self._make_data()
        tuples = create_tuples(nsf, summary, ["dvid-001"])
        edge_annots = [
            t for t in tuples
            if len(t) == 5 and "RO_0015004" in str(t[1])
        ]
        attrs = [str(t[3]).split("#")[-1] for t in edge_annots]
        self.assertIn("Precision", attrs)
        self.assertIn("TP", attrs)
        self.assertIn("FN", attrs)

    # --- root UBERON term rollup -------------------------------------------

    @staticmethod
    def _anatomical_objects(tuples, predicate):
        """Return the UBERON terms an edge with this predicate connects to."""
        return {
            str(t[2]).rsplit("/", 1)[-1]
            for t in tuples
            if len(t) == 3 and predicate in str(t[1]) and "/UBERON_" in str(t[2])
        }

    def _heart_data(self):
        nsf, summary = self._make_data()
        nsf.loc[0, "clusterName"] = "Pericyte"
        summary.loc[0, "organ"] = "heart_plus_pericardium"
        summary.loc[0, "tissue_ontology_term_id"] = (
            "UBERON:0006566 | UBERON:0006567 | UBERON:0002084"
        )
        return nsf, summary

    def test_cell_set_derives_from_root_term_alone(self):
        nsf, summary = self._heart_data()
        tuples = create_tuples(
            nsf, summary, ["dvid-001"], root_uberon_term="UBERON:0015410"
        )
        # derives_from
        self.assertEqual(
            self._anatomical_objects(tuples, "RO_0001000"), {"UBERON_0015410"}
        )
        # is_about
        self.assertEqual(
            self._anatomical_objects(tuples, "IAO_0000136"), {"UBERON_0015410"}
        )

    def test_sampled_tissue_annotates_the_root_edge(self):
        nsf, summary = self._heart_data()
        tuples = create_tuples(
            nsf, summary, ["dvid-001"], root_uberon_term="UBERON:0015410"
        )
        sampled = {
            str(t[4])
            for t in tuples
            if len(t) == 5 and str(t[3]).endswith("#Sampled_tissue")
        }
        self.assertEqual(
            sampled, {"UBERON:0006566|UBERON:0006567|UBERON:0002084"}
        )

    def test_without_a_root_term_every_tissue_term_connects(self):
        nsf, summary = self._heart_data()
        tuples = create_tuples(nsf, summary, ["dvid-001"])
        self.assertEqual(
            self._anatomical_objects(tuples, "RO_0001000"),
            {"UBERON_0006566", "UBERON_0006567", "UBERON_0002084"},
        )

    # --- member_of dataset_version_id resolution ---------------------------

    @staticmethod
    def _member_of(tuples):
        """Return {(CS_term, CSD_term)} for member_of core triples.

        A CellSetDataset term only ever appears as the OBJECT of member_of
        (is_about has the CSD as subject), so a 3-tuple whose object is a
        CSD_* term is a member_of edge.
        """
        return {
            (str(t[0]).rsplit("/", 1)[-1], str(t[2]).rsplit("/", 1)[-1])
            for t in tuples
            if len(t) == 3 and "/CSD_" in str(t[2])
        }

    def _multi_dataset_data(self):
        nsf = pd.DataFrame({
            "clusterName": ["A", "B"],
            "clusterSize": [50, 60],
            "f_score": [0.9, 0.8],
            "NSForest_markers": ["['TP53']", "['EGFR']"],
            "binary_genes": ["['TP53']", "['EGFR']"],
            "uuid": ["ua", "ub"],
        })
        summary = pd.DataFrame({"tissue_ontology_term_id": ["UBERON:0000966"]})
        return nsf, summary

    def test_single_dvid_member_of_unchanged(self):
        nsf, summary = self._make_data()
        tuples = create_tuples(nsf, summary, ["dvid-001"])
        self.assertEqual(self._member_of(tuples), {("CS_abc123", "CSD_dvid-001")})

    def test_multi_dvid_resolves_one_edge_per_cluster(self):
        nsf, summary = self._multi_dataset_data()
        cluster_dvid_map = {"A": "dv1", "B": "dv2"}
        tuples = create_tuples(
            nsf, summary, ["dv1", "dv2"], None, cluster_dvid_map
        )
        # Each cell set is a member of ONLY its mapped dataset — no fan-out.
        self.assertEqual(
            self._member_of(tuples),
            {("CS_ua", "CSD_dv1"), ("CS_ub", "CSD_dv2")},
        )

    def test_multi_dvid_unresolved_cluster_raises(self):
        nsf, summary = self._multi_dataset_data()
        cluster_dvid_map = {"A": "dv1"}  # B unmapped
        with self.assertRaises(Exception):
            create_tuples(nsf, summary, ["dv1", "dv2"], None, cluster_dvid_map)

    def test_multi_dvid_unknown_dataset_raises(self):
        nsf, summary = self._multi_dataset_data()
        cluster_dvid_map = {"A": "dv1", "B": "dvX"}  # dvX not a known dataset
        with self.assertRaises(Exception):
            create_tuples(nsf, summary, ["dv1", "dv2"], None, cluster_dvid_map)


if __name__ == "__main__":
    unittest.main()
