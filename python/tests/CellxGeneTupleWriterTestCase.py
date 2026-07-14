import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from CellxGeneTupleWriter import create_tuples


class CellxGeneTupleWriterTestCase(unittest.TestCase):
    """Tests for CellxGeneTupleWriter.create_tuples."""

    def _make_dataset(self, dvid, doi):
        return {
            "Dataset_name": "Test Dataset",
            "Organism": "Homo sapiens",
            "Tissue": "brain",
            "Disease_status": "normal",
            "Number_of_cells": 10000,
            "Citation": "Smith et al. (2024)",
            "Link_to_publication": doi,
            "Link_to_CELLxGENE_collection": "https://cellxgene.cziscience.com/collections/abc",
            "Link_to_CELLxGENE_dataset": f"https://cellxgene.cziscience.com/e/{dvid}.cxg/",
            "Collection_ID": "abc",
            "Collection_version_ID": "cv-001",
            "Dataset_ID": f"ds-{dvid}",
            "Dataset_version_ID": dvid,
        }

    def _make_data(self):
        return {"dvid-001": self._make_dataset("dvid-001", "https://doi.org/10.1234/test")}

    def _make_multi_dataset_data(self):
        # Two datasets of one paper, plus a third dataset of another paper.
        return {
            "dvid-001": self._make_dataset("dvid-001", "https://doi.org/10.1234/test"),
            "dvid-002": self._make_dataset("dvid-002", "https://doi.org/10.1234/test"),
            "dvid-003": self._make_dataset("dvid-003", "https://doi.org/10.5678/other"),
        }

    def _attributed_to_edges(self, tuples):
        """Return the (subject, object) terms of the was_attributed_to edges."""
        return {
            (str(t[0]).rsplit("/", 1)[-1], str(t[2]).rsplit("/", 1)[-1])
            for t in tuples
            if str(t[1]).endswith("wasAttributedTo")
        }

    def test_creates_tuples(self):
        tuples = create_tuples(self._make_data())
        self.assertGreater(len(tuples), 0)

    def test_contains_source_quintuple(self):
        tuples = create_tuples(self._make_data())
        quints = [t for t in tuples if len(t) == 5 and "Source" in str(t[3])]
        self.assertTrue(any("CELLxGENE" in str(t[4]) for t in quints))

    def test_csd_annotations(self):
        tuples = create_tuples(self._make_data())
        csd_annots = [
            t for t in tuples
            if len(t) == 3 and "CSD_" in str(t[0])
        ]
        self.assertGreater(len(csd_annots), 0)

    def test_publication_keyed_by_doi(self):
        tuples = create_tuples(self._make_data())
        self.assertEqual(
            self._attributed_to_edges(tuples),
            {("CSD_dvid-001", "PUB_10.1234-test")},
        )

    def test_datasets_of_one_paper_share_one_publication(self):
        tuples = create_tuples(self._make_multi_dataset_data())
        self.assertEqual(
            self._attributed_to_edges(tuples),
            {
                ("CSD_dvid-001", "PUB_10.1234-test"),
                ("CSD_dvid-002", "PUB_10.1234-test"),
                ("CSD_dvid-003", "PUB_10.5678-other"),
            },
        )

    def test_distinct_papers_stay_distinct(self):
        tuples = create_tuples(self._make_multi_dataset_data())
        pub_terms = {
            str(t[0]).rsplit("/", 1)[-1]
            for t in tuples
            if str(t[0]).rsplit("/", 1)[-1].startswith("PUB_")
        }
        self.assertEqual(pub_terms, {"PUB_10.1234-test", "PUB_10.5678-other"})

    def test_publication_doi_annotation_is_a_bare_doi(self):
        tuples = create_tuples(self._make_data())
        dois = [
            str(t[2])
            for t in tuples
            if len(t) == 3
            and str(t[0]).endswith("PUB_10.1234-test")
            and str(t[1]).endswith("publication_doi")
        ]
        self.assertEqual(dois, ["10.1234/test"])


if __name__ == "__main__":
    unittest.main()
