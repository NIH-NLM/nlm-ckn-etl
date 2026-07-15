import sys
import tempfile
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd

from LoaderUtilities import (
    get_uberon_root_map,
    resolve_root_uberon_term,
    resolve_summary_root_uberon_term,
)

# One root, as every organ but the respiratory system has.
KIDNEY_CSV = """obo_id,label,level
UBERON:0002113,kidney,root
UBERON:0001225,cortex of kidney,descendant
UBERON:8920012,segmental renal vein,descendant
"""

# Two roots, as the respiratory system has: its table was built from a
# respiratory system query and a nose query.
RESPIRATORY_CSV = """obo_id,label,level
UBERON:0001004,respiratory system,root
UBERON:0000004,nose,root
UBERON:0002048,lung,descendant
UBERON:0001005,respiratory airway,descendant
"""


class UberonRootMapTestCase(unittest.TestCase):
    """Tests for the organ to UBERON root mapping and its resolver."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.results_dir = Path(self._tmp.name)
        (self.results_dir / "uberon_kidney.csv").write_text(KIDNEY_CSV)
        (self.results_dir / "uberon_respiratory_system.csv").write_text(
            RESPIRATORY_CSV
        )
        self.root_map = get_uberon_root_map(self.results_dir)

    def tearDown(self):
        self._tmp.cleanup()

    def test_maps_each_organ_file(self):
        self.assertEqual(
            set(self.root_map), {"kidney", "respiratory_system"}
        )
        self.assertEqual(self.root_map["kidney"]["roots"], [("UBERON:0002113", "kidney")])
        self.assertIn("UBERON:8920012", self.root_map["kidney"]["terms"])

    def test_nested_organ_file_is_discovered(self):
        # The validator discovers UBERON tables recursively, so the root map
        # must too: a table nested a directory deep would otherwise pass
        # validation yet be missing from the map, sending writers down the
        # descendant-tissue fallback the rollup exists to avoid.
        nested = self.results_dir / "extracted"
        nested.mkdir()
        (nested / "uberon_liver.csv").write_text(
            "obo_id,label,level\nUBERON:0002107,liver,root\n"
        )
        root_map = get_uberon_root_map(self.results_dir)
        self.assertIn("liver", root_map)
        self.assertEqual(root_map["liver"]["roots"], [("UBERON:0002107", "liver")])

    def test_descendant_resolves_to_organ_root(self):
        self.assertEqual(
            resolve_root_uberon_term(
                "kidney", ["UBERON:0001225", "UBERON:8920012"], self.root_map
            ),
            "UBERON:0002113",
        )

    def test_multi_root_organ_resolves_to_organ_named_root(self):
        self.assertEqual(
            resolve_root_uberon_term(
                "respiratory_system", ["UBERON:0002048"], self.root_map
            ),
            "UBERON:0001004",
        )

    def test_dataset_sampled_from_a_root_resolves_to_that_root(self):
        # The nose dataset connects to nose, not to the respiratory system.
        self.assertEqual(
            resolve_root_uberon_term(
                "respiratory_system", ["UBERON:0000004"], self.root_map
            ),
            "UBERON:0000004",
        )

    def test_unknown_tissue_term_still_resolves_to_organ_root(self):
        self.assertEqual(
            resolve_root_uberon_term("kidney", ["UBERON:0002371"], self.root_map),
            "UBERON:0002113",
        )

    def test_organ_without_a_file_uses_the_override(self):
        self.assertEqual(
            resolve_root_uberon_term("neocortex", ["UBERON:0002686"], self.root_map),
            "UBERON:0000955",
        )

    def test_organ_without_a_file_or_override_resolves_to_none(self):
        self.assertIsNone(
            resolve_root_uberon_term("spleen", ["UBERON:0002106"], self.root_map)
        )

    def test_resolves_from_a_summary(self):
        summary = pd.DataFrame({
            "organ": ["kidney"],
            "tissue_ontology_term_id": ["UBERON:0001225 | UBERON:8920012"],
        })
        self.assertEqual(
            resolve_summary_root_uberon_term(summary, self.root_map),
            "UBERON:0002113",
        )

    def test_summary_without_an_organ_resolves_to_none(self):
        summary = pd.DataFrame({"tissue_ontology_term_id": ["UBERON:0001225"]})
        self.assertIsNone(resolve_summary_root_uberon_term(summary, self.root_map))
        self.assertIsNone(
            resolve_summary_root_uberon_term(pd.DataFrame(), self.root_map)
        )


if __name__ == "__main__":
    unittest.main()
