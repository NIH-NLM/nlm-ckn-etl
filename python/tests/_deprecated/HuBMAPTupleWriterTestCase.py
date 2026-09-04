import sys
from pathlib import Path
import unittest

# Both directories: the module itself was retired to src/_deprecated/, but it
# still imports LoaderUtilities and TupleWriterUtilities from src/.
_PYTHON_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_PYTHON_ROOT / "src"))
sys.path.insert(0, str(_PYTHON_ROOT / "src" / "_deprecated"))

from HuBMAPTupleWriter import create_tuples


class HuBMAPTupleWriterTestCase(unittest.TestCase):
    """Tests for HuBMAPTupleWriter.create_tuples."""

    def _make_data(self):
        return {
            "data": {
                "anatomical_structures": [
                    {
                        "id": "UBERON:0000955",
                        "ccf_pref_label": "brain",
                        "ccf_part_of": ["UBERON:0000468"],
                    }
                ],
            }
        }

    def test_anatomical_structure_part_of(self):
        tuples = create_tuples(self._make_data())
        preds = [str(t[1]) for t in tuples if len(t) == 3 and "#" not in str(t[1])]
        self.assertTrue(any("BFO_0000050" in p for p in preds))

    def test_no_label_annotations(self):
        """The UBERON ontology supplies the label, so HuBMAP must not emit one.

        ccf_pref_label diverges from the UBERON label for 317 of the 572
        HuBMAP UBERON terms, and the loader keeps the last non-None value,
        so emitting it here would overwrite the ontology label.
        """
        tuples = create_tuples(self._make_data())
        labels = [t for t in tuples if len(t) == 3 and "#label" in str(t[1])]
        self.assertEqual(labels, [])


if __name__ == "__main__":
    unittest.main()
