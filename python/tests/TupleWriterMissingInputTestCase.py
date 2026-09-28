import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import CellxGeneTupleWriter
import GeneTupleWriter
import OpenTargetsTupleWriter
import UniProtTupleWriter


class TupleWriterMissingInputTestCase(unittest.TestCase):
    """A tuple writer whose input is absent must fail, not exit quietly.

    TupleWriterPipeline runs each writer's main() in one process; a writer
    that returns without writing would otherwise let the pipeline report
    success with a tuple file missing.
    """

    def _assert_raises_when_empty(self, module, expected_name):
        with tempfile.TemporaryDirectory() as tmpdir:
            run = SimpleNamespace(external_dir=Path(tmpdir))
            with patch.object(module, "get_current_run", return_value=run):
                with self.assertRaises(FileNotFoundError) as ctx:
                    module.main()
        self.assertIn(expected_name, str(ctx.exception))

    def test_cellxgene_writer_raises(self):
        self._assert_raises_when_empty(CellxGeneTupleWriter, "cellxgene")

    def test_gene_writer_raises(self):
        self._assert_raises_when_empty(GeneTupleWriter, "gene_transformed.json")

    def test_uniprot_writer_raises(self):
        self._assert_raises_when_empty(UniProtTupleWriter, "uniprot_transformed.json")

    def test_opentargets_writer_raises_for_each_missing_input(self):
        names = [
            "opentargets_transformed.json",
            "gene_transformed.json",
            "uniprot_transformed.json",
        ]
        for present in range(len(names)):
            with tempfile.TemporaryDirectory() as tmpdir:
                for name in names[:present]:
                    (Path(tmpdir) / name).write_text("{}")
                run = SimpleNamespace(external_dir=Path(tmpdir))
                with patch.object(
                    OpenTargetsTupleWriter, "get_current_run", return_value=run
                ):
                    with self.assertRaises(FileNotFoundError) as ctx:
                        OpenTargetsTupleWriter.main()
            self.assertIn(names[present], str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
